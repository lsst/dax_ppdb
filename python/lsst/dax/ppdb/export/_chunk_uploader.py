# This file is part of dax_ppdb
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import logging
import os
import posixpath
import shutil
import traceback
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from google.cloud import storage

__all__ = ["ChunkUploader"]

_LOG = logging.getLogger(__name__)


class ChunkUploader:
    """Scans a local directory tree for parquet files that are ready to be
    uploaded to Google Cloud Storage (GCS).

    Parameters
    ----------
    directory : `str`
        The local directory to scan for parquet files.
    bucket_name : `str`
        The name of the GCS bucket to upload files to.
    folder_name : `str`
        The folder name in the GCS bucket where files will be uploaded.
    wait_interval : `int`
        The time in seconds to wait between scans of the local directory.
    exit_on_empty : `bool`
        If `True`, the uploader will exit if no files are found during a scan.
    """

    def __init__(
        self,
        directory: str,
        bucket_name: str,
        folder_name: str,
        wait_interval: int,
        exit_on_empty: bool,
    ):
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.directory = directory
        self.wait_interval = wait_interval
        self.exit_on_empty = exit_on_empty

        # Environment check
        if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            raise RuntimeError("Environment variable GOOGLE_APPLICATION_CREDENTIALS is not set.")
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path or not os.path.exists(credentials_path):
            raise RuntimeError("Invalid GOOGLE_APPLICATION_CREDENTIALS path.")

        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)

    def run(self):
        """Start the uploader to scan for files and upload them."""
        while True:
            _LOG.info("Checking for new chunks to upload...")
            ready_files = list(Path(self.directory).rglob(".ready"))
            if ready_files:
                _LOG.info("Found %d ready files", len(ready_files))
                for ready_file in ready_files:
                    if ready_file.exists() and ready_file.is_file():
                        try:
                            self._process_chunk(ready_file.parent)
                            ready_file.unlink()  # Do this in case directory can't be deleted
                            _LOG.debug("Deleting chunk directory %s", ready_file.parent)
                            shutil.rmtree(ready_file.parent)
                        except Exception:
                            _LOG.exception("Failed to process chunk %s", ready_file.parent)
                            self._set_failed(ready_file.parent)
            else:
                _LOG.info("No ready files found.")
                if self.exit_on_empty:
                    _LOG.info("Exiting as no files were found.")
                    break

            _LOG.info("Sleeping for %d seconds", self.wait_interval)
            time.sleep(self.wait_interval)

    def _process_chunk(self, chunk_dir: Path):
        parquet_files = list(chunk_dir.glob("*.parquet"))

        if not parquet_files:
            _LOG.warning("No parquet files in %s", chunk_dir)
            return

        relative_chunk_path = Path(chunk_dir).relative_to(self.directory)
        base_gcs_path = posixpath.join(self.folder_name, str(relative_chunk_path))
        gcs_paths = {file: posixpath.join(base_gcs_path, file.name) for file in parquet_files}
        try:
            self._upload_files(gcs_paths)  # Upload files to GCS
            self._set_ready(base_gcs_path)  # Mark the chunk as ready for ingest
        except Exception as e:
            _LOG.exception("Upload to cloud storage failed")
            self._delete_objects(gcs_paths.values())  # Delete the files in GCS
            self._set_failed(chunk_dir, e)  # Mark the chunk as failed

    def _upload_files(self, gcs_paths: dict[str, str]) -> None:
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self._upload_single_file, file, path) for file, path in gcs_paths.items()
            ]
            _LOG.debug("Uploading %d files to GCS", len(futures))
            for future in as_completed(futures):
                try:
                    future.result()  # Trigger exception if any
                except Exception:
                    _LOG.exception("Error uploading file")
                    raise

    def _upload_single_file(self, file_path: Path, gcs_path: str) -> None:
        blob = self.bucket.blob(gcs_path)
        try:
            blob.upload_from_filename(file_path)
            _LOG.info("Uploaded %s to %s", file_path, gcs_path)
        except Exception:
            _LOG.exception("Failed to upload %s", file_path)
            raise

    def _set_ready(self, chunk_dir: Path) -> None:
        relative_chunk_path = Path(chunk_dir).relative_to(self.directory)
        ready_file_path = posixpath.join(self.folder_name, str(relative_chunk_path), ".ready")
        blob = self.bucket.blob(ready_file_path)
        try:
            blob.upload_from_string("")
            _LOG.info("Created .ready file at GCS path: %s", ready_file_path)
        except Exception:
            _LOG.exception("Failed to create .ready file")
            raise

    def _set_failed(self, chunk_dir: Path, exc: Exception = None) -> None:
        try:
            with open(chunk_dir / ".error", "w") as f:
                f.write(traceback.format_exc() if exc else "Upload failed")
        except Exception:
            _LOG.exception("Failed to write .error file for chunk %s", chunk_dir)
        _LOG.info("Marked chunk %s upload as failed.", chunk_dir)

    def _delete_objects(self, gcs_paths: list[str]) -> None:
        for gcs_path in gcs_paths:
            blob = self.bucket.blob(gcs_path)
            try:
                blob.delete()
                _LOG.debug("Deleted GCS path %s", gcs_path)
            except Exception:
                _LOG.exception("Failed to delete %s", gcs_path)
