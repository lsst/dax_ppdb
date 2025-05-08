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

import json
import logging
import os
import posixpath
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import google.auth
from google.cloud import pubsub_v1, storage

__all__ = ["ChunkUploader"]

_LOG = logging.getLogger(__name__)

_PUBSUB_TOPIC_NAME = "stage-chunk-topic"


class ChunkUploader:
    """Scans a local directory tree for chunks that are ready to be uploaded
    and copies their parquet files to the specified GCS bucket and prefix.

    Parameters
    ----------
    directory : `str`
        The local directory to scan for parquet files.
    bucket_name : `str`
        The name of the GCS bucket for uploads.
    prefix : `str`
        The base prefix for the uploaded objects, e.g., 'data/staging'.
    wait_interval : `int`
        The time in seconds to wait between scans of the local directory.
    upload_interval : `int`
        The time in seconds to wait between uploads of files.
    exit_on_empty : `bool`
        If `True`, the uploader will exit if no files are found during a scan.
    delete_chunks : `bool`
        If `True`, the files in the chunk directory will be deleted after
        upload. The directory is left so that the marker file ".uploaded" can
        be created and used to indicate that the chunk has been processed.
    exit_on_error : `bool`
        If `True`, the uploader will exit if an error occurs during upload.
    """

    def __init__(
        self,
        directory: str,
        bucket_name: str,
        prefix: str,
        wait_interval: int = 30,
        upload_interval: int = 0,
        exit_on_empty: bool = False,
        delete_chunks: bool = False,
        exit_on_error: bool = False,
    ):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.directory = directory
        self.wait_interval = wait_interval
        self.upload_interval = upload_interval
        self.exit_on_empty = exit_on_empty
        self.delete_chunks = delete_chunks
        self.exit_on_error = exit_on_error

        # Environment check
        if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            raise RuntimeError("Environment variable GOOGLE_APPLICATION_CREDENTIALS is not set.")
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path or not os.path.exists(credentials_path):
            raise RuntimeError("Invalid GOOGLE_APPLICATION_CREDENTIALS path.")

        # Setup Google authentication
        try:
            self.credentials, self.project_id = google.auth.default()
            if not self.project_id:
                raise RuntimeError("Project ID could not be determined from the credentials.")
        except Exception as e:
            raise RuntimeError("Failed to setup Google credentials.") from e

        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)

        self.topic_name = _PUBSUB_TOPIC_NAME

    def run(self) -> None:
        """Start the uploader to scan for files and upload them."""
        while True:
            _LOG.info("Checking for new chunks to upload...")
            ready_files = list(Path(self.directory).rglob(".ready"))
            if ready_files:
                _LOG.info("Found %d chunks ready for upload", len(ready_files))
                chunk_map = self._make_chunk_map(
                    [
                        ready_file.parent
                        for ready_file in ready_files
                        if ready_file.exists() and ready_file.is_file()
                    ]
                )
                for chunk_id, chunk_dir in chunk_map.items():
                    _LOG.info("Processing chunk %s in directory %s", chunk_id, chunk_dir)
                    try:
                        self._process_chunk(chunk_dir)
                    except Exception:
                        _LOG.exception("Failed to process chunk %s", chunk_dir)
                        if self.exit_on_error:
                            _LOG.error("Exiting due to error in processing chunk %s", chunk_id)
                            sys.exit(1)
                    _LOG.info("Done processing chunk %s", chunk_id)
                    if self.upload_interval > 0:
                        _LOG.info("Sleeping for %d seconds before next upload", self.upload_interval)
                        time.sleep(self.upload_interval)
            else:
                _LOG.info("No ready chunks were found.")
                if self.exit_on_empty:
                    _LOG.info("Exiting as no ready chunks were found.")
                    break

            if self.wait_interval > 0:
                _LOG.info("Sleeping for %d seconds before next scan", self.wait_interval)
                time.sleep(self.wait_interval)

    @classmethod
    def _make_chunk_map(cls, chunk_paths: list[Path]) -> dict[int, Path]:
        """Create a mapping of chunk IDs to their respective paths.

        Parameters
        ----------
        chunk_paths : `list`
            A list of paths to chunk directories.

        Returns
        -------
        chunk_map : `dict`
            A dictionary mapping chunk IDs to their respective paths.

        Notes
        -----
        Paths which are not directories or do not contain valid chunk IDs are
        ignored with a warning logged. The chunk IDs are expected to be
        integers.
        """
        chunk_map = {}
        for path in chunk_paths:
            if path.is_dir():
                try:
                    chunk_id = int(path.name)
                    chunk_map[chunk_id] = path
                except ValueError:
                    _LOG.warning("Invalid chunk ID %s from path %s", path.name, path)
                    continue
            else:
                _LOG.warning("Path %s is not a directory", path)
                continue
        return dict(sorted(chunk_map.items()))

    def _process_chunk(self, chunk_dir: Path) -> None:
        """Process a chunk directory by uploading its files to GCS.

        Parameters
        ----------
        chunk_dir : `Path`
            The directory containing the chunk files.

        Raises
        ------
        RuntimeError
            If no parquet files are found in the chunk directory or if the
            processing fails for any reason.
        """
        parquet_files = list(chunk_dir.glob("*.parquet"))

        if not parquet_files:
            raise RuntimeError(f"No parquet files found in {chunk_dir}")

        try:
            # Get the GCS names for the parquet files
            relative_chunk_path = Path(chunk_dir).relative_to(self.directory)
            gcs_prefix = posixpath.join(self.prefix, str(relative_chunk_path))
            gcs_names = {file: posixpath.join(gcs_prefix, file.name) for file in parquet_files}

            # Upload parquet files to GCS
            self._upload_files(gcs_names)

            # Generate and upload manifest
            manifest = self._generate_manifest(chunk_dir)
            _LOG.info("Generated manifest: %s", manifest)
            manifest_path = posixpath.join(gcs_prefix, "manifest.json")
            self._upload_manifest(manifest, manifest_path)

            # Post to Pub/Sub topic
            self._post_to_stage_chunk_topic(self.bucket_name, gcs_prefix)

            # Optionally delete the chunk directory
            if self.delete_chunks:
                self._delete_chunk(chunk_dir)

            # Mark the chunk as uploaded
            self._mark_uploaded(chunk_dir)

        except Exception as e:
            try:
                # Recursively delete objects under the GCS prefix if the upload
                # fails
                self._delete_objects(gcs_prefix)

                # Mark the chunk as failed
                self._mark_failed(chunk_dir, e)
            except Exception:
                _LOG.exception("Error cleaning up after failed upload")
            finally:
                raise RuntimeError(
                    "Processing failed to upload chunk %s to %s",
                    chunk_dir.name,
                    f"gc://{self.bucket_name}/{gcs_prefix}",
                ) from e

    def _upload_files(self, gcs_names: dict[Path, str]) -> None:
        """Upload files to GCS using a thread pool.

        Parameters
        ----------
        gcs_paths : `dict`
            A dictionary mapping local file paths to GCS paths.
        """
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self._upload_single_file, file, path) for file, path in gcs_names.items()
            ]
            _LOG.debug("Uploading %d files to GCS", len(futures))
            for future in as_completed(futures):
                try:
                    future.result()  # Trigger exception if any
                except Exception:
                    _LOG.exception("Error uploading file")
                    raise

    def _upload_single_file(self, file_path: Path, gcs_name: str) -> None:
        """Upload a single file to GCS.

        Parameters
        ----------
        file_path : `Path`
            The local file path to upload.
        gcs_path : `str`
            The target GCS path.
        """
        blob = self.bucket.blob(gcs_name)
        try:
            blob.upload_from_filename(file_path)
            _LOG.info("Uploaded %s to %s", file_path, gcs_name)
        except Exception:
            _LOG.exception("Failed to upload %s", file_path)
            raise

    def _upload_manifest(self, manifest: dict[str, str], gcs_name: str) -> None:
        """Upload the manifest file to GCS.

        Parameters
        ----------
        manifest : `dict`
            The manifest data to upload.
        gcs_path : `str`
            The target GCS name for the manifest file.
        """
        blob = self.bucket.blob(gcs_name)
        try:
            blob.upload_from_string(json.dumps(manifest), content_type="application/json")
            _LOG.info("Uploaded manifest to %s", gcs_name)
        except Exception:
            _LOG.exception("Failed to upload manifest")
            raise

    def _mark_failed(self, chunk_dir: Path, exc: Exception | None = None) -> None:
        """Mark the chunk as failed by creating a ".error" file.

        Parameters
        ----------
        chunk_dir : `Path`
            The directory containing the chunk files.
        exc : `Exception`, optional
            The exception that caused the failure, if any.
        """
        with open(chunk_dir / ".error", "w") as f:
            f.write(traceback.format_exc() if exc else "Upload failed")
        _LOG.info("Marked chunk %s upload as failed", chunk_dir)

    def _mark_uploaded(self, chunk_dir: Path) -> None:
        """Mark the chunk as uploaded by creating a ".uploaded" file.

        Parameters
        ----------
        chunk_dir : `Path`
            The directory containing the chunk files.
        """
        uploaded_file = chunk_dir / ".uploaded"
        if not uploaded_file.exists():
            uploaded_file.touch()
            _LOG.debug("Marked chunk %s as uploaded", chunk_dir)

    def _delete_objects(self, gcs_prefix: str) -> None:
        """Recursively delete all objects under a GCS prefix.

        Parameters
        ----------
        gcs_path : `str`
            The GCS prefix to recursively delete.
        """
        try:
            blobs = self.bucket.list_blobs(prefix=gcs_prefix)
            for blob in blobs:
                blob.delete()
                _LOG.debug("Deleted GCS object: %s", blob.name)
        except Exception as e:
            raise RuntimeError(f"Failed to delete objects under prefix {gcs_prefix}") from e
        _LOG.info("Deleted all objects under GCS prefix: %s", gcs_prefix)

    def _delete_chunk(self, chunk_dir: Path) -> None:
        """Delete the files in the chunk directory after upload.

        Parameters
        ----------
        chunk_dir : `Path`
            The directory containing the chunk files.

        Notes
        -----
        The directory itself is left so that the marker file ".uploaded" can be
        created and used to indicate that the chunk has been successfully
        processed.
        """
        for file in chunk_dir.glob("*"):
            file.unlink()
        _LOG.debug("Deleted chunk %s", chunk_dir)

    def _generate_manifest(self, chunk_dir: Path) -> dict[str, Any]:
        """Generate a manifest file for the chunk.

        Parameters
        ----------
        chunk_dir : `Path`
            The directory containing the chunk files.

        Returns
        -------
        dict
            The manifest data.
        """
        manifest = {
            "chunk_id": str(chunk_dir.name),
            "schema_version": str(chunk_dir.parent.name).removeprefix("v").replace("_", "."),
            "table_files": {file.stem: str(file.name) for file in chunk_dir.glob("*.parquet")},
            "ingest_time": datetime.now(tz=timezone.utc).isoformat(),
        }
        return manifest

    def _post_to_stage_chunk_topic(self, bucket_name: str, chunk_path: str) -> None:
        """Publish a message to the 'stage-chunk-topic' Pub/Sub topic.

        Parameters
        ----------
        topic_name : str
            The name of the Pub/Sub topic (e.g., 'stage-chunk-topic').
        bucket_name : str
            The name of the GCS bucket.
        chunk_path : str
            The path to the chunk in the bucket.
        chunk_id : str
            The unique ID of the chunk.
        """
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self.project_id, self.topic_name)

        # Construct the message payload
        message = {"bucket": bucket_name, "name": chunk_path}

        try:
            # Publish the message
            future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
            future.result()  # Wait for the publish to complete
            _LOG.info("Published message to topic %s: %s", self.topic_name, message)
        except Exception:
            _LOG.exception("Failed to publish message to topic %s: %s", self.topic_name, message)
            raise
