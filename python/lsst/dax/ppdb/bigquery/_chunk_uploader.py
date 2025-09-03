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

from __future__ import annotations

import logging
import posixpath
import sys
import time
from pathlib import Path

from lsst.dax.apdb import monitor
from lsst.dax.apdb.timer import Timer
from lsst.dax.ppdbx.gcp.auth import get_auth_default
from lsst.dax.ppdbx.gcp.gcs import DeleteError, StorageClient, UploadError
from lsst.dax.ppdbx.gcp.pubsub import Publisher

from ..config import PpdbConfig
from ._config import PpdbBigQueryConfig
from ._manifest import Manifest
from ._replica_chunk import ChunkStatus, PpdbReplicaChunkExtended, PpdbReplicaChunkSql

__all__ = ["ChunkUploader", "ChunkUploadError"]

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


class ChunkUploadError(RuntimeError):
    """Top-level error for failures while processing a single chunk."""

    def __init__(self, chunk_id: int, message: str):
        self.chunk_id = chunk_id
        super().__init__(f"[chunk_id={chunk_id}] {message}")


class ChunkUploader:
    """Periodically queries a database to find APDB replica chunk data that is
    ready to be uploaded to Google Cloud Storage (GCS), copies the chunk's
    parquet files to the specified GCS bucket and prefix, and publishes a
    message to a Pub/Sub topic to trigger the staging of the chunk in BigQuery.

    Parameters
    ----------
    config : `PpdbConfig`
        The configuration object which must have the type `PpdbBigQueryConfig`.
    wait_interval : `int`
        The time in seconds to wait between scans for new chunks.
    upload_interval : `int`
        The time in seconds to wait between uploads of files.
        Setting this to a value greater than 0 will cause the
        uploader to wait for this amount of time before processing the next
        chunk after a successful upload.
    exit_on_empty : `bool`
        If `True`, the uploader will exit if no files are found during a scan.
    exit_on_error : `bool`
        If `True`, the uploader will exit if an error occurs during upload.

    Notes
    -----
    This class checks for new APDB replica chunks to upload by querying the
    ``PpdbReplicaChunk`` table in the PPDB (Postgres) database. Chunks with a
    status of "exported", meaning their table data has been written locally
    to parquet files, will be uploaded to a Google Cloud Storage (GCS) bucket.
    The `exit_on_empty` flag controls whether the process exits if no new
    chunks are found after a query. The process will also exit if there is an
    exception and the `exit_on_error` flag is set to `True`. The
    `wait_interval` controls how often the process will query the database for
    new chunks, and the `upload_interval` controls the wait interval between
    uploading the files for a single chunk.
    """

    def __init__(
        self,
        config: PpdbConfig,
        wait_interval: int = 30,
        upload_interval: int = 0,
        exit_on_empty: bool = False,
        exit_on_error: bool = False,
    ):
        # Check for correct config type
        if not isinstance(config, PpdbBigQueryConfig):
            raise TypeError(f"Expecting PpdbBigQueryConfig instance but got {type(config)}")
        self.config = config

        # Setup SQL interface for accessing replica chunk data.
        self._sql = PpdbReplicaChunkSql(config)

        # Read parameters from config
        if self.config.prefix is None:
            raise ValueError("GCS prefix is not set in configuration.")
        self.prefix: str = self.config.prefix
        if self.config.bucket is None:
            raise ValueError("GCS bucket name is not set in configuration.")
        self.bucket_name: str = self.config.bucket
        if self.config.dataset is None:
            raise ValueError("BigQuery dataset is not set in configuration.")
        self.dataset: str = self.config.dataset
        self.topic_name = self.config.stage_chunk_topic

        # Command line parameters
        self.wait_interval = wait_interval
        self.upload_interval = upload_interval
        self.exit_on_empty = exit_on_empty
        self.exit_on_error = exit_on_error

        # Authenticate with Google Cloud to set credentials and project ID.
        self.credentials, self.project_id = get_auth_default()

        # Initialize the storage client for interacting with GCS.
        self.storage = StorageClient(bucket_name=self.bucket_name)

        # Initialize the Pub/Sub publisher for staging chunks in BigQuery.
        self.publisher = Publisher(self.project_id, self.topic_name)

    def run(self) -> None:
        """Check in the database for replica chunks which have been exported
        from the APDB and upload the data and a manifest to cloud storage for
        each one.
        """
        while True:
            _LOG.info("Checking for new replica chunks to upload...")

            try:
                # Get replica chunks that have been exported and are ready for
                # upload to cloud storage.
                replica_chunks = self._sql.get_replica_chunks_by_status(status=ChunkStatus.EXPORTED)
            except Exception:
                # Some problem occurred while retrieving replica chunk data.
                # Log the error and continue to the next iteration or exit if
                # configured to do so.
                _LOG.exception("Failed to retrieve replica chunks from the database")
                if self.exit_on_error:
                    sys.exit(1)
                self._sleep_if(self.wait_interval)
                continue

            if replica_chunks:
                _LOG.info("Found %d replica chunks ready for upload", len(replica_chunks))
                for replica_chunk in replica_chunks:
                    try:
                        # Process each replica chunk
                        _LOG.info("Processing %d", replica_chunk.id)
                        self._process_chunk(replica_chunk)
                    except ChunkUploadError:
                        _LOG.exception("Processing failed for %s", replica_chunk.id)
                        if self.exit_on_error:
                            sys.exit(1)
                    except Exception:
                        _LOG.exception("Unexpected error while processing %s", replica_chunk.id)
                        if self.exit_on_error:
                            sys.exit(1)
                    else:
                        _LOG.info("Finished processing %s", replica_chunk.id)
                        self._sleep_if(self.upload_interval)
            else:
                # No replica chunks were found for upload.
                # Log the information and check if we should exit.
                _LOG.info("No replica chunks were found for upload.")
                if self.exit_on_empty:
                    _LOG.info("Exiting: no ready replica chunks were found.")
                    break

            self._sleep_if(self.wait_interval)

    def _process_chunk(self, replica_chunk: PpdbReplicaChunkExtended) -> None:
        """Process a replica chunk by uploading its parquet files and a
        JSON manifest to Google Cloud Storage.

        Parameters
        ----------
        replica_chunk : `PpdbReplicaChunk`
            The replica chunk to process, which includes its ID and directory.

        Raises
        ------
        ChunkUploadError
            Raised if there is an error during the upload process, such as
            missing files, upload failures, or database update issues.
        """
        chunk_id = replica_chunk.id

        if replica_chunk.directory is None:
            raise ChunkUploadError(chunk_id, "No directory specified on replica chunk")
        chunk_dir = Path(replica_chunk.directory)

        # Read the manifest file to get metadata about the chunk.
        manifest: Manifest | None = None
        try:
            if not replica_chunk.manifest_path.exists():
                raise ChunkUploadError(
                    chunk_id, f"Manifest file does not exist: {replica_chunk.manifest_path}"
                )
            manifest = Manifest.from_json_file(replica_chunk.manifest_path)
        except Exception as e:
            raise ChunkUploadError(chunk_id, f"Failed to read manifest file for: {replica_chunk.id}") from e

        # Construct the GCS prefix for this chunk's files.
        gcs_prefix = posixpath.join(
            self.prefix, f"chunks/{manifest.exported_at.strftime('%Y/%m/%d')}/{chunk_id}"
        )

        # Make a list of local parquet files to upload.
        parquet_files = list(chunk_dir.glob("*.parquet"))
        if not parquet_files:
            raise ChunkUploadError(chunk_id, f"No parquet files found in {chunk_dir}")

        # Calculate total size of files to upload in bytes.
        total_bytes = sum(p.stat().st_size for p in parquet_files)

        uploads_completed = False

        try:
            # 1) Upload parquet files (may raise ExceptionGroup[UploadError]).
            gcs_names = {path: posixpath.join(gcs_prefix, path.name) for path in parquet_files}
            try:
                _LOG.info("Uploading %d parquet files to GCS under prefix: %s", len(gcs_names), gcs_prefix)
                with Timer(
                    "upload_files_time", _MON, tags={"prefix": str(gcs_prefix), "chunk_id": str(chunk_id)}
                ) as timer:
                    self.storage.upload_files(gcs_names)
                    timer.add_values(file_count=len(gcs_names), total_bytes=total_bytes)
            except* UploadError as eg:
                raise ChunkUploadError(chunk_id, f"{len(eg.exceptions)} upload(s) failed") from eg

            uploads_completed = True

            # 2) Upload manifest (catch single UploadError on failure).
            try:
                self.storage.upload_from_string(
                    posixpath.join(gcs_prefix, replica_chunk.manifest_name),
                    manifest.model_dump_json(),
                )
            except UploadError as e:
                raise ChunkUploadError(chunk_id, "Manifest upload failed") from e

            # 3) Update DB status.
            try:
                self._sql.store_chunk(replica_chunk.with_new_status(ChunkStatus.UPLOADED), True)
            except Exception as e:
                raise ChunkUploadError(chunk_id, "failed to update replica chunk status in database") from e

            # 4) Publish Pub/Sub staging message.
            try:
                self._post_to_stage_chunk_topic(self.bucket_name, gcs_prefix, chunk_id)
            except Exception as e:
                raise ChunkUploadError(chunk_id, "failed to publish staging message") from e

        except ChunkUploadError as err:
            # Best-effort cleanup only if parquet uploads did not complete.
            if not uploads_completed:
                try:
                    self.storage.delete_recursive(gcs_prefix)
                except DeleteError as cleanup_err:
                    # Note (Python 3.11+): annotate without masking the
                    # original error.
                    err.add_note(
                        f"cleanup warning: failed to delete "
                        f"gs://{posixpath.join(self.bucket_name, gcs_prefix)}: {cleanup_err}"
                    )
            raise

    def _post_to_stage_chunk_topic(self, bucket_name: str, chunk_prefix: str, chunk_id: int) -> None:
        message = {
            "dataset": self.dataset,
            "chunk_id": str(chunk_id),
            "folder": f"gs://{posixpath.join(bucket_name, chunk_prefix)}",
        }
        self.publisher.publish(message).result(timeout=60)

    @staticmethod
    def _sleep_if(seconds: int) -> None:
        if seconds > 0:
            _LOG.debug("Sleeping for %d seconds...", seconds)
            time.sleep(seconds)
