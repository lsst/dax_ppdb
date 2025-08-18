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
import posixpath
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lsst.dax.ppdbx.gcp.auth import get_auth_default
from lsst.dax.ppdbx.gcp.gcs import DeleteError, StorageClient, UploadError
from lsst.dax.ppdbx.gcp.pubsub import Publisher

from ..config import PpdbConfig
from ..ppdb import PpdbReplicaChunk
from ..sql._ppdb_replica_chunk_sql import ChunkStatus, PpdbReplicaChunkSql

__all__ = ["ChunkUploader"]

_LOG = logging.getLogger(__name__)


class ChunkUploader:
    """Periodically queries a database to find APDB replica chunk data that is
    ready to be uploaded to Google Cloud Storage (GCS), copies the chunk's
    parquet files to the specified GCS bucket and prefix, and publishes a
    message to a Pub/Sub topic to trigger the staging of the chunk in BigQuery.

    Parameters
    ----------
    config : `str`
        The PPDB database config file.
    bucket_name : `str`
        The name of the Google Cloud Storage bucket for upload.
    prefix : `str`
        The base prefix of the uploaded objects, e.g., 'data/staging'.
    dataset : `str`
        The name of the target BigQuery dataset. This may also include the
        project name, e.g., 'my_project:my_dataset'. If the project name is
        not specified, the project name from the environment will be used.
    topic : `str`
        The name of the Pub/Sub topic to which the message will be published.
        Publishing a message to this topic will trigger the staging of the
        chunk in BigQuery. If not specified, the default topic name
        'stage-chunk-topic' will be used.
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
    This class runs as a daemon process, continuously checking for new replica
    chunks to upload by querying the ``PpdbReplicaChunk`` table in the "local"
    PPDB database. The internal ``exit_on_empty`` flag controls whether the
    process exits if no new chunks are found after a scan. The process will
    also exit if there is an exception and the ``exit_on_error`` flag is set to
    `True`. The ``wait_interval`` controls how often the process will query for
    new chunks, and the ``upload_interval`` controls how often chunks are
    uploaded.
    """

    def __init__(
        self,
        config: PpdbConfig,
        bucket_name: str,
        prefix: str,
        dataset: str,
        topic: str | None = None,
        wait_interval: int = 30,
        upload_interval: int = 0,
        exit_on_empty: bool = False,
        exit_on_error: bool = False,
    ) -> None:
        self._sql = PpdbReplicaChunkSql(config)
        self.prefix = prefix
        self.bucket_name = bucket_name
        self.dataset = dataset
        self.wait_interval = wait_interval
        self.upload_interval = upload_interval
        self.exit_on_empty = exit_on_empty
        self.exit_on_error = exit_on_error

        # Authenticate with Google Cloud to set credentials and project ID.
        self.credentials, self.project_id = get_auth_default()

        # Initialize the storage client for interacting with GCS.
        self.storage = StorageClient(bucket_name=self.bucket_name)

        # Initialize the Pub/Sub publisher for staging chunks in BigQuery.
        self.topic_name = topic if topic else "stage-chunk-topic"
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
                _LOG.exception("Failed to retrieve replica chunks from the database.")
                if self.exit_on_error:
                    _LOG.error("Exiting due to error in retrieving replica chunks.")
                    sys.exit(1)
                continue

            replica_chunk_count = len(replica_chunks)
            if replica_chunk_count > 0:
                _LOG.info("Found %d chunks ready for upload", replica_chunk_count)
                for replica_chunk in replica_chunks:
                    try:
                        # Process each replica chunk
                        self._process_chunk(replica_chunk)
                    except Exception:
                        # Some error occurred while processing the chunk.
                        # Log the error and continue to the next chunk or
                        # exit if configured to do so.
                        _LOG.exception("Failed to process replica chunk %s", replica_chunk.id)
                        if self.exit_on_error:
                            _LOG.error(
                                "Exiting due to error in processing replica chunk %s", replica_chunk.id
                            )
                            sys.exit(1)
                    _LOG.info("Done processing %s", replica_chunk.id)
                    if self.upload_interval > 0:
                        # If upload_interval is set, wait a certain amount of
                        # time before processing the next chunk.
                        _LOG.info("Sleeping for %d seconds before next upload", self.upload_interval)
                        time.sleep(self.upload_interval)
            else:
                # No replica chunks were found for upload.
                # Log the information and check if we should exit.
                _LOG.info("No replica chunks were found for upload.")
                if self.exit_on_empty:
                    _LOG.info("Exiting as no ready replica chunks were found.")
                    break

            if self.wait_interval > 0:
                # If wait_interval is set, wait a certain amount of time before
                # checking for new replica chunks again.
                _LOG.info("Sleeping for %d seconds before checking for more chunks...", self.wait_interval)
                time.sleep(self.wait_interval)

    def _process_chunk(self, replica_chunk: PpdbReplicaChunk) -> None:
        """Process a replica chunk by uploading its parquet files and a
        JSON manifest to Google Cloud Storage.

        Parameters
        ----------
        replica_chunk : `PpdbReplicaChunk`
            The replica chunk to process, which includes its ID and directory.

        Raises
        ------
        RuntimeError
            If no parquet files are found in the chunk directory or if the
            processing fails for any reason.
        """
        # Get the information for the chunk.
        chunk_id = replica_chunk.id

        if replica_chunk.directory is None:
            raise RuntimeError(f"Replica chunk {chunk_id} does not have a directory specified.")
        chunk_dir = Path(replica_chunk.directory)
        _LOG.info("Processing chunk %d in directory %s", chunk_id, chunk_dir)

        # Get the local manifest file for the chunk.
        manifest_path = chunk_dir / f"chunk_{chunk_id}.manifest.json"
        if not manifest_path.exists():
            raise RuntimeError(f"Manifest file {manifest_path} does not exist for chunk {chunk_id}")
        manifest_data = json.loads(manifest_path.read_text())

        # Set the GCS prefix for the chunk.
        exported_at = datetime.fromisoformat(manifest_data.get("exported_at"))
        gcs_prefix = posixpath.join(self.prefix, f"chunks/{exported_at.strftime('%Y/%m/%d')}/{chunk_id}")
        _LOG.info("GCS path for chunk %d: %s", chunk_id, gcs_prefix)

        # Get the Parquet files for the chunk and raise an error if there are
        # no files found in the chunk directory.
        parquet_files = list(chunk_dir.glob("*.parquet"))
        if not parquet_files:
            raise RuntimeError(f"No parquet files found in chunk directory {chunk_dir}")

        try:
            # Create full object names for the Parquet files.
            gcs_names = {file: posixpath.join(gcs_prefix, file.name) for file in parquet_files}

            # TODO: Check if any of the files already exist in GCS and raise an
            # exception if they do. There should probably also be a flag to
            # allow overwriting existing files for development and test envs.

            try:
                # Upload Parquet files to GCS.
                self.storage.upload_files(gcs_names)
            except* UploadError as upload_errors:
                # If any uploads failed, display the error that occurred for
                # each upload, including the traceback, and then raise a single
                # RuntimeError.
                for upload_error in upload_errors.exceptions:
                    _LOG.error(
                        "%s",
                        upload_error,
                        exc_info=(type(upload_error), upload_error, upload_error.__traceback__),
                    )
                raise RuntimeError(
                    f"{len(upload_errors.exceptions)} uploads failed for chunk {chunk_id}"
                ) from upload_errors

            # Create the cloud manifest from the local one, update it, and then
            # upload it to GCS.
            manifest = self._update_manifest_uploaded(manifest_data)
            _LOG.info("Generated manifest: %s", manifest)
            try:
                self.storage.upload_from_string(
                    json.dumps(manifest), posixpath.join(gcs_prefix, f"chunk_{chunk_id}.manifest.json")
                )
            except UploadError as e:
                raise RuntimeError(f"Failed to upload manifest for chunk {chunk_id} to GCS: {e}") from e

            # Update the record for the replica chunk in the database to
            # indicate that it has been uploaded.
            try:
                with self._sql._engine.begin() as connection:
                    self._sql._store_insert_id(
                        replica_chunk,
                        connection,
                        True,  # Update the chunk as exported
                        status=ChunkStatus.UPLOADED,
                    )
            except Exception:
                _LOG.exception("Failed to update replica chunk %s in database", replica_chunk.id)
                raise

            # Post to the Pub/Sub topic, triggering the staging of the chunk
            # in BigQuery. This happens after the database to ensure state
            # consistency.
            self._post_to_stage_chunk_topic(self.bucket_name, gcs_prefix, chunk_id)

        except Exception as e:
            try:
                # Recursively delete objects under the GCS prefix if the upload
                # fails.
                try:
                    self.storage.delete_objects(gcs_prefix)
                except DeleteError as delete_error:
                    _LOG.error(
                        "Failed to delete objects under prefix %s: %s",
                        gcs_prefix,
                        delete_error,
                        exc_info=True,
                    )

                # Update the local manifest to indicate that a failure
                # occurred.
                self._update_manifest_failure(manifest_path, manifest_data, e)
            except Exception:
                _LOG.exception("Error cleaning up after failed upload")
            finally:
                raise RuntimeError(
                    "Processing failed to upload chunk %s to %s",
                    chunk_dir.name,
                    f"gc://{self.bucket_name}/{gcs_prefix}",
                ) from e

    def _update_manifest_failure(
        self, manifest_path: Path, manifest_data: dict[str, Any], error: Exception | str
    ) -> None:
        """Update the manifest file to indicate a failure.

        Parameters
        ----------
        manifest_path : `Path`
            The path to the manifest file.
        error : `Exception`
            The exception that occurred during processing.
        """
        if not manifest_path.exists():
            raise RuntimeError(f"Manifest file {manifest_path} does not exist, cannot update failure")
        manifest_data["error"] = str(error) if isinstance(error, Exception) else error
        with open(manifest_path, "w") as f:
            json.dump(manifest_data, f, indent=4)

    def _update_manifest_uploaded(self, manifest_data: dict[str, Any]) -> dict[str, Any]:
        """Generate a manifest file for the chunk.

        This just copies the local manifest data and updates it with some
        additional information.

        Parameters
        ----------
        manifest data : `dict`
            The manifest data for the chunk, including when it was exported and
            other relevant information.

        Returns
        -------
        manifest : `dict`
            The manifest data.
        """
        manifest = manifest_data.copy()
        manifest["uploaded_at"] = datetime.now(tz=timezone.utc).isoformat()
        return manifest

    def _post_to_stage_chunk_topic(self, bucket_name: str, chunk_prefix: str, chunk_id: int) -> None:
        """Publish a message to the 'stage-chunk-topic' Pub/Sub topic.

        This will trigger the staging of the chunk in BigQuery.

        Parameters
        ----------
        bucket_name : str
            The name of the GCS bucket.
        chunk_prefix : str
            The prefix to the chunk in the bucket.
        chunk_id : int
            The ID of the chunk being staged.
        """
        # Construct the message payload
        message = {
            "dataset": self.dataset,
            "chunk_id": str(chunk_id),
            "folder": f"gs://{posixpath.join(bucket_name, chunk_prefix)}",
        }

        self.publisher.publish(message)
