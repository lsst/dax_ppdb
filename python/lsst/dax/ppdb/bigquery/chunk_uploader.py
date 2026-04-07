# This file is part of dax_ppdb.
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

__all__ = ["ChunkUploadError", "ChunkUploader"]

import logging
import posixpath
import sys
import time
from pathlib import Path

from lsst.dax.apdb import monitor
from lsst.dax.apdb.timer import Timer
from lsst.dax.ppdbx.gcp.gcs import DeleteError, StorageClient, UploadError
from lsst.dax.ppdbx.gcp.pubsub import Publisher

from .manifest import Manifest
from .ppdb_bigquery import PpdbBigQuery, PpdbBigQueryConfig
from .ppdb_replica_chunk_extended import ChunkStatus, PpdbReplicaChunkExtended
from .updates.update_records import UpdateRecords

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


class ChunkUploadError(RuntimeError):
    """Top-level error for failures while processing a single chunk.

    Parameters
    ----------
    chunk_id : `int`
        The ID of the chunk being processed when the error occurred.
    message : `str`
        A message describing the error.
    """

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
    ppdb : `PpdbBigQuery`
        The PPDB interface which must have the type `PpdbBigQuery`.
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
        ppdb: PpdbBigQuery,
        wait_interval: int = 30,
        upload_interval: int = 0,
        exit_on_empty: bool = False,
        exit_on_error: bool = False,
    ):
        # Setup interface for accessing replica chunk data
        self._ppdb = ppdb

        # Command line parameters
        self._wait_interval = wait_interval
        self._upload_interval = upload_interval
        self._exit_on_empty = exit_on_empty
        self._exit_on_error = exit_on_error

        # Initialize the storage client for interacting with GCS
        self._storage = StorageClient(bucket_name=self.config.bucket_name)

        # Initialize the Pub/Sub publisher for staging chunks in BigQuery
        self._publisher = Publisher(self.config.project_id, self.config.stage_chunk_topic)

    @property
    def config(self) -> PpdbBigQueryConfig:
        """Config associated with this instance (`PpdbBigQueryConfig`)."""
        return self._ppdb.config

    def run(self) -> None:
        """Check in the database for replica chunks which have been exported
        from the APDB and upload the data and a manifest to cloud storage for
        each one.
        """
        while True:
            _LOG.debug("Checking for new replica chunks to upload...")

            try:
                # Get replica chunks that have been exported and are ready for
                # upload to cloud storage
                replica_chunks = self._ppdb.get_replica_chunks_ext(status=ChunkStatus.EXPORTED)
            except Exception:
                # Some problem occurred while retrieving replica chunk data.
                # Log the error and continue to the next iteration or exit if
                # configured to do so.
                _LOG.exception("Failed to retrieve replica chunks from the database")
                if self._exit_on_error:
                    sys.exit(1)
                self._sleep_if(self._wait_interval)
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
                        if self._exit_on_error:
                            sys.exit(1)
                    except Exception:
                        _LOG.exception("Unexpected error while processing %s", replica_chunk.id)
                        if self._exit_on_error:
                            sys.exit(1)
                    else:
                        _LOG.info("Finished processing %s", replica_chunk.id)
                        self._sleep_if(self._upload_interval)
            else:
                # No replica chunks were found for upload.
                # Log the information and check if we should exit.
                _LOG.info("No replica chunks were found for upload.")
                if self._exit_on_empty:
                    _LOG.info("Exiting: no ready replica chunks were found.")
                    break

            self._sleep_if(self._wait_interval)

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
        chunk_dir = replica_chunk.directory

        # Read the manifest file to get metadata about the chunk
        manifest: Manifest | None = None
        try:
            if not replica_chunk.manifest_path.exists():
                raise ChunkUploadError(
                    chunk_id, f"Manifest file does not exist: {replica_chunk.manifest_path}"
                )
            manifest = Manifest.from_json_file(replica_chunk.manifest_path)
        except Exception as e:
            raise ChunkUploadError(chunk_id, f"Failed to read manifest file for: {replica_chunk.id}") from e

        # Construct the GCS prefix for this chunk's files
        gcs_prefix = posixpath.join(
            self.config.object_prefix,
            f"{manifest.exported_at.strftime('%Y/%m/%d')}/{chunk_id}",
        )

        # Make a list of local parquet files to upload
        upload_file_list = list(chunk_dir.glob("*.parquet"))

        # Check if the chunk is expected to be empty
        is_empty = manifest.is_empty_chunk()

        # Raise an error if no files are present for non-empty chunks since
        # this likely indicates a problem during the export process.
        if not upload_file_list and not is_empty:
            raise ChunkUploadError(chunk_id, f"No files found to upload in {chunk_dir} for non-empty chunk")

        # Check that all expected parquet files from the manifest containing
        # table data are present.
        for table_name, table_stats in manifest.table_data.items():
            if table_stats.row_count > 0:
                expected_file = chunk_dir / f"{table_name}.parquet"
                if not expected_file.exists():
                    raise ChunkUploadError(
                        chunk_id,
                        f"Expected parquet file for table '{table_name}' does not exist: {expected_file}",
                    )

        # Ensure that the parquet file containing updates exists if the
        # manifest indicates that there are update records in this chunk.
        if manifest.includes_update_records:
            update_records_file = chunk_dir / UpdateRecords.PARQUET_FILE_NAME
            if not update_records_file.exists():
                raise ChunkUploadError(
                    chunk_id,
                    f"Manifest indicates that replica chunk {chunk_id} has update records but file does not "
                    f"exist: {update_records_file}",
                )

        try:
            # 1) Upload the files to GCS for non-empty chunks
            if upload_file_list:
                gcs_names = {path: posixpath.join(gcs_prefix, path.name) for path in upload_file_list}
                try:
                    _LOG.info("Uploading %d files to GCS under prefix: %s", len(gcs_names), gcs_prefix)
                    with Timer(
                        "upload_files_time", _MON, tags={"prefix": str(gcs_prefix), "chunk_id": str(chunk_id)}
                    ) as timer:
                        self._storage.upload_files(gcs_names)
                        total_bytes = sum(p.stat().st_size for p in upload_file_list)
                        timer.add_values(file_count=len(gcs_names), total_bytes=total_bytes)
                except* UploadError as eg:
                    raise ChunkUploadError(chunk_id, f"{len(eg.exceptions)} upload(s) failed") from eg

            # 2) Upload manifest, even for empty chunks
            try:
                self._storage.upload_from_string(
                    posixpath.join(gcs_prefix, Path(replica_chunk.manifest_path).name),
                    manifest.model_dump_json(),
                )
            except UploadError as e:
                raise ChunkUploadError(chunk_id, "Manifest upload failed") from e

            # Next two steps are inapplicable to empty chunks.
            if not is_empty:
                # 3) Update the status and GCS URI in the database, marking
                # chunks with no table data as "staged" since they don't need
                # to go through the staging process in Dataflow
                gcs_uri = posixpath.join(self.config.bucket_name, gcs_prefix)
                status = ChunkStatus.UPLOADED if manifest.has_table_data() else ChunkStatus.STAGED
                updated_replica_chunk = replica_chunk.with_new_status(status).with_new_gcs_uri(
                    f"gs://{gcs_uri}"
                )
                try:
                    self._ppdb.store_chunk(updated_replica_chunk, True)
                    _LOG.info(
                        "Updated replica chunk %d in database with status '%s' and GCS URI: %s",
                        chunk_id,
                        status,
                        gcs_uri,
                    )
                except Exception as e:
                    raise ChunkUploadError(chunk_id, "Failed to update replica chunk in database") from e

                # 4) Publish Pub/Sub event to trigger the Dataflow staging
                # job for chunks with table data (skipped for updates-only)
                if manifest.has_table_data():
                    try:
                        self._post_to_stage_chunk_topic(gcs_uri, chunk_id)
                    except Exception as e:
                        raise ChunkUploadError(chunk_id, "Failed to publish staging message") from e

        except ChunkUploadError as err:
            try:
                # Recursively delete any uploaded files from cloud storage
                # on error. Locally written files should be used for debugging
                # errors rather than partially uploaded files in GCS.
                self._storage.delete_recursive(gcs_prefix)
            except DeleteError as cleanup_err:
                # Note (Python 3.11+): annotate without masking the
                # original error
                err.add_note(f"cleanup warning: failed to delete gs://{gcs_uri}: {cleanup_err}")
            raise

    def _post_to_stage_chunk_topic(self, gcs_uri: str, chunk_id: int) -> None:
        message = {
            "dataset": self.config.fq_dataset_id,
            "chunk_id": str(chunk_id),
            "folder": f"gs://{gcs_uri}",
        }
        _LOG.info("Publishing message to stage chunk topic: %s", message)
        self._publisher.publish(message).result(timeout=60)

    @staticmethod
    def _sleep_if(seconds: int) -> None:
        if seconds > 0:
            _LOG.debug("Sleeping for %d seconds...", seconds)
            time.sleep(seconds)
