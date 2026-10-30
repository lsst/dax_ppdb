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

__all__ = ["ChunkStager", "ChunkStagingError", "ChunkStagingRetryableError"]

import logging
import posixpath
from pathlib import PurePosixPath

import sqlalchemy.exc
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

from ..gcp import CloudEventLogger, handle_bigquery_error, handle_database_error
from .chunk_tracker import ChunkStatusError, ChunkTracker
from .manifest import Manifest
from .ppdb_bigquery import PpdbBigQuery
from .ppdb_replica_chunk_extended import ChunkStatus
from .table_refs import TableRefs


class ChunkStagingError(Exception):
    """Raised when a replica chunk could not be staged and retrying would not
    help.

    Parameters
    ----------
    chunk_id
        ID of the replica chunk.
    message
        Description of the failure.
    job_ids
        IDs of the BigQuery jobs run before the failure, by table name plus
        ``commit``.
    """

    def __init__(self, chunk_id: int, message: str, job_ids: dict[str, str] | None = None):
        super().__init__(f"Failed to stage chunk {chunk_id}: {message}")
        self.chunk_id = chunk_id
        self.job_ids = job_ids or {}


class ChunkStagingRetryableError(ChunkStagingError):
    """Raised when staging a replica chunk failed with a transient error and
    may succeed if retried.
    """


class ChunkStager:
    """Stages a replica chunk by loading its Parquet files into per-chunk
    landing tables and copying them into the staging tables in one
    transaction.

    Parameters
    ----------
    ppdb
        Interface to the PPDB.
    logger
        Logger for the staging request, e.g. with the ID of the Pub/Sub
        message that triggered it.
    folder
        URI of the folder containing the chunk's Parquet files.
    manifest
        The chunk's manifest, read from the folder by the caller. The chunk ID
        is taken from it.

    Notes
    -----
    An instance stages one chunk. BigQuery runs each job in the location of
    the tables it uses, so no location is given.
    """

    def __init__(self, ppdb: PpdbBigQuery, logger: CloudEventLogger, folder: str, manifest: Manifest):
        self._logger = logger
        self._tracker = ChunkTracker(ppdb)
        self._table_refs = TableRefs(ppdb.config)
        self._client = bigquery.Client(project=ppdb.config.project_id)
        self._chunk_id = int(manifest.replica_chunk_id)
        self._folder = folder

        # File names and manifest row counts, by staging table name. Each
        # Parquet file is loaded into the staging table named after it, e.g.
        # DiaObject.parquet into DiaObject.
        self._tables: dict[str, str] = {}
        self._row_counts: dict[str, int] = {}
        for file_name, entry in manifest.files.items():
            table_name = PurePosixPath(file_name).stem
            self._tables[table_name] = file_name
            self._row_counts[table_name] = entry.row_count

        # Staging table schemas, by table name. Read once the chunk is
        # claimed.
        self._staging_schemas: dict[str, list[bigquery.SchemaField]] = {}

        # IDs of the BigQuery jobs run, by table name plus "commit". Each ID is
        # added as soon as its job is submitted, so that the IDs are recorded
        # even if a job fails.
        self._job_ids: dict[str, str] = {}

    def stage_chunk(self) -> dict[str, str]:
        """Stage the replica chunk and record its status.

        Returns
        -------
        `dict` [ `str`, `str` ]
            IDs of the BigQuery jobs run, by table name plus ``commit``.

        Raises
        ------
        ChunkStatusError
            Raised if the chunk does not exist or can't be staged from its
            current status. Nothing is changed.
        ChunkStagingRetryableError
            Raised if staging failed with a transient error. The chunk is set
            to ``retrying``, or left unchanged if it could not be claimed.
        ChunkStagingError
            Raised if staging failed with any other error. The chunk is set to
            ``failed``, or left unchanged if it could not be claimed.

        Notes
        -----
        If a status can't be recorded after the chunk is claimed, the
        tracker's error is raised instead, with the staging error as its
        context, and the chunk is left in ``staging``.
        """
        # Claim the chunk by setting it to staging. Only one attempt can claim
        # it, so a duplicate or concurrent message gets a ChunkStatusError,
        # which is passed on unchanged.
        try:
            self._tracker.update_status(self._chunk_id, ChunkStatus.STAGING)
        except ChunkStatusError as e:
            self._logger.log_event(
                logging.WARNING,
                "Chunk can't be staged from its current status",
                "chunk_rejected",
                error=e,
                chunk_id=self._chunk_id,
            )
            raise
        except sqlalchemy.exc.SQLAlchemyError as e:
            # The status is unchanged, so a retry can claim the chunk.
            # handle_database_error logs the error.
            message = f"could not claim chunk: {e}"
            if handle_database_error(self._logger, e):
                raise ChunkStagingRetryableError(self._chunk_id, message) from e
            raise ChunkStagingError(self._chunk_id, message) from e
        self._logger.log_event(
            logging.INFO, "Staging chunk", "chunk_staging", chunk_id=self._chunk_id, folder=self._folder
        )

        # Load the chunk's files into landing tables. Nothing has been written
        # to the staging tables yet, so a failure leaves nothing to undo.
        try:
            # The landing tables are loaded with the staging tables' schemas,
            # so that the data is checked against them before the copy into
            # staging.
            for table_name in self._tables:
                staging_table = self._client.get_table(self._table_refs.staging(table_name))
                self._staging_schemas[table_name] = staging_table.schema
            landing_tables = self._copy_to_landing()
        except Exception as e:
            raise self._fail("copying to landing", e) from e

        # Copy the landing tables into the staging tables in one transaction.
        # It either commits completely or rolls back, so a failure leaves the
        # staging tables unchanged.
        try:
            self._copy_to_staging(landing_tables)
        except Exception as e:
            raise self._fail("copying to staging", e) from e

        self._tracker.update_status(self._chunk_id, ChunkStatus.STAGED, bigquery_job_ids=self._job_ids)
        self._logger.log_event(
            logging.INFO,
            "Staged chunk",
            "chunk_staged",
            chunk_id=self._chunk_id,
            bigquery_job_ids=self._job_ids,
        )
        return self._job_ids

    def _copy_to_landing(self) -> dict[str, str]:
        """Load each of the chunk's Parquet files into its own landing table,
        with the schema of the staging table it is copied into later.

        Returns
        -------
        `dict` [ `str`, `str` ]
            Fully qualified names of the loaded landing tables, by staging
            table name.

        Notes
        -----
        Landing tables of an earlier attempt are overwritten. The landing
        dataset's table expiration removes them later.
        """
        # All jobs are submitted before any is waited on, so that they run
        # concurrently.
        landing_tables: dict[str, str] = {}
        jobs: dict[str, bigquery.LoadJob] = {}
        for table_name, file_name in self._tables.items():
            landing_tables[table_name] = self._table_refs.landing(table_name, self._chunk_id)
            jobs[table_name] = self._client.load_table_from_uri(
                posixpath.join(self._folder, file_name),
                landing_tables[table_name],
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    schema=self._staging_schemas[table_name],
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ),
                job_id_prefix=f"stage_chunk_{self._chunk_id}_{table_name}_",
            )
            self._job_ids[table_name] = jobs[table_name].job_id
        for job in jobs.values():
            job.result()
        return landing_tables

    def _copy_to_staging(self, landing_tables: dict[str, str]) -> None:
        """Copy the chunk's landing tables into the staging tables in one
        transaction, so the chunk is staged completely or not at all.

        Parameters
        ----------
        landing_tables
            Fully qualified names of the landing tables, by staging table
            name.

        Notes
        -----
        Each ``ASSERT`` rolls back the transaction unless the staging table
        ends up with exactly the manifest's row count for the chunk, which
        also catches a chunk that was already staged.
        """
        # Columns are listed explicitly because the landing and staging
        # tables can order them differently.
        statements = ["BEGIN TRANSACTION;"]
        parameters = [bigquery.ScalarQueryParameter("chunk_id", "INT64", self._chunk_id)]
        for index, (table_name, landing) in enumerate(landing_tables.items()):
            staging = self._table_refs.staging(table_name)
            columns = ", ".join(f"`{field.name}`" for field in self._staging_schemas[table_name])
            statements += [
                f"INSERT INTO `{staging}` ({columns}) SELECT {columns} FROM `{landing}`;",
                f"ASSERT (SELECT COUNT(*) FROM `{staging}` WHERE apdb_replica_chunk = @chunk_id) "
                f"= @rows_{index} AS 'row count in staging table {table_name} does not match the manifest';",
            ]
            parameters.append(
                bigquery.ScalarQueryParameter(f"rows_{index}", "INT64", self._row_counts[table_name])
            )
        statements.append("COMMIT TRANSACTION;")

        commit = self._client.query(
            "\n".join(statements),
            job_config=bigquery.QueryJobConfig(query_parameters=parameters),
            job_id_prefix=f"stage_chunk_{self._chunk_id}_commit_",
        )
        self._job_ids["commit"] = commit.job_id
        commit.result()

    def _fail(self, step: str, e: Exception) -> ChunkStagingError:
        """Set the chunk to ``retrying`` or ``failed`` after a staging step
        failed, and return the error to raise.

        Parameters
        ----------
        step
            The step that failed, used as a prefix of the error message.
        e
            The exception raised by the step.

        Returns
        -------
        `ChunkStagingError`
            `ChunkStagingRetryableError` if the chunk was set to
            ``retrying``, otherwise `ChunkStagingError`.
        """
        # Google API errors carry the request in their string form, which is
        # too long for the chunk table.
        message = f"{step}: {e.message if isinstance(e, GoogleAPICallError) else e}"

        # handle_bigquery_error logs the error. Anything not retryable, e.g. a
        # missing table or a failed ASSERT, fails the chunk.
        if handle_bigquery_error(self._logger, e, f"{step} for chunk {self._chunk_id}"):
            self._tracker.update_status(self._chunk_id, ChunkStatus.RETRYING, bigquery_job_ids=self._job_ids)
            return ChunkStagingRetryableError(self._chunk_id, message, self._job_ids)
        self._tracker.update_status(
            self._chunk_id, ChunkStatus.FAILED, error_message=message, bigquery_job_ids=self._job_ids
        )
        return ChunkStagingError(self._chunk_id, message, self._job_ids)
