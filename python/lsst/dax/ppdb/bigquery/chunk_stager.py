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

__all__ = ["ChunkStager", "ChunkStagingError", "ChunkStagingRetryableError"]

import logging
import posixpath
from collections.abc import Iterable
from datetime import timedelta

from google.api_core.exceptions import GoogleAPICallError, RetryError, ServerError, TooManyRequests
from google.cloud import bigquery
from google.cloud.bigquery.retry import DEFAULT_RETRY

from ..gcp import DATABASE_ERRORS, NETWORK_ERRORS, CloudEventLogger, handle_database_error
from .manifest import InvalidManifestError, Manifest
from .ppdb_bigquery import PpdbBigQuery
from .ppdb_replica_chunk_extended import (
    ChunkStatus,
    ChunkStatusError,
    PpdbReplicaChunkExtended,
    UpdatableField,
)
from .table_refs import TableRefs


class ChunkStagingError(Exception):
    """Raised when a replica chunk could not be staged and retrying would not
    help.
    """


class ChunkStagingRetryableError(Exception):
    """Raised when staging a replica chunk failed with a transient error
    which may succeed if retried.
    """


class ChunkStager:
    """Stages a replica chunk by loading its Parquet files into per-chunk
    landing tables and then copying them into the staging tables in one
    transaction.

    Parameters
    ----------
    ppdb
        Interface to the PPDB.
    client
        BigQuery client that runs the jobs.
    check_timeout
        Limit on the query that checks the staging tables for a chunk's rows.
        BigQuery cancels the query if it runs longer.
    job_timeout
        Limit on each load job and on the transaction. BigQuery cancels a job
        that runs longer.
    retry_timeout
        Limit on how long each BigQuery API request is retried after a
        temporary error.

    Notes
    -----
    An instance can be created once and used for every chunk. BigQuery API
    requests are retried for up to ``retry_timeout``, but a failed job is
    never resubmitted.
    """

    def __init__(
        self,
        ppdb: PpdbBigQuery,
        client: bigquery.Client,
        check_timeout: timedelta,
        job_timeout: timedelta,
        retry_timeout: timedelta,
    ):
        self._ppdb = ppdb
        self._table_refs = TableRefs(ppdb.config)
        self._client = client
        self._check_timeout_ms = int(check_timeout.total_seconds() * 1000)
        self._job_timeout_ms = int(job_timeout.total_seconds() * 1000)
        self._retry = DEFAULT_RETRY.with_timeout(retry_timeout.total_seconds())

    def stage_chunk(self, logger: CloudEventLogger, chunk_id: int, folder: str) -> str:
        """Stage a replica chunk and record its status.

        Parameters
        ----------
        logger
            Logger for the staging request, e.g. with the ID of the Pub/Sub
            message that triggered it.
        chunk_id
            ID of the replica chunk.
        folder
            URI of the folder containing the chunk's manifest and Parquet
            files.

        Returns
        -------
        `str`
            ID of the BigQuery job that copied the chunk into the staging
            tables.

        Raises
        ------
        ChunkStatusError
            Raised if the chunk can't be staged from its current status.
        ChunkStagingRetryableError
            Raised if staging failed with a temporary error, so the message
            should be retried.
        ChunkStagingError
            Raised if the chunk does not exist, if staging failed with a
            permanent error, or if the chunk's status could not be recorded.

        Notes
        -----
        Only an ``uploaded`` or ``retrying`` chunk is staged. Once staging has
        started, temporary errors set the chunk to ``retrying``, and any other
        error sets it to ``failed``.

        The chunk is claimed by setting it to ``staging`` in one conditional
        update, so only one of several concurrent deliveries stages it. How
        this is done may be refactored in DM-56938.
        """
        stageable = (ChunkStatus.UPLOADED, ChunkStatus.RETRYING)

        # Check that the chunk can be staged and set it to staging.
        try:
            chunk = self._ppdb.find_chunk_by_id(chunk_id)
            if chunk is None:
                logger.log_event(logging.WARNING, "Chunk does not exist", "chunk_not_found")
                raise ChunkStagingError(f"Replica chunk {chunk_id} does not exist")
            if chunk.status not in stageable:
                raise ChunkStatusError(f"Replica chunk {chunk_id} can't be staged from {chunk.status}")
            # Clear the error and staging job ID of an earlier attempt.
            chunk = (
                chunk.with_new_status(ChunkStatus.STAGING)
                .with_new_error_message(None)
                .with_new_staging_job_id(None)
            )
            # Claim the chunk. This may be refactored in DM-56938.
            self._ppdb.update_chunks(
                [chunk],
                {UpdatableField.STATUS, UpdatableField.ERROR_MESSAGE, UpdatableField.STAGING_JOB_ID},
                from_statuses=stageable,
            )
        except ChunkStatusError as e:
            logger.log_event(
                logging.WARNING, "Chunk can't be staged from its current status", "chunk_rejected", error=e
            )
            raise
        except DATABASE_ERRORS as e:
            # The chunk status has likely not been changed yet, so a temporary
            # error is retried by Pub/Sub.
            if handle_database_error(logger, e):
                raise ChunkStagingRetryableError(str(e)) from e
            raise ChunkStagingError(str(e)) from e
        logger.log_event(logging.INFO, "Staging chunk", "chunk_staging", folder=folder)

        # Stage the chunk and set its final status.
        try:
            try:
                manifest = Manifest.from_uri(folder)
                # Check that the manifest is for this chunk.
                if manifest.replica_chunk_id != str(chunk_id):
                    raise InvalidManifestError(
                        f"Manifest in {folder} is for chunk {manifest.replica_chunk_id}"
                    )

                table_files = manifest.table_files
                self._check_not_already_staged(chunk_id, table_files)

                # Get the staging table schemas to use for the landing tables.
                schemas = {
                    table_name: self._client.get_table(
                        self._table_refs.staging(table_name), retry=self._retry
                    ).schema
                    for table_name in table_files
                }

                landing_tables = self._copy_to_landing(logger, chunk_id, folder, table_files, schemas)
                staging_job_id = self._copy_to_staging(logger, chunk, landing_tables, schemas, table_files)
            except (TooManyRequests, ServerError) as e:
                # Temporary error, so Pub/Sub retries the message.
                self._record_failure(logger, chunk, ChunkStatus.RETRYING, e.message, e)
                raise ChunkStagingRetryableError(e.message) from e
            except NETWORK_ERRORS as e:
                # Network error, so Pub/Sub retries the message.
                self._record_failure(logger, chunk, ChunkStatus.RETRYING, str(e), e)
                raise ChunkStagingRetryableError(str(e)) from e
            except GoogleAPICallError as e:
                if any(error.get("reason") in ("quotaExceeded", "rateLimitExceeded") for error in e.errors):
                    # BigQuery quota or rate limit, so Pub/Sub retries.
                    self._record_failure(logger, chunk, ChunkStatus.RETRYING, e.message, e)
                    raise ChunkStagingRetryableError(e.message) from e
                # Any other Google Cloud error, e.g. a failed ASSERT.
                self._record_failure(logger, chunk, ChunkStatus.FAILED, e.message, e)
                raise ChunkStagingError(e.message) from e
            except Exception as e:
                # Any other error, e.g. a bad manifest or rows already staged.
                self._record_failure(logger, chunk, ChunkStatus.FAILED, str(e), e)
                raise ChunkStagingError(str(e)) from e

            self._ppdb.update_chunks([chunk.with_new_status(ChunkStatus.STAGED)], {UpdatableField.STATUS})
        except (LookupError, *DATABASE_ERRORS) as e:
            # The chunk is left in staging. A retry would be rejected.
            logger.log_event(
                logging.ERROR,
                "Could not record the chunk's status",
                "chunk_status_not_recorded",
                error=e,
            )
            raise ChunkStagingError(f"Could not record the chunk's status: {e}") from e

        # Drop the landing tables. If this fails, the tables expire later.
        for landing_table in landing_tables.values():
            try:
                self._client.delete_table(landing_table, not_found_ok=True, retry=self._retry)
            except (GoogleAPICallError, *NETWORK_ERRORS) as e:
                logger.log_event(
                    logging.WARNING,
                    "Could not drop landing table",
                    "landing_table_not_dropped",
                    error=e,
                    landing_table=landing_table,
                )

        logger.log_event(logging.INFO, "Staged chunk", "chunk_staged", staging_job_id=staging_job_id)
        return staging_job_id

    def _record_failure(
        self,
        logger: CloudEventLogger,
        chunk: PpdbReplicaChunkExtended,
        status: ChunkStatus,
        message: str,
        error: Exception,
    ) -> None:
        """Log a staging failure and set the chunk's status and error message.

        Parameters
        ----------
        logger
            Logger for the staging request.
        chunk
            The replica chunk.
        status
            The chunk's new status, ``retrying`` or ``failed``.
        message
            The error message to store with the chunk.
        error
            The error that made staging fail.
        """
        if status == ChunkStatus.RETRYING:
            logger.log_event(logging.WARNING, "Staging failed, will retry", "chunk_retrying", error=error)
        else:
            logger.log_event(logging.ERROR, "Staging failed", "chunk_failed", error=error)
        self._ppdb.update_chunks(
            [chunk.with_new_status(status).with_new_error_message(message)],
            {UpdatableField.STATUS, UpdatableField.ERROR_MESSAGE},
        )

    def _check_not_already_staged(self, chunk_id: int, tables: Iterable[str]) -> None:
        """Check that the staging tables have no rows for a chunk, so that its
        data is not staged twice.

        Parameters
        ----------
        chunk_id
            ID of the replica chunk.
        tables
            Names of the chunk's staging tables.

        Raises
        ------
        ChunkStagingError
            Raised if any staging table already has rows for the chunk.
        """
        # Count the chunk's rows in each staging table.
        selects = [
            f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count "
            f"FROM `{self._table_refs.staging(table_name)}` WHERE apdb_replica_chunk = @chunk_id"
            for table_name in tables
        ]
        query = "\nUNION ALL\n".join(selects)
        job = self._client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("chunk_id", "INT64", chunk_id)],
                job_timeout_ms=self._check_timeout_ms,
            ),
            job_id_prefix=f"stage_chunk_{chunk_id}_check_",
            retry=self._retry,
            job_retry=None,
        )

        existing: dict[str, int] = {}
        for row in job.result(retry=self._retry):
            if row.row_count > 0:
                existing[row.table_name] = row.row_count
        if existing:
            raise ChunkStagingError(f"Staging tables already have rows for the chunk: {existing}")

    def _copy_to_landing(
        self,
        logger: CloudEventLogger,
        chunk_id: int,
        folder: str,
        table_files: dict[str, tuple[str, int]],
        schemas: dict[str, list[bigquery.SchemaField]],
    ) -> dict[str, str]:
        """Load each of a chunk's Parquet files into its own landing table.

        Parameters
        ----------
        logger
            Logger for the staging request.
        chunk_id
            ID of the replica chunk.
        folder
            URI of the folder containing the chunk's Parquet files.
        table_files
            Parquet file names and row counts, by staging table name.
        schemas
            Staging table schemas, by table name.

        Returns
        -------
        `dict` [ `str`, `str` ]
            Fully qualified names of the loaded landing tables, by staging
            table name.
        """
        # Submit a load job for each Parquet file.
        landing_tables: dict[str, str] = {}
        jobs: dict[str, bigquery.LoadJob] = {}
        for table_name, (file_name, _) in table_files.items():
            # Name the landing table, e.g. ppdb_landing.DiaObject_chunk_42.
            landing_tables[table_name] = self._table_refs.landing(table_name, chunk_id)
            jobs[table_name] = self._client.load_table_from_uri(
                posixpath.join(folder, file_name),
                landing_tables[table_name],
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    schema=[field for field in schemas[table_name] if field.name != "geo_point"],
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    job_timeout_ms=self._job_timeout_ms,
                ),
                job_id_prefix=f"stage_chunk_{chunk_id}_{table_name}_",
                retry=self._retry,
            )
        job_ids = {table_name: job.job_id for table_name, job in jobs.items()}
        logger.log_event(
            logging.INFO,
            "Submitted landing load jobs",
            "landing_jobs_submitted",
            landing_tables=landing_tables,
            job_ids=job_ids,
        )
        # Wait for the jobs only after all are submitted, so they run
        # concurrently.
        for job in jobs.values():
            job.result(retry=self._retry)
        logger.log_event(logging.INFO, "Landing load jobs finished", "landing_jobs_finished", job_ids=job_ids)
        return landing_tables

    def _copy_to_staging(
        self,
        logger: CloudEventLogger,
        chunk: PpdbReplicaChunkExtended,
        landing_tables: dict[str, str],
        schemas: dict[str, list[bigquery.SchemaField]],
        table_files: dict[str, tuple[str, int]],
    ) -> str:
        """Copy a chunk's landing tables into the staging tables in one
        transaction, so the chunk is staged completely or not at all.

        Parameters
        ----------
        logger
            Logger for the staging request.
        chunk
            The replica chunk.
        landing_tables
            Fully qualified names of the landing tables, by staging table
            name.
        schemas
            Staging table schemas, by table name.
        table_files
            Parquet file names and row counts from the chunk's manifest, by
            staging table name.

        Returns
        -------
        `str`
            ID of the BigQuery job that ran the transaction.

        Raises
        ------
        ChunkStagingError
            Raised if the transaction's outcome is unknown: submitting it
            failed with a temporary error, or waiting for it failed before the
            job was known to be finished. A job that was being waited on is
            cancelled. The chunk is failed rather than retried, so that a retry
            can't stage it twice.

        Notes
        -----
        If submitting the transaction ran out of retries because BigQuery kept
        rate limiting the request, the last refusal is raised, so the chunk is
        retried. An earlier attempt could in principle have created the job
        with its response lost, but the next attempt's check for rows already
        in staging catches a job that has committed.

        If the job has finished with an error, its transaction was rolled back,
        and the error is raised unchanged, so the caller decides whether to
        retry. If the job has finished without an error, its transaction
        committed, and an error from fetching its result afterwards is only
        logged.
        """
        # Build the transaction. For each table, copy the landing table into
        # staging, then check the chunk's row count against the manifest.
        statements = ["BEGIN TRANSACTION;"]
        for table_name, landing in landing_tables.items():
            staging = self._table_refs.staging(table_name)
            names = [field.name for field in schemas[table_name]]
            target_columns = ", ".join(f"`{name}`" for name in names)
            source_columns = ", ".join(
                "ST_GEOGPOINT(`ra`, `dec`)" if name == "geo_point" else f"`{name}`" for name in names
            )
            statements += [
                f"INSERT INTO `{staging}` ({target_columns}) SELECT {source_columns} FROM `{landing}`;",
                f"ASSERT (SELECT COUNT(*) FROM `{staging}` WHERE apdb_replica_chunk = @chunk_id) "
                f"= {int(table_files[table_name][1])} "
                f"AS 'row count in staging table {table_name} does not match the manifest';",
            ]
        statements.append("COMMIT TRANSACTION;")

        # If any statement fails, including an ASSERT, the whole transaction is
        # rolled back.
        try:
            job = self._client.query(
                "\n".join(statements),
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("chunk_id", "INT64", chunk.id)],
                    job_timeout_ms=self._job_timeout_ms,
                ),
                job_id_prefix=f"stage_chunk_{chunk.id}_staging_",
                retry=self._retry,
                job_retry=None,
            )
        except (ServerError, *NETWORK_ERRORS) as e:
            cause = e.cause if isinstance(e, RetryError) else None
            if isinstance(cause, TooManyRequests) or (
                isinstance(cause, GoogleAPICallError)
                and any(error.get("reason") == "rateLimitExceeded" for error in cause.errors)
            ):
                # BigQuery kept refusing the request, so the chunk is retried.
                raise cause from e
            # The job may have been created, so the chunk isn't retried.
            raise ChunkStagingError(
                f"Could not submit the staging transaction. It may have been created and may still "
                f"commit (job ID prefix stage_chunk_{chunk.id}_staging_): {e}"
            ) from e

        # Record the job ID before waiting on the job.
        try:
            self._ppdb.update_chunks(
                [chunk.with_new_staging_job_id(job.job_id)], {UpdatableField.STAGING_JOB_ID}
            )
        except (LookupError, *DATABASE_ERRORS) as e:
            logger.log_event(
                logging.WARNING,
                "Could not record the staging job ID",
                "staging_job_id_not_recorded",
                error=e,
                staging_job_id=job.job_id,
            )

        # Wait for the transaction. A failed ASSERT raises here.
        try:
            job.result(retry=self._retry)
        except (GoogleAPICallError, *NETWORK_ERRORS) as e:
            if job.done(reload=False):
                if job.error_result is None:
                    # The transaction committed, and only fetching its result
                    # failed.
                    logger.log_event(
                        logging.WARNING,
                        "Could not fetch the result of the committed staging job",
                        "staging_result_not_fetched",
                        error=e,
                        staging_job_id=job.job_id,
                    )
                    return job.job_id
                # The job failed, and its transaction was rolled back.
                raise
            # The job may still be running, so cancel it.
            try:
                job.cancel(retry=self._retry)
            except (GoogleAPICallError, *NETWORK_ERRORS) as cancel_error:
                logger.log_event(
                    logging.WARNING,
                    "Could not cancel the staging job",
                    "staging_job_not_cancelled",
                    error=cancel_error,
                    staging_job_id=job.job_id,
                )
            raise ChunkStagingError(f"Could not get the result of staging job {job.job_id}: {e}") from e
        return job.job_id
