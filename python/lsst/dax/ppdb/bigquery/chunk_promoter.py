# This file is part of dax_ppdbx_gcp
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

__all__ = [
    "ChunkPromoter",
    "ChunkPromotionError",
    "NoPromotableChunksError",
]

import logging
from collections.abc import Sequence

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from lsst.dax.apdb import ApdbTables

from ..gcp import CloudEventLogger
from .ppdb_bigquery import PpdbBigQuery, UpdatableField
from .ppdb_bigquery_config import PpdbBigQueryConfig
from .ppdb_replica_chunk_extended import ChunkStatus, PpdbReplicaChunkExtended
from .sql_resource import SqlResource
from .table_refs import TableRefs
from .updates.updates_manager import UpdatesManager


class NoPromotableChunksError(Exception):
    """Raised when an empty chunk list is passed to ``promote_chunks``.

    Callers should check for an empty list before calling if they want to
    handle this condition gracefully rather than catching this exception.
    """


class ChunkPromotionError(Exception):
    """Base exception for errors related to the chunk promotion process."""


class ChunkPromoter:
    """Class to promote replica chunks in BigQuery.

    Parameters
    ----------
    ppdb
        Interface to the PPDB in BigQuery.
    logger
        Cloud event logger used to emit structured log events.
    table_names
        Table names to promote or None to use a default set.
    """

    _DEFAULT_TABLE_NAMES = (
        ApdbTables.DiaObject.value,
        ApdbTables.DiaSource.value,
        ApdbTables.DiaForcedSource.value,
    )

    _UPDATES_TABLE_NAME = "updates"

    def __init__(
        self,
        ppdb: PpdbBigQuery,
        logger: CloudEventLogger,
        table_names: Sequence[str] | None = None,
    ):
        self._ppdb = ppdb
        self._logger = logger
        self._table_names = tuple(table_names) if table_names is not None else self._DEFAULT_TABLE_NAMES
        if len(self._table_names) == 0:
            raise ChunkPromotionError("table_names must not be empty")

        self._bq_client = bigquery.Client(project=self.config.project_id)
        internal_dataset_id = f"{self.config.project_id}.{self.config.datasets.internal}"
        self._dataset = self._bq_client.get_dataset(internal_dataset_id)
        self._location = self._dataset.location
        self._table_refs = TableRefs(self.config)
        self._updates_manager = UpdatesManager(self.config)

        self._promotable_chunks: list[PpdbReplicaChunkExtended] = []

    @property
    def config(self) -> PpdbBigQueryConfig:
        """Config associated with this instance (`PpdbBigQueryConfig`)."""
        return self._ppdb.config

    @property
    def promotable_chunks(self) -> list[PpdbReplicaChunkExtended]:
        """List of promotable chunks (`list` [ `PpdbReplicaChunkExtended` ],
        read-only).
        """
        return self._promotable_chunks

    @property
    def table_refs(self) -> TableRefs:
        """Table references (`TableRefs`, read-only)."""
        return self._table_refs

    @property
    def table_names(self) -> tuple[str, ...]:
        """Table names to promote (`tuple` [`str`], read-only)."""
        return self._table_names

    def promote_chunks(self, chunks: list[PpdbReplicaChunkExtended]) -> None:
        """Promote APDB replica chunks into production by executing a series of
        steps in BigQuery.

        Parameters
        ----------
        chunks
            List of `PpdbReplicaChunkExtended` objects to promote. Must not be
            empty.

        Raises
        ------
        ChunkPromotionError
            Raised if any error occurs during execution of the promotion
            steps in BigQuery.
        NoPromotableChunksError
            Raised if ``chunks`` is empty.
        """
        if not chunks:
            raise NoPromotableChunksError("No promotable chunks provided for promotion")

        chunk_ids = [c.id for c in chunks]
        self._logger.log_event(
            logging.INFO,
            "Starting chunk promotion",
            "chunk_promotion_started",
            chunk_count=len(chunks),
            chunk_ids=chunk_ids,
        )

        # Set the list of promotable chunks for use in the promotion phases.
        self._promotable_chunks = chunks

        # Execute the promotion steps in order.
        try:
            # Copy prod tables to temp tables and insert staged data.
            self._copy_staging_to_promotion()

            # Fill in validityEndMjdTai for DiaObjects in the temp table.
            self._fill_diaobject_validity_end()

            # Apply record updates to the temp tables.
            self._updates_manager.apply_updates(self.promotable_chunks)

            # Promote the temp tables to prod using atomic table swaps.
            self._copy_promotion_to_internal()

            # Create the copy of the DiaObject table in the public dataset
            # containing only the most recent versions.
            self._create_diaobject_latest()

            # Delete the staged chunks from the staging tables.
            self._delete_staged_chunks()

            # Mark the chunks promoted in the database.
            self._mark_chunks_promoted()

        except Exception as e:
            raise ChunkPromotionError("Chunk promotion failed") from e
        finally:
            # Always execute the cleanup, even if there were errors.
            try:
                self._cleanup()
            except Exception as e:
                self._logger.log_event(
                    logging.ERROR,
                    "Chunk promotion cleanup failed",
                    "chunk_promotion_cleanup_failed",
                    error=e,
                )

        self._logger.log_event(
            logging.INFO,
            "Completed chunk promotion",
            "chunk_promotion_completed",
            chunk_count=len(chunks),
        )

    def _run_job(
        self, label: str, sql: str, job_config: bigquery.QueryJobConfig | None = None
    ) -> bigquery.job.QueryJob:
        """Run a BigQuery job with the given SQL and configuration.

        Parameters
        ----------
        label
            A label for the job, typically indicating the type of operation
            (e.g., "insert", "delete", "copy").
        sql
            The SQL query to execute.
        job_config
            Configuration for the job, such as query parameters or write
            dispositions. If not provided, a default configuration will be
            used.

        Returns
        -------
        `google.cloud.bigquery.job.QueryJob`
            The BigQuery job object representing the executed query.
        """
        job = self._bq_client.query(sql, job_config=job_config, location=self._location)
        job.result()
        self._log_bigquery_job(job, label)
        return job

    def _log_bigquery_job(
        self,
        job: bigquery.job.QueryJob
        | bigquery.job.LoadJob
        | bigquery.job.CopyJob
        | bigquery.job.ExtractJob
        | bigquery.job.UnknownJob,
        label: str,
    ) -> None:
        """Log details of a completed BigQuery job."""
        self._logger.log_event(
            logging.DEBUG,
            "BigQuery job completed",
            "bigquery_job_completed",
            label=label,
            job_id=job.job_id,
            location=job.location,
            state=job.state,
            bytes_processed=getattr(job, "total_bytes_processed", None),
            bytes_billed=getattr(job, "total_bytes_billed", None),
            slot_millis=getattr(job, "slot_millis", None),
            dml_rows=getattr(job, "num_dml_affected_rows", None),
            reference_tables=getattr(job, "referenced_tables", None),
        )

    def _log_dml_rows_affected(
        self, job: bigquery.job.QueryJob, event_name: str, message: str, **fields: object
    ) -> None:
        """Log rows affected by a DML job, sourced the same way as
        `_log_bigquery_job` so counts are consistent across job types.
        """
        rows_affected = job.num_dml_affected_rows
        if rows_affected is not None:
            self._logger.log_event(logging.INFO, message, event_name, rows_affected=rows_affected, **fields)
        else:
            self._logger.log_event(
                logging.WARNING,
                f"{message}, but affected row count is unavailable",
                f"{event_name}_unavailable",
                **fields,
            )

    def _copy_staging_to_promotion(self) -> None:
        """Build promotion tables by cloning the current internal tables and
        inserting staged rows for the promotable chunks.
        """
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ids", "INT64", [c.id for c in self._promotable_chunks])
            ]
        )

        for table_name in self.table_names:
            # Build fully qualified table names which will be used in the
            # queries.
            staging_table_fqn = self.table_refs.staging(table_name)
            promotion_table_fqn = self.table_refs.promotion(table_name)
            internal_table_fqn = self.table_refs.internal(table_name)

            # Drop existing promotion table, if it exists.
            self._run_job("drop_promotion_if_exists", f"DROP TABLE IF EXISTS `{promotion_table_fqn}`")

            # Clone the current internal table structure and data (zero-copy).
            self._run_job(
                "clone_internal_to_promotion",
                f"CREATE OR REPLACE TABLE `{promotion_table_fqn}` CLONE `{internal_table_fqn}`",
            )

            # Build target column list for SQL statement from the internal
            # table's schema. This will include the geo_point column.
            target_schema = self._bq_client.get_table(internal_table_fqn).schema
            target_names = [target_column.name for target_column in target_schema]
            target_list_sql = ", ".join(f"`{column_name}`" for column_name in target_names)

            # Build source column list for SQL statement from the target list,
            # converting ra/dec to ST_GEOGPOINT for geo_point column.
            source_list_sql = ", ".join(
                "ST_GEOGPOINT(s.`ra`, s.`dec`)" if column_name == "geo_point" else f"s.`{column_name}`"
                for column_name in target_names
            )

            # Insert staged rows into the promotion table. If the staging
            # and internal schemas do not match, this will fail.
            sql = f"""
            INSERT INTO `{promotion_table_fqn}` ({target_list_sql})
            SELECT {source_list_sql}
            FROM `{staging_table_fqn}` AS s
            WHERE s.apdb_replica_chunk IN UNNEST(@ids)
            """
            self._logger.log_event(
                logging.DEBUG,
                "Built SQL for inserting staged rows",
                "staging_insert_sql_built",
                promotion_table=promotion_table_fqn,
                sql=sql,
            )
            self._run_job("insert_staged_to_promotion", sql, job_config=job_cfg)

    def _fill_diaobject_validity_end(self) -> None:
        """Fill null ``validityEndMjdTai`` values for promoted DiaObject
        records.
        """
        job_name = "fill_diaobject_validity_end"

        target_table_fqn = self.table_refs.promotion(ApdbTables.DiaObject.value)

        staging_table = ApdbTables.DiaObject.value
        staging_table_fqn = self.table_refs.staging(staging_table)

        sql = SqlResource(
            job_name,
            format_args={
                "target_table": target_table_fqn,
                "staging_table": staging_table_fqn,
            },
        ).sql
        job = self._run_job(job_name, sql)

        self._log_dml_rows_affected(
            job, "diaobject_validity_end_filled", "Finished filling DiaObject validity end", job_name=job_name
        )

    def _copy_promotion_to_internal(self) -> None:
        """Swap each internal table with its corresponding promotion table by
        replacing internal contents in a single atomic copy job. This preserves
        schema, partitioning, and clustering with zero-copy when in the same
        dataset.
        """
        for table_name in self.table_names:
            promotion_ref = self.table_refs.promotion(table_name)
            internal_ref = self.table_refs.internal(table_name)

            # Ensure promotion table exists.
            try:
                self._bq_client.get_table(promotion_ref)
            except NotFound as e:
                raise RuntimeError(f"Missing promotion table: {promotion_ref}") from e

            # Perform an atomic, zero-copy replacement of internal with its
            # corresponding promotion table.
            copy_cfg = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = self._bq_client.copy_table(
                promotion_ref, internal_ref, job_config=copy_cfg, location=self._location
            )
            job.result()
            self._log_bigquery_job(job, "copy_promotion_to_internal")

    def _create_diaobject_latest(self) -> None:
        """Create the copy of the DiaObject table in the public dataset
        containing only the most recent versions.
        """
        internal_table_fqn = self.table_refs.internal(ApdbTables.DiaObject.value)
        public_table_fqn = self.table_refs.public(ApdbTables.DiaObject.value)

        job_name = "create_diaobject_latest"

        self._run_job(
            job_name,
            f"""CREATE OR REPLACE TABLE `{public_table_fqn}`
            CLUSTER BY geo_point AS
            SELECT * EXCEPT (validityEndMjdTai)
            FROM `{internal_table_fqn}`
            WHERE validityEndMjdTai IS NULL""",
        )

    def _delete_staged_chunks(self) -> None:
        """Delete only rows for the promoted replica chunk IDs from each
        staging table.
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ids", "INT64", [c.id for c in self._promotable_chunks])
            ]
        )

        # Include the updates table in the list of staging tables from which
        # to delete the promoted chunks.
        for table_name in (*self.table_names, self._UPDATES_TABLE_NAME):
            staging_table_fqn = self.table_refs.staging(table_name)
            try:
                sql = f"DELETE FROM `{staging_table_fqn}` WHERE apdb_replica_chunk IN UNNEST(@ids)"
                job = self._run_job("delete_staged_chunks", sql, job_config=job_config)
                self._log_dml_rows_affected(
                    job,
                    "staged_chunks_deleted",
                    "Deleted chunk(s) from staging table",
                    staging_table=staging_table_fqn,
                )
            except NotFound:
                self._logger.log_event(
                    logging.WARNING,
                    "Staging table does not exist, skipping delete",
                    "staging_table_not_found",
                    staging_table=staging_table_fqn,
                )

    def _mark_chunks_promoted(self) -> None:
        """Mark the replica chunks as promoted in the database."""
        promoted = [chunk.with_new_status(ChunkStatus.PROMOTED) for chunk in self.promotable_chunks]
        self._ppdb.update_chunks(promoted, fields={UpdatableField.STATUS})

    def _cleanup(self) -> None:
        """Cleanup state after executing the promotion."""
        # Delete the promotion tables.
        for table_name in self.table_names:
            promotion_ref = self.table_refs.promotion(table_name)
            self._bq_client.delete_table(promotion_ref, not_found_ok=True)
            self._logger.log_event(
                logging.DEBUG,
                "Dropped promotion table (if it existed)",
                "promotion_table_dropped",
                table=promotion_ref,
            )

        # Cleanup the updates manager.
        self._updates_manager.cleanup()

        # Reset the chunk list.
        self._promotable_chunks = []
