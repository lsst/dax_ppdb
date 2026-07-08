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

from .ppdb_bigquery import PpdbBigQuery
from .ppdb_bigquery_config import PpdbBigQueryConfig
from .ppdb_replica_chunk_extended import ChunkStatus, PpdbReplicaChunkExtended
from .query_runner import QueryRunner
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
    table_names
        Table names to promote or None to use a default set.
    """

    _DEFAULT_TABLE_NAMES = (
        ApdbTables.DiaObject.value,
        ApdbTables.DiaSource.value,
        ApdbTables.DiaForcedSource.value,
    )

    _PROMOTED_TMP_SUFFIX = "_promoted_tmp"

    def __init__(
        self,
        ppdb: PpdbBigQuery,
        table_names: Sequence[str] | None = None,
    ):
        self._ppdb = ppdb

        self._runner = QueryRunner(self.config.project_id, self.config.datasets.internal)

        self._table_names = tuple(table_names) if table_names is not None else self._DEFAULT_TABLE_NAMES
        if len(self._table_names) == 0:
            raise ChunkPromotionError("table_names must not be empty")

        self._table_refs = TableRefs(self.config)

        self._bq_client = bigquery.Client(project=self.config.project_id)

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

    @classmethod
    def _promoted_tmp_name(cls, table_name: str) -> str:
        """Return the promoted temporary table name for a given base table
        name.
        """
        return f"{table_name}{cls._PROMOTED_TMP_SUFFIX}"

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
        logging.info("Starting promotion of %d chunk(s): %s", len(chunks), chunk_ids)

        # Set the list of promotable chunks for use in the promotion phases.
        self._promotable_chunks = chunks

        # Execute the promotion steps in order.
        try:
            # Copy prod tables to temp tables and insert staged data.
            self._copy_to_promoted_tmp()

            # Fill in validityEndMjdTai for DiaObjects in the temp table.
            self._fill_diaobject_validity_end()

            # Apply record updates to the temp tables.
            self._apply_record_updates()

            # Promote the temp tables to prod using atomic table swaps.
            self._promote_tmp_to_prod()

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
            except Exception:
                logging.exception("Cleanup of chunk promotion failed")

        logging.info("Completed promotion of %d chunk(s)", len(chunks))

    def _copy_to_promoted_tmp(self) -> None:
        """Build temporary tables by cloning the current prod tables and
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
            internal_table_fqn = self.table_refs.internal(table_name)
            tmp_table_fqn = self.table_refs.internal(self._promoted_tmp_name(table_name))

            # Drop existing promoted tmp table, if it exists.
            self._runner.run_job("drop_tmp", f"DROP TABLE IF EXISTS `{tmp_table_fqn}`")

            # Clone prod table structure and data (zero-copy).
            self._runner.run_job("clone_prod", f"CREATE TABLE `{tmp_table_fqn}` CLONE `{internal_table_fqn}`")

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

            # Insert staged rows into the promoted tmp table. If the staging
            # and internal schemas do not match, this will fail.
            sql = f"""
            INSERT INTO `{tmp_table_fqn}` ({target_list_sql})
            SELECT {source_list_sql}
            FROM `{staging_table_fqn}` AS s
            WHERE s.apdb_replica_chunk IN UNNEST(@ids)
            """
            logging.debug("SQL for inserting staged rows into %s: %s", tmp_table_fqn, sql)
            self._runner.run_job("insert_staged_to_tmp", sql, job_config=job_cfg)

    def _apply_record_updates(self) -> None:
        """Apply record updates to the promoted temporary tables."""
        updates_manager = UpdatesManager(
            self._ppdb.config,
            table_name_format="{}" + self._PROMOTED_TMP_SUFFIX,
        )

        # Apply the updates for the chunks. The manager will skip the process
        # entirely if there are no updates, so we don't need to check that
        # here.
        updates_manager.apply_updates(self.promotable_chunks)

    def _fill_diaobject_validity_end(self) -> None:
        """Fill null ``validityEndMjdTai`` values for promoted DiaObject
        records.
        """
        job_name = "fill_diaobject_validity_end"

        target_table = self._promoted_tmp_name(ApdbTables.DiaObject.value)
        target_table_fqn = self.table_refs.internal(target_table)

        staging_table = ApdbTables.DiaObject.value
        staging_table_fqn = self.table_refs.staging(staging_table)

        sql = SqlResource(
            job_name,
            format_args={
                "target_table": target_table_fqn,
                "staging_table": staging_table_fqn,
            },
        ).sql
        job = self._runner.run_job(job_name, sql)

        # Log the number of number of rows updated.
        dml_stats = job.dml_stats
        if dml_stats:
            updated = dml_stats.updated_row_count
            logging.info(
                "Finished job '%s' with %d rows updated",
                job_name,
                updated,
            )
        else:
            logging.warning("Finished job '%s' but DML stats are not available", job_name)

    def _promote_tmp_to_prod(self) -> None:
        """Swap each prod table with its corresponding *_promoted_tmp by
        replacing prod contents in a single atomic copy job. This preserves
        schema, partitioning, and clustering with zero-copy when in the same
        dataset.
        """
        for table_name in self.table_names:
            tmp_ref = self.table_refs.internal(self._promoted_tmp_name(table_name))
            prod_ref = self.table_refs.internal(table_name)

            # Ensure tmp exists.
            try:
                self._bq_client.get_table(tmp_ref)
            except NotFound as e:
                raise RuntimeError(f"Missing tmp table for promotion: {tmp_ref}") from e

            # Perform an atomic, zero-copy replacement of prod with temp.
            copy_cfg = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = self._bq_client.copy_table(
                tmp_ref, prod_ref, job_config=copy_cfg, location=self._runner.location
            )
            job.result()
            QueryRunner.log_job(job, "promote_tmp_to_prod")

    def _delete_staged_chunks(self) -> None:
        """Delete only rows for the promoted replica chunk IDs from each
        staging table.
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ids", "INT64", [c.id for c in self._promotable_chunks])
            ]
        )

        for table_name in self.table_names:
            staging_table_fqn = self.table_refs.staging(table_name)
            try:
                sql = f"DELETE FROM `{staging_table_fqn}` WHERE apdb_replica_chunk IN UNNEST(@ids)"
                self._runner.run_job("delete_staged_chunks", sql, job_config=job_config)
                logging.debug(
                    "Deleted %d chunk(s) from staging table %s",
                    len(self.promotable_chunks),
                    staging_table_fqn,
                )
            except NotFound:
                logging.warning("Staging table %s does not exist, skipping delete", staging_table_fqn)

    def _mark_chunks_promoted(self) -> None:
        """Mark the replica chunks as promoted in the database."""
        promoted = [chunk.with_new_status(ChunkStatus.PROMOTED) for chunk in self.promotable_chunks]
        self._ppdb.update_chunks(promoted, fields={"status"})

    def _cleanup(self) -> None:
        """Cleanup state after executing the promotion."""
        # Delete the tmp tables.
        for table_name in self.table_names:
            tmp_ref = self.table_refs.internal(self._promoted_tmp_name(table_name))
            self._bq_client.delete_table(tmp_ref, not_found_ok=True)
            logging.debug("Dropped %s (if it existed)", tmp_ref)

        # Reset the chunk list.
        self._promotable_chunks = []
