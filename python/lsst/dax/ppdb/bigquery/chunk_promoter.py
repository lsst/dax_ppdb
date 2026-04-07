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
    "NoPromotableChunksError",
]

import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from lsst.dax.apdb import ApdbTables

from .ppdb_bigquery import PpdbBigQuery, PpdbBigQueryConfig
from .query_runner import QueryRunner
from .table_refs import TableRefs
from .updates.updates_manager import UpdatesManager


class NoPromotableChunksError(Exception):
    """Exception raised when there are no promotable chunks available.

    This is not really an error condition, but it is useful for managing the
    control flow of the promotion process when there are no chunks to promote.
    """


class ChunkPromoterError(Exception):
    """Base exception for errors related to the chunk promotion process."""


class ChunkPromoter:
    """Class to promote replica chunks in BigQuery.

    Parameters
    ----------
    ppdb : `PpdbBigQuery`
        Interface to the PPDB in BigQuery.
    table_names : `list`[`str`], optional
        List of table names to promote or if None a default list will be used.
    """

    _DEFAULT_TABLE_NAMES = [
        ApdbTables.DiaObject.value,
        ApdbTables.DiaSource.value,
        ApdbTables.DiaForcedSource.value,
    ]

    def __init__(
        self,
        ppdb: PpdbBigQuery,
        table_names: list[str] | None = None,
    ):
        self._ppdb = ppdb
        self._runner = QueryRunner(self.config.project_id, self.config.dataset_id)
        self._table_names = table_names if table_names is not None else self._DEFAULT_TABLE_NAMES
        self._bq_client = bigquery.Client(project=self.config.project_id)

        self._table_refs = TableRefs(
            project_id=self.config.project_id,
            dataset_id=self.config.dataset_id,
            table_names=tuple(self._table_names),
        )

        # Build a mapping of phases to run during the promotion process, not
        # including cleanup, which is executed separately
        _phase_methods = [
            self._copy_to_promoted_tmp,
            self._apply_record_updates,
            self._promote_tmp_to_prod,
            self._delete_staged_chunks,
            self._mark_chunks_promoted,
        ]
        self._phases = {m.__name__.lstrip("_"): m for m in _phase_methods}

        self._promotable_chunks: list[int] = []

    @property
    def config(self) -> PpdbBigQueryConfig:
        """Config associated with this instance (`PpdbBigQueryConfig`)."""
        return self._ppdb.config

    @property
    def promotable_chunks(self) -> list[int]:
        """List of promotable chunks (`list` [ `int` ], read-only)."""
        return self._promotable_chunks

    @property
    def table_refs(self) -> TableRefs:
        """Table references (`TableRefs`, read-only)."""
        return self._table_refs

    def _copy_to_promoted_tmp(self) -> None:
        """Build ``_{table_name}_promoted_tmp`` efficiently by cloning prod and
        inserting only staged rows for the given replica chunk IDs.
        """
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", self.promotable_chunks)]
        )

        for prod_ref, tmp_ref, stage_ref in zip(
            self._table_refs.prod, self._table_refs.promoted_tmp, self._table_refs.staging, strict=True
        ):
            # Drop any existing tmp table (should not exist but just to be
            # safe)
            self._runner.run_job("drop_tmp", f"DROP TABLE IF EXISTS `{tmp_ref}`")

            # Clone prod table structure and data (zero-copy)
            self._runner.run_job("clone_prod", f"CREATE TABLE `{tmp_ref}` CLONE `{prod_ref}`")

            # Build ordered target list from the cloned tmp schema
            tmp_schema = self._bq_client.get_table(tmp_ref).schema
            target_names = [f.name for f in tmp_schema if f.name != "apdb_replica_chunk"]
            target_list_sql = ", ".join(f"`{n}`" for n in target_names)

            # Build source list, handling geo_point conversion
            source_list_sql = ", ".join(
                "ST_GEOGPOINT(s.`ra`, s.`dec`)" if n == "geo_point" else f"s.`{n}`" for n in target_names
            )

            # Insert staged rows into tmp, excluding apdb_replica_chunk column
            sql = f"""
            INSERT INTO `{tmp_ref}` ({target_list_sql})
            SELECT {source_list_sql}
            FROM `{stage_ref}` AS s
            WHERE s.apdb_replica_chunk IN UNNEST(@ids)
            """
            logging.debug("SQL for inserting staged rows into %s: %s", tmp_ref, sql)
            self._runner.run_job("insert_staged_to_tmp", sql, job_config=job_cfg)

    def _promote_tmp_to_prod(self) -> None:
        """Swap each prod table with its corresponding *_promoted_tmp by
        replacing prod contents in a single atomic copy job. This preserves
        schema, partitioning, and clustering with zero-copy when in the same
        dataset.
        """
        for prod_ref, tmp_ref in zip(self._table_refs.prod, self._table_refs.promoted_tmp, strict=True):
            # Ensure tmp exists
            try:
                self._bq_client.get_table(tmp_ref)
            except NotFound as e:
                raise RuntimeError(f"Missing tmp table for promotion: {tmp_ref}") from e

            # Atomic zero-copy replacement of prod with tmp
            copy_cfg = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = self._bq_client.copy_table(
                tmp_ref, prod_ref, job_config=copy_cfg, location=self._runner.location
            )
            job.result()
            QueryRunner.log_job(job, "promote_tmp_to_prod")

    def _cleanup(self) -> None:
        """Drop the promotion temporary tables."""
        for tmp_ref in self._table_refs.promoted_tmp:
            self._bq_client.delete_table(tmp_ref, not_found_ok=True)
            logging.debug("Dropped %s (if it existed)", tmp_ref)

    def _delete_staged_chunks(self) -> None:
        """Delete only rows for the promoted replica chunk IDs from each
        staging table.
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", self.promotable_chunks)]
        )

        for staging_ref in self._table_refs.staging:
            try:
                sql = f"DELETE FROM `{staging_ref}` WHERE apdb_replica_chunk IN UNNEST(@ids)"
                self._runner.run_job("delete_staged_chunks", sql, job_config=job_config)
                logging.debug(
                    "Deleted %d chunk(s) from staging table %s", len(self.promotable_chunks), staging_ref
                )
            except NotFound:
                logging.warning("Staging table %s does not exist, skipping delete", staging_ref)

    def _apply_record_updates(self) -> None:
        """Apply record updates to the promoted temporary tables."""
        updates_manager = UpdatesManager(
            self._ppdb.config,
            table_name_format="_{}_promoted_tmp",
        )
        # TODO: It would be preferable if the extended replica chunk interface
        # included a flag indicating if there were updates or not so that this
        # list could be pre-filtered.
        replica_chunks = self._ppdb.get_replica_chunks_ext_by_ids(self.promotable_chunks)
        updates_manager.apply_updates(replica_chunks)

    def _mark_chunks_promoted(self) -> None:
        """Mark the replica chunks as promoted in the database."""
        self._ppdb.mark_chunks_promoted(self.promotable_chunks)

    # TODO: It would be preferable if this method received a list of
    # `PpdbReplicaChunkExtended` objects rather than integer IDs. This could
    # be easily provided with the `PpdbBigquery` interface.
    def promote_chunks(self, chunks: list[int]) -> None:
        """Promote APDB replica chunks into production by executing a series of
        phases.
        """
        if not chunks:
            raise NoPromotableChunksError("No promotable chunks provided for promotion")

        logging.info("Starting promotion of %d chunk(s): %s", len(chunks), chunks)

        # Set the list of promotable chunks for use in the promotion phases.
        self._promotable_chunks = chunks

        # Execute the promotion phases in order.
        try:
            for name, phase in self._phases.items():
                logging.debug("Starting phase: %s", name)
                phase()
                logging.debug("Completed phase: %s", name)
        except Exception as e:
            raise ChunkPromoterError("Chunk promotion failed") from e
        finally:
            # Always execute the cleanup, even if there were errors.
            try:
                self._cleanup()
            except Exception:
                logging.exception("Cleanup of chunk promotion failed")

        logging.info("Completed promotion of %d chunk(s)", len(chunks))
