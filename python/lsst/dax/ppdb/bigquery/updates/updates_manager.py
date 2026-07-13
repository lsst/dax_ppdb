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

__all__ = ["UpdatesManager", "UpdatesManagerError"]

import logging
from collections.abc import Sequence

from google.cloud import bigquery

from lsst.dax.apdb.apdbUpdateRecord import ApdbUpdateRecord

from ..ppdb_bigquery_config import DatasetType, PpdbBigQueryConfig
from ..ppdb_replica_chunk_extended import PpdbReplicaChunkExtended
from .expanded_update_record import ExpandedUpdateRecord
from .expanded_updates_table import ExpandedUpdatesTable
from .updates_merger import (
    DiaForcedSourceUpdatesMerger,
    DiaObjectUpdatesMerger,
    DiaSourceUpdatesMerger,
    UpdatesMerger,
)

_LOG = logging.getLogger(__name__)


class UpdatesManagerError(Exception):
    """Base exception for errors related to the updates process."""


class UpdatesManager:
    """Class responsible for managing the process of applying updates to the
    PPDB database.

    Raw update records are staged into the ``updates`` table in the staging
    dataset by the Dataflow staging job. This manager reads those raw records,
    expands them into field-level rows in the ``expanded_updates`` table in the
    promotion dataset, selects only the latest update for each field into the
    ``latest_only`` table, and finally merges those into the target tables.

    Parameters
    ----------
    config
        Configuration for the PPDB BigQuery interface.
    target_dataset_fqn
        Fully qualified name of the dataset containing the target tables to
        merge updates into, or `None` to use the promotion dataset from the
        configuration.
    mergers
        Merger instances to apply, or `None` to use the default set of mergers
        for the standard target tables.
    """

    def __init__(
        self,
        config: PpdbBigQueryConfig,
        target_dataset_fqn: str | None = None,
        mergers: Sequence[UpdatesMerger] | None = None,
    ) -> None:
        self._updates_table_fqn = config.fqn_for(DatasetType.STAGING, "updates")

        # Set the merger instances for handling each target table, falling back
        # to a default set of mergers if none were provided.
        if mergers is not None:
            self._mergers = tuple(mergers)
        else:
            self._mergers = (
                DiaObjectUpdatesMerger(),
                DiaSourceUpdatesMerger(),
                DiaForcedSourceUpdatesMerger(),
            )

        self._bq_client = bigquery.Client()

        # This is the table that will be used to store the expanded update
        # records and generate the latest only updates table.
        self._expanded_updates_table = ExpandedUpdatesTable(self._bq_client, config)

        # Set the target dataset FQN for the mergers to use.
        if target_dataset_fqn is None:
            # By default, use the promotion dataset as the merge target.
            self._target_dataset_fqn = config.fqn_for(DatasetType.PROMOTION)
        else:
            self._target_dataset_fqn = target_dataset_fqn

    def apply_updates(self, replica_chunks: Sequence[PpdbReplicaChunkExtended]) -> None:
        """Apply update records from replica chunk data to target tables in
        BigQuery.

        Parameters
        ----------
        replica_chunks
            The replica chunks with the update records.

        Raises
        ------
        UpdatesManagerError
            Raised if any step of the updates process fails.
        """
        # Filter the list of chunks to only those with updates and skip
        # processing entirely if there are none. Caller may also already have
        # done this but we don't rely on it.
        update_chunks = [chunk for chunk in replica_chunks if chunk.update_count > 0]
        if not update_chunks:
            _LOG.info("No update records found in the provided replica chunks")
            return

        # Log the total number of update records. We already checked above
        # that there is at least one update record in the batch of chunks.
        total_update_count = sum(chunk.update_count for chunk in update_chunks)
        _LOG.info("Processing %d update records from %d chunks", total_update_count, len(update_chunks))

        # Read the raw update records from the staging table and expand them
        # into the promotion expanded updates table.
        try:
            self._build_expanded_updates_table(update_chunks)
        except Exception as e:
            raise UpdatesManagerError("Failed to build expanded updates table") from e

        # Select only the latest update records into a new table.
        try:
            self._expanded_updates_table.create_latest_only()
        except Exception as e:
            raise UpdatesManagerError("Failed to create latest-only updates table") from e

        # Merge the latest-only updates into the target tables.
        try:
            self._merge_updates(self._expanded_updates_table.latest_only_fqn)
        except Exception as e:
            raise UpdatesManagerError("Failed to merge updates into target tables") from e

    def _build_expanded_updates_table(self, chunks: Sequence[PpdbReplicaChunkExtended]) -> None:
        """Read raw update records for the given chunks from the staging
        updates table, expand them, and load them into the promotion expanded
        updates table.

        Parameters
        ----------
        chunks
            Replica chunks with update_count > 0.
        """
        chunk_ids = [chunk.id for chunk in chunks]
        raw_records = self._read_staged_updates(chunk_ids)

        expanded_records: list[ExpandedUpdateRecord] = []
        for apdb_replica_chunk, record in raw_records:
            expanded_records.extend(ExpandedUpdateRecord.from_update_record(record, apdb_replica_chunk))

        _LOG.info(
            "Expanded %d raw update records into %d field-level records",
            len(raw_records),
            len(expanded_records),
        )

        # Create the new expanded updates table, dropping any existing one.
        self._expanded_updates_table.create(drop_if_exists=True)

        # Insert the expanded records into the new table.
        self._expanded_updates_table.insert(expanded_records)

    def _read_staged_updates(self, apdb_replica_chunks: Sequence[int]) -> list[tuple[int, ApdbUpdateRecord]]:
        """Read staged update records for the given replica chunks.

        Parameters
        ----------
        apdb_replica_chunks
            Replica chunk IDs whose update records should be read.

        Returns
        -------
        `list` [ `tuple` [ `int`, `ApdbUpdateRecord` ] ]
            Pairs of ``(apdb_replica_chunk, record)`` for each row, with the
            record reconstructed from its stored JSON payload.
        """
        if not apdb_replica_chunks:
            return []

        query = f"""
        SELECT update_time_ns, update_order, json_payload, apdb_replica_chunk
        FROM `{self._updates_table_fqn}`
        WHERE apdb_replica_chunk IN UNNEST(@ids)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", list(apdb_replica_chunks))]
        )
        rows = self._bq_client.query(query, job_config=job_config).result()

        records: list[tuple[int, ApdbUpdateRecord]] = []
        for row in rows:
            record = ApdbUpdateRecord.from_json(
                row["update_time_ns"], row["update_order"], row["json_payload"]
            )
            records.append((row["apdb_replica_chunk"], record))
        return records

    def _merge_updates(self, target_table_fqn: str) -> None:
        """Merge the latest-only updates into the target tables using the
        table-specific merger classes.

        Parameters
        ----------
        target_table_fqn
            Fully qualified name of the latest-only updates table for the
            merge operation.
        """
        for merger in self._mergers:
            try:
                _LOG.debug(
                    "Merging updates into target table '%s' using %s", target_table_fqn, type(merger).__name__
                )
                job = merger.merge(
                    client=self._bq_client,
                    updates_table_fqn=target_table_fqn,
                    target_dataset_fqn=self._target_dataset_fqn,
                )

                self._log_job_stats(job, target_table_fqn)

            except Exception as e:
                raise UpdatesManagerError(f"Failed to merge updates using {type(merger).__name__}") from e

    def _log_job_stats(self, job: bigquery.QueryJob, target_table_fqn: str) -> None:
        """Log relevant statistics for the merge operation from a BigQuery job.

        Parameters
        ----------
        job
            The BigQuery job with the statistics to log.
        target_table_fqn
            Fully qualified name of the target table for the merge operation.
        """
        total = job.num_dml_affected_rows
        dml_stats = job.dml_stats
        if dml_stats is None:
            _LOG.warning(
                "Merge job for target table '%s' does not have DML statistics available",
                target_table_fqn,
            )
            return
        inserted = dml_stats.inserted_row_count
        updated = dml_stats.updated_row_count
        deleted = dml_stats.deleted_row_count

        _LOG.info(
            "Finished merging updates into '%s': total=%d, inserted=%d, updated=%d, deleted=%d",
            target_table_fqn,
            total,
            inserted,
            updated,
            deleted,
        )
