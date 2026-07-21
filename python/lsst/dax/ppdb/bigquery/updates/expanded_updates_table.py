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

__all__ = ["ExpandedUpdatesTable"]

from collections.abc import Iterable
from typing import Any

from google.cloud import bigquery

from ..ppdb_bigquery_config import DatasetType, PpdbBigQueryConfig
from .expanded_update_record import ExpandedUpdateRecord


class ExpandedUpdatesTable:
    """Manage the BigQuery tables holding expanded update records.

    This class manages two related tables in the promotion dataset: the
    ``expanded_updates`` table, which contains one row per updated field, and
    the ``latest_only`` table, which is derived from it and contains only the
    latest update for each field.

    Parameters
    ----------
    client
        BigQuery client.
    config
        Configuration for the PPDB BigQuery interface.
    """

    _EXPANDED_UPDATES_NAME: str = "expanded_updates"
    _LATEST_ONLY_NAME: str = "latest_only"

    def __init__(
        self,
        client: bigquery.Client,
        config: PpdbBigQueryConfig,
    ) -> None:
        self._client: bigquery.Client = client
        self._expanded_updates_fqn = config.fqn_for(DatasetType.PROMOTION, self._EXPANDED_UPDATES_NAME)
        self._latest_only_fqn = config.fqn_for(DatasetType.PROMOTION, self._LATEST_ONLY_NAME)

    @property
    def expanded_updates_fqn(self) -> str:
        """Fully-qualified expanded updates table name (`str`, read-only)."""
        return self._expanded_updates_fqn

    @property
    def latest_only_fqn(self) -> str:
        """Fully-qualified latest-only table name (`str`, read-only)."""
        return self._latest_only_fqn

    @staticmethod
    def _make_record_key(record_id: Iterable[int]) -> str:
        """Make a string key from a list of integer ID values.

        Parameters
        ----------
        record_id
            The record ID as an iterable of integers.

        Returns
        -------
        `str`
            The record ID values joined by ``"-"``.
        """
        return "-".join(str(x) for x in record_id)

    def create(self, drop_if_exists: bool = False) -> bigquery.Table:
        """Create the expanded updates table.

        Parameters
        ----------
        drop_if_exists
            If True, drop the table if it already exists before creating it.

        Returns
        -------
        `~google.cloud.bigquery.table.Table`
            The created table.

        Raises
        ------
        google.api_core.exceptions.Conflict
            Raised if the table already exists and ``drop_if_exists`` is False.
        """
        if drop_if_exists:
            self._client.delete_table(self._expanded_updates_fqn, not_found_ok=True)
        schema: list[bigquery.SchemaField] = [
            bigquery.SchemaField("apdb_replica_chunk", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("record_id", "INT64", mode="REPEATED"),
            bigquery.SchemaField("record_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("field_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("value_json", "JSON", mode="REQUIRED"),
            bigquery.SchemaField("update_order", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("update_time_ns", "INT64", mode="REQUIRED"),
        ]
        table = bigquery.Table(self.expanded_updates_fqn, schema=schema)
        return self._client.create_table(table)

    def insert(self, records: Iterable[ExpandedUpdateRecord]) -> bigquery.LoadJob:
        """Insert `ExpandedUpdateRecord` rows into the expanded updates table.

        Parameters
        ----------
        records
            Iterable of expanded update records to insert.

        Returns
        -------
        `~google.cloud.bigquery.job.LoadJob`
            Completed BigQuery load job.

        Raises
        ------
        RuntimeError
            Raised if the BigQuery load job completes with errors.

        Notes
        -----
        This uses a batch load via ``Client.load_table_from_json`` (not
        streaming inserts). The table must already exist.
        """
        rows: list[dict[str, Any]] = [
            {
                "apdb_replica_chunk": r.apdb_replica_chunk,
                "table_name": r.table_name,
                "record_id": r.record_id,
                "record_key": self._make_record_key(r.record_id),
                "field_name": r.field_name,
                "value_json": r.field_value,
                "update_order": r.update_order,
                "update_time_ns": r.update_time_ns,
            }
            for r in records
        ]

        job = self._client.load_table_from_json(
            rows,
            self._expanded_updates_fqn,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ),
        )
        job.result()

        if job.errors:
            raise RuntimeError(f"BigQuery load failed: {job.errors}")

        return job

    def create_latest_only(self) -> None:
        """Select only the latest update for each unique
        ``(table_name, record_key, field_name)`` combination and write them to
        the latest-only table.

        Notes
        -----
        This keeps only the latest record with an update on an identical
        ``(table_name, record_key, field_name)``, based on the descending
        ordering of ``apdb_replica_chunk``, ``update_time_ns``, and
        ``update_order``.
        """
        query = f"""
        CREATE OR REPLACE TABLE `{self._latest_only_fqn}`
        AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY table_name, record_key, field_name
                    ORDER BY
                        apdb_replica_chunk DESC,
                        update_time_ns DESC,
                        update_order DESC
                ) as row_num
            FROM `{self._expanded_updates_fqn}`
        )
        WHERE row_num = 1
        """

        job = self._client.query(query)
        job.result()

    def cleanup(self) -> None:
        """Delete the expanded updates and latest-only tables."""
        for table_fqn in (self._expanded_updates_fqn, self._latest_only_fqn):
            self._client.delete_table(table_fqn, not_found_ok=True)
