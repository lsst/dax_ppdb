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

__all__ = ["UpdatesTable"]

from collections.abc import Iterable
from typing import Any

from google.cloud import bigquery

from .expanded_update_record import ExpandedUpdateRecord


class UpdatesTable:
    """Manage the BigQuery table used for inserting expanded update records
    which contain one update per row.

    Parameters
    ----------
    client : `google.cloud.bigquery.Client`
        BigQuery client.
    project_id : `str`
        Google Cloud project ID.
    dataset_id : `str`
        BigQuery dataset ID.
    table_name : `str`, optional
        Name of the updates table. Defaults to ``"updates"``.
    latest_only_table_name : `str`, optional
        Name of the latest-only updates table. Defaults to
        ``"updates_latest_only"``.
    """

    _DEFAULT_TABLE_NAME: str = "updates"
    _DEFAULT_LATEST_ONLY_TABLE_NAME: str = "updates_latest_only"

    def __init__(
        self,
        client: bigquery.Client,
        project_id: str,
        dataset_id: str,
        table_name: str | None = None,
        latest_only_table_name: str | None = None,
    ) -> None:
        self._client: bigquery.Client = client
        table_name = table_name or self._DEFAULT_TABLE_NAME
        latest_only_table_name = latest_only_table_name or self._DEFAULT_LATEST_ONLY_TABLE_NAME
        self._table_fqn = f"{project_id}.{dataset_id}.{table_name}"
        self._latest_only_table_fqn = f"{project_id}.{dataset_id}.{latest_only_table_name}"

    @property
    def latest_only_table_fqn(self) -> str:
        """Fully-qualified BigQuery latest-only table name in the form
        ``"project.dataset.table"`` (`str`, read-only).
        """
        return self._latest_only_table_fqn

    @staticmethod
    def _make_record_key(record_id: Iterable[int]) -> str:
        """Make a string key from a list of integer ID values.

        Parameters
        ----------
        record_id : `Iterable`[`int`]
            The record ID as an iterable of integers.

        Returns
        -------
        id_str : `str`
            The record ID values joined by ``"-"``.
        """
        return "-".join(str(x) for x in record_id)

    @property
    def table_fqn(self) -> str:
        """Fully-qualified BigQuery table name in the form
        ``"project.dataset.table"`` (`str`, read-only).
        """
        return self._table_fqn

    def create(self) -> bigquery.Table:
        """Create the updates table.

        Returns
        -------
        table : `google.cloud.bigquery.Table`
            The created table.

        Raises
        ------
        google.api_core.exceptions.Conflict
            Raised if the table already exists.

        Notes
        -----
        Schema:

        - table_name: STRING (REQUIRED)
        - record_id: ARRAY<INT64> (REQUIRED)
        - record_key: STRING (REQUIRED)
        - field_name: STRING (REQUIRED)
        - value_json: JSON (REQUIRED)
        - replica_chunk_id: INT64 (REQUIRED)
        - update_order: INT64 (NULLABLE)
        - update_time_ns: INT64 (NULLABLE)
        """
        schema: list[bigquery.SchemaField] = [
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("record_id", "INT64", mode="REPEATED"),
            bigquery.SchemaField("record_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("field_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("value_json", "JSON", mode="REQUIRED"),
            bigquery.SchemaField("replica_chunk_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("update_order", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("update_time_ns", "INT64", mode="REQUIRED"),
        ]

        table = bigquery.Table(self._table_fqn, schema=schema)
        return self._client.create_table(table)

    def drop(self) -> None:
        """Drop the table if it exists."""
        self._client.delete_table(self._table_fqn, not_found_ok=True)

    def recreate(self) -> None:
        """Drop the table if it exists and then create it."""
        self.drop()
        self.create()

    def insert(self, records: Iterable[ExpandedUpdateRecord]) -> bigquery.LoadJob:
        """Insert `ExpandedUpdateRecord` rows into the updates table.

        Parameters
        ----------
        records : `Iterable` [ `ExpandedUpdateRecord` ]
            Iterable of update records to insert.

        Returns
        -------
        load_job : `google.cloud.bigquery.LoadJob`
            Completed BigQuery load job.

        Raises
        ------
        RuntimeError
            Raised if the BigQuery load job completes with errors.

        Notes
        -----
        This uses a batch load via `Client.load_table_from_json` (not streaming
        inserts). The table must already exist.
        """
        rows: list[dict[str, Any]] = [
            {
                "table_name": r.table_name,
                "record_id": r.record_id,
                "record_key": self._make_record_key(r.record_id),
                "field_name": r.field_name,
                "value_json": r.field_value,
                "replica_chunk_id": r.replica_chunk_id,
                "update_order": r.update_order,
                "update_time_ns": r.update_time_ns,
            }
            for r in records
        ]

        job = self._client.load_table_from_json(
            rows,
            self._table_fqn,
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
        ``(table_name, record_id, field_name)`` combination and write them to a
        new table.

        Parameters
        ----------
        target_table_fqn : `str`
            Target fully-qualified BigQuery table name in the form
            ``"project.dataset.table"``.

        Notes
        -----
        This keeps only the latest record with an update on an identical
        ``(table_name, record_id, field_name)``, based on the descending
        ordering of ``replica_chunk_id``, ``update_time_ns``, and
        ``update_order``.
        """
        query = f"""
        CREATE OR REPLACE TABLE `{self._latest_only_table_fqn}`
        AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY table_name, record_key, field_name
                    ORDER BY
                        replica_chunk_id DESC,
                        update_time_ns DESC,
                        update_order DESC
                ) as row_num
            FROM `{self._table_fqn}`
        )
        WHERE row_num = 1
        """

        job = self._client.query(query)
        job.result()
