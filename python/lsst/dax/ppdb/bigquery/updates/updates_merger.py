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

__all__ = [
    "DiaForcedSourceUpdatesMerger",
    "DiaObjectUpdatesMerger",
    "DiaSourceUpdatesMerger",
    "UpdatesMerger",
]

from google.cloud import bigquery

from ..sql_resource import SqlResource


class UpdatesMerger:
    """Abstract base class for merging expanded update records into target
    tables in BigQuery.

    Parameters
    ----------
    table_name_format
        Optional format string for the target table name. The
        class-level ``TABLE_NAME`` will be substituted into ``{}``
        (e.g., ``"_{}_promoted_tmp"`` produces
        ``"_DiaObject_promoted_tmp"``).
        If not provided, ``TABLE_NAME`` is used as-is.
    """

    TABLE_NAME: str
    """Logical name of the target table this merger applies to
    (e.g., 'DiaObject').
    """

    SQL_RESOURCE_NAME: str
    """Base name of the SQL file (without .sql extension) containing the MERGE
    statement for this merger. The SQL file must be located in the
    `lsst.dax.ppdb.config.sql` package."""

    def __init__(self, table_name_format: str | None = None) -> None:
        if table_name_format:
            self._target_table_name = table_name_format.format(self.TABLE_NAME)
        else:
            self._target_table_name = self.TABLE_NAME

    @property
    def target_table_name(self) -> str:
        """Name of the target table to which this merger applies."""
        return self._target_table_name

    def merge(
        self, *, client: bigquery.Client, updates_table_fqn: str, target_dataset_fqn: str
    ) -> bigquery.QueryJob:
        """Apply updates from the updates table specified by
        `updates_table_fqn` to the target table in the `target_dataset_fqn`
        dataset.

        Parameters
        ----------
        client
            BigQuery client.
        updates_table_fqn
            Fully-qualified BigQuery table name containing updates.
        target_dataset_fqn
            Fully-qualified BigQuery dataset name containing the target table.

        Returns
        -------
        job : `google.cloud.bigquery.job.QueryJob`
            The completed BigQuery job.
        """
        sql = SqlResource(
            self.SQL_RESOURCE_NAME,
            format_args={
                "updates_table": updates_table_fqn,
                "target_dataset": target_dataset_fqn,
                "target_table": self.target_table_name,
            },
        ).sql
        job = client.query(sql)
        job.result()

        return job


class DiaObjectUpdatesMerger(UpdatesMerger):
    """Merger for DiaObject updates."""

    TABLE_NAME = "DiaObject"
    SQL_RESOURCE_NAME = "merge_diaobject_updates"


class DiaSourceUpdatesMerger(UpdatesMerger):
    """Merger for DiaSource updates."""

    TABLE_NAME = "DiaSource"
    SQL_RESOURCE_NAME = "merge_diasource_updates"


class DiaForcedSourceUpdatesMerger(UpdatesMerger):
    """Merger for DiaForcedSource updates."""

    TABLE_NAME = "DiaForcedSource"
    SQL_RESOURCE_NAME = "merge_diaforcedsource_updates"
