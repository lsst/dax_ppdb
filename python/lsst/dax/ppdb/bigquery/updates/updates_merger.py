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

__all__ = ["UpdatesMerger"]

from collections.abc import Iterable

from google.cloud import bigquery

from lsst.dax.apdb import ApdbTables


class UpdatesMerger:
    """Abstract base class for merging expanded update records into target
    tables in BigQuery.

    Parameters
    ----------
    table : `ApdbTables`
        Table that receives updates.
    id_fields : `~collections.abc.Iterable` [`str`]
        Names of the identifying fields in that table.
    payload_fields : `~collections.abc.Iterable` [`tuple`[`str`, `str`]]
        Tuples of (name, type) of the fields being updates. Type is BigQuery
        type name (e.g. "INT64").
    table_name_format : `str`, optional
        Optional format string for the target table name. The
        class-level ``TABLE_NAME`` will be substituted into ``{}``
        (e.g., ``"_{}_promoted_tmp"`` produces
        ``"_DiaObject_promoted_tmp"``).
        If not provided, ``TABLE_NAME`` is used as-is.
    """

    _TYPE_MAP = {"int": "INT64", "float": "FLOAT64", "bool": "BOOL", "str": "STRING"}

    def __init__(
        self,
        table: ApdbTables,
        id_fields: Iterable[str],
        payload_fields: Iterable[tuple[str, str]],
        table_name_format: str | None = None,
    ) -> None:
        self.table = table
        self.id_fields = tuple(id_fields)
        self.payload_fields = tuple(
            (field, self._TYPE_MAP[field_type]) for field, field_type in payload_fields
        )
        if table_name_format:
            self._target_table_name = table_name_format.format(table.table_name())
        else:
            self._target_table_name = table.table_name()

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
        client : `google.cloud.bigquery.Client`
            BigQuery client.
        updates_table_fqn : `str`
            Fully-qualified BigQuery table name containing updates.
        target_dataset_fqn : `str`
            Fully-qualified BigQuery dataset name containing the target table.

        Returns
        -------
        job : `google.cloud.bigquery.job.QueryJob`
            The completed BigQuery job.
        """
        sql = self._generate_merge(updates_table_fqn, target_dataset_fqn)
        job = client.query(sql)
        job.result()

        return job

    def _generate_merge(self, updates_table_fqn: str, target_dataset_fqn: str) -> str:
        """Generate SQL statement to merge updates."""
        # Merge statement will look like this:
        #
        # MERGE `{target_dataset}.{target_table}` T
        # USING (
        #   WITH patch AS (
        #     SELECT
        #       -- repeat for each ID field
        #       record_id[OFFSET({i})] AS {idField},
        #
        #       -- repeat for each payload field
        #       ANY_VALUE(
        #         CASE WHEN field_name = '{payloadField}'
        #              THEN CAST(JSON_VALUE(value_json) AS {payloadType})
        #         END
        #       ) AS payloadField{i}_value,
        #       COUNTIF(field_name = '{payloadField}') > 0
        #               AS {payloadField}_present,
        #
        #     FROM `{updates_table}`
        #     WHERE table_name = '{table_name}'
        #       AND field_name IN ('{payloadField}', ...)
        #     GROUP BY {idField}, ...
        #   )
        #   SELECT * FROM patch
        # ) P
        # ON T.{idField} = P.{idField}, ...
        # WHEN MATCHED THEN
        # UPDATE SET
        #   -- repeat for each payloadField
        #   {payloadField} = IF(
        #       P.{payloadField}_present,
        #       P.{payloadField}_value,
        #       T.{payloadField}
        #   ),
        # ;

        select_ids = ",".join(f"record_id[OFFSET({i})] AS {field}" for i, field in enumerate(self.id_fields))
        select_payloads = ",".join(
            "ANY_VALUE("
            f"CASE WHEN field_name = '{field}' THEN  CAST(JSON_VALUE(value_json) AS {field_type}) END"
            f") AS {field}_value,"
            f"COUNTIF(field_name = '{field}) > 0) AS {field}_present"
            for field, field_type in self.payload_fields
        )

        id_fields = ",".join(f"{field}" for field in self.id_fields)
        payload_fields = ",".join(f"'{field}'" for field, _ in self.payload_fields)
        where = f"table_name = '{self.table.table_name()}' AND field_name IN ({payload_fields})"

        on_constraint = ",".join(f"T.{field} = P.{field}" for field in self.id_fields)

        updates = ",".join(
            f"{field} = IF(P.{field}_present, P.{field}_value, T.{field})" for field, _ in self.payload_fields
        )

        query = f"""
        MERGE `{target_dataset_fqn}.{self.target_table_name}`
        USING (
            WITH patch AS (
                SELECT {select_ids}, {select_payloads}
                FROM {updates_table_fqn}
                WHERE {where}
                GROUP BY {id_fields}
            )
            SELECT * from patch
        ) P
        ON {on_constraint}
        WHEN MATCHED THEN
        UPDATE SET {updates}
        """

        return query
