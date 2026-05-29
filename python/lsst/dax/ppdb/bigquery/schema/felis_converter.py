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
    "FelisConverter",
    "FelisConverterError",
]

from collections.abc import Sequence
from typing import ClassVar

from felis import Schema
from felis.datamodel import DataType, Table
from google.cloud import bigquery


class FelisConverterError(RuntimeError):
    """Raised when Felis to BigQuery conversion fails."""


class FelisConverter:
    """Convert Felis objects to BigQuery.

    Parameters
    ----------
    schema
        Felis schema containing database objects to convert.
    """

    _TYPE_MAP: ClassVar[dict[DataType, str]] = {
        DataType.boolean: "BOOL",
        DataType.byte: "INT64",
        DataType.short: "INT64",
        DataType.int: "INT64",
        DataType.long: "INT64",
        DataType.float: "FLOAT64",
        DataType.double: "FLOAT64",
        DataType.char: "STRING",
        DataType.string: "STRING",
        DataType.unicode: "STRING",
        DataType.text: "STRING",
        DataType.binary: "BYTES",
        DataType.timestamp: "TIMESTAMP",
    }

    def __init__(self, schema: Schema) -> None:
        self._tables_by_name: dict[str, Table] = {table.name: table for table in schema.tables}

    def find_table(self, name: str) -> Table:
        """Find a Felis table by name.

        Parameters
        ----------
        name
            Name of the Felis table.

        Returns
        -------
        `felis.datamodel.Table`
            Matching Felis table.

        Raises
        ------
        FelisConverterError
            Raised if the table is not present in the schema.
        """
        table = self._tables_by_name.get(name)
        if table is None:
            raise FelisConverterError(f"Table {name!r} not found in Felis schema")
        return table

    def convert_tables(self, names: Sequence[str], dataset_fqn: str) -> list[bigquery.Table]:
        """Create BigQuery table objects for selected Felis table names.

        Parameters
        ----------
        names
            Sequence of Felis table names to convert.
        dataset_fqn
            Fully qualified BigQuery dataset name (project.dataset) used for
            table references.

        Returns
        -------
        list[`google.cloud.bigquery.Table`]
            Converted BigQuery table objects in the same order as ``names``.

        """
        return [self._to_bigquery_table(self.find_table(name), dataset_fqn=dataset_fqn) for name in names]

    def _to_bigquery_table(self, table: Table, dataset_fqn: str) -> bigquery.Table:
        """Convert a Felis table to a BigQuery table object.

        Parameters
        ----------
        table
            Felis table definition.
        dataset_fqn
            Fully qualified BigQuery dataset name (project.dataset) used for
            table references.

        Returns
        -------
        `google.cloud.bigquery.Table`
            BigQuery table object, not yet created remotely.

        Raises
        ------
        FelisConverterError
            Raised if any of the table's columns have unsupported datatypes for
            conversion.
        """
        schema_fields: list[bigquery.SchemaField] = []
        for column in table.columns:
            try:
                field_type = self._to_bigquery_type(column.datatype)
            except FelisConverterError as e:
                raise FelisConverterError(
                    f"Failed to map datatype for {table.name}.{column.name}: {e}"
                ) from e

            schema_fields.append(
                bigquery.SchemaField(
                    name=column.name,
                    field_type=field_type,
                    mode=self._to_bigquery_mode(column.nullable),
                )
            )

        table_fqn = f"{dataset_fqn}.{table.name}"
        return bigquery.Table(table_fqn, schema=schema_fields)

    @classmethod
    def _to_bigquery_type(cls, datatype: DataType) -> str:
        """Map a Felis datatype to a BigQuery type string.

        Parameters
        ----------
        datatype
            Felis datatype to map.

        Returns
        -------
        str
            Corresponding BigQuery type string.

        Raises
        ------
        FelisConverterError
            Raised if the datatype is not supported for conversion.
        """
        if datatype in cls._TYPE_MAP:
            return cls._TYPE_MAP[datatype]
        raise FelisConverterError(f"Unsupported Felis type {datatype!r}")

    @staticmethod
    def _to_bigquery_mode(nullable: bool) -> str:
        """Return the BigQuery mode string for a nullable column.

        Parameters
        ----------
        nullable
            Whether the column is nullable.

        Returns
        -------
        str
            "NULLABLE" if the column is nullable, otherwise "REQUIRED".
        """
        return "NULLABLE" if nullable else "REQUIRED"
