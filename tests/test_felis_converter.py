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

import unittest

from felis import datamodel

from lsst.dax.ppdb.bigquery.schema.felis_converter import FelisConverter, FelisConverterError


class FelisConverterTestCase(unittest.TestCase):
    """Unit tests for FelisConverter."""

    def setUp(self) -> None:
        """Build a minimal converter used by missing-table tests."""
        table = datamodel.Table(
            name="KnownTable",
            id="#KnownTable",
            columns=[
                datamodel.Column(
                    name="id",
                    id="#KnownTable.id",
                    datatype=datamodel.DataType.int,
                    nullable=False,
                )
            ],
        )
        schema = datamodel.Schema(
            name="InlineSchema",
            id="#InlineSchema",
            tables=[table],
        )
        self.converter = FelisConverter(schema)

    def test_convert_tables_maps_all_supported_types_and_modes(self) -> None:
        """Convert a synthetic table covering all supported Felis types."""
        expected_types = FelisConverter._TYPE_MAP.copy()

        base_column = datamodel.Column(
            name="base",
            id="#TypeCoverage.base",
            datatype=datamodel.DataType.int,
            nullable=True,
        )

        columns: list[datamodel.Column] = []
        for i, data_type in enumerate(expected_types):
            column_name = f"c_{data_type.name}"
            nullable = i % 2 == 0
            columns.append(
                base_column.model_copy(
                    update={
                        "name": column_name,
                        "id": f"#TypeCoverage.{column_name}",
                        "datatype": data_type,
                        "nullable": nullable,
                    }
                )
            )

        table = datamodel.Table(
            name="TypeCoverage",
            id="#TypeCoverage",
            columns=columns,
        )
        schema = datamodel.Schema(
            name="InlineSchema",
            id="#InlineSchema",
            tables=[table],
        )

        converter = FelisConverter(schema)
        converted = converter.convert_tables(["TypeCoverage"], "dummy-project.dummy_dataset")

        self.assertEqual(len(converted), 1)
        bq_table = converted[0]
        self.assertEqual(bq_table.project, "dummy-project")
        self.assertEqual(bq_table.dataset_id, "dummy_dataset")
        self.assertEqual(bq_table.table_id, "TypeCoverage")

        self.assertEqual(len(bq_table.schema), len(columns))
        for source_column, field in zip(columns, bq_table.schema, strict=True):
            self.assertEqual(field.name, source_column.name)
            self.assertEqual(field.field_type, expected_types[source_column.datatype])
            self.assertEqual(field.mode, "NULLABLE" if source_column.nullable else "REQUIRED")

    def test_find_table_raises_for_missing_name(self) -> None:
        """Missing table names should raise FelisConverterError."""
        with self.assertRaisesRegex(FelisConverterError, r"Table 'MissingTable' not found"):
            self.converter.find_table("MissingTable")

    def test_convert_tables_raises_for_missing_name(self) -> None:
        """convert_tables should surface missing table lookup failures."""
        with self.assertRaisesRegex(FelisConverterError, r"Table 'MissingTable' not found"):
            self.converter.convert_tables(["MissingTable"], "dummy-project.dummy_dataset")


if __name__ == "__main__":
    unittest.main()
