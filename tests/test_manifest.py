# This file is part of dax_ppdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import unittest

from lsst.dax.ppdb.bigquery import Manifest, ParquetFileEntry
from lsst.dax.ppdb.bigquery.updates.update_records import UpdateRecords


class ManifestTestCase(unittest.TestCase):
    """A test case for the Manifest class."""

    @staticmethod
    def _create_manifest_with_files(file_names: list[str]):
        return Manifest(
            replica_chunk_id="12345",
            unique_id="550e8400-e29b-41d4-a716-446655440000",
            schema_version="1.0",
            exported_at="2025-12-17 07:03:09.991638+00:00",
            last_update_time="1765951277.036",
            files={name: {"row_count": 10} for name in file_names},
            compression_format="snappy",
        )

    def test_is_empty_chunk(self):
        """Test the is_empty_chunk method of the Manifest class."""
        # A manifest with no files should be considered empty.
        manifest_no_files = self._create_manifest_with_files([])
        self.assertTrue(manifest_no_files.is_empty_chunk)

        # A manifest with updates but no table data should not be considered
        # empty.
        manifest_with_updates = self._create_manifest_with_files([UpdateRecords.PARQUET_FILE_NAME])
        self.assertFalse(manifest_with_updates.is_empty_chunk)

        # A manifest with some table data but no updates should not be
        # considered empty.
        manifest_non_empty = self._create_manifest_with_files(["DiaObject"])
        self.assertFalse(manifest_non_empty.is_empty_chunk)

        # A manifest with updates but no table data should not be considered
        # empty.
        manifest_with_updates = self._create_manifest_with_files([UpdateRecords.PARQUET_FILE_NAME])
        self.assertFalse(manifest_with_updates.is_empty_chunk)

        # A manifest with both table data and updates should not be considered
        # empty.
        manifest_with_both = self._create_manifest_with_files(
            ["DiaObject", "DiaSource", "DiaForcedSource", UpdateRecords.PARQUET_FILE_NAME]
        )
        self.assertFalse(manifest_with_both.is_empty_chunk)

    def test_parquet_file_stats_roundtrip(self) -> None:
        """Test that the file statistics are preserved through JSON
        roundtrip.
        """
        manifest = self._create_manifest_with_files([])

        manifest.files = {
            "DiaObject": ParquetFileEntry(row_count=10, checksum="a" * 64, size_bytes=100),
            "DiaSource": ParquetFileEntry(row_count=1, checksum="b" * 64, size_bytes=200),
            "DiaForcedSource": ParquetFileEntry(row_count=3, checksum="c" * 64, size_bytes=300),
            UpdateRecords.PARQUET_FILE_NAME: ParquetFileEntry(row_count=3, checksum="d" * 64, size_bytes=400),
        }

        # Check that the data loaded from the JSON string matches the original
        # data.
        loaded = Manifest.from_json_str(manifest.model_dump_json())
        self.assertEqual(loaded.files["DiaObject"].row_count, 10)
        self.assertEqual(loaded.files["DiaObject"].checksum, "a" * 64)
        self.assertEqual(loaded.files["DiaObject"].size_bytes, 100)
        self.assertEqual(loaded.files["DiaSource"].row_count, 1)
        self.assertEqual(loaded.files["DiaSource"].checksum, "b" * 64)
        self.assertEqual(loaded.files["DiaSource"].size_bytes, 200)
        self.assertEqual(loaded.files["DiaForcedSource"].row_count, 3)
        self.assertEqual(loaded.files["DiaForcedSource"].checksum, "c" * 64)
        self.assertEqual(loaded.files["DiaForcedSource"].size_bytes, 300)
        self.assertEqual(loaded.files[UpdateRecords.PARQUET_FILE_NAME].row_count, 3)
        self.assertEqual(loaded.files[UpdateRecords.PARQUET_FILE_NAME].checksum, "d" * 64)
        self.assertEqual(loaded.files[UpdateRecords.PARQUET_FILE_NAME].size_bytes, 400)

    def test_row_count_zero_raises(self) -> None:
        """Test that a row count of zero raises a ValueError."""
        with self.assertRaises(ValueError):
            ParquetFileEntry(row_count=0, checksum="a" * 64, size_bytes=100)
