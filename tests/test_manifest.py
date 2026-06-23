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

from lsst.dax.ppdb.bigquery.manifest import Manifest


class ManifestTestCase(unittest.TestCase):
    """A test case for the Manifest class."""

    def test_is_empty_chunk(self):
        """Test the is_empty_chunk method of the Manifest class."""
        manifest = Manifest(
            replica_chunk_id="12345",
            unique_id="550e8400-e29b-41d4-a716-446655440000",
            schema_version="1.0",
            exported_at="2025-12-17 07:03:09.991638+00:00",
            last_update_time="1765951277.036",
            compression_format="snappy",
            table_data={
                "DiaObject": {"row_count": 0},
                "DiaSource": {"row_count": 0},
                "DiaForcedSource": {"row_count": 0},
            },
        )
        self.assertTrue(manifest.is_empty_chunk())

        # A manifest with update_count > 0 should not be empty.
        manifest_with_updates = Manifest(
            replica_chunk_id="12345",
            unique_id="550e8400-e29b-41d4-a716-446655440000",
            schema_version="1.0",
            exported_at="2025-12-17 07:03:09.991638+00:00",
            last_update_time="1765951277.036",
            compression_format="snappy",
            table_data={
                "DiaObject": {"row_count": 0},
                "DiaSource": {"row_count": 0},
                "DiaForcedSource": {"row_count": 0},
            },
            update_count=3,
        )
        self.assertFalse(manifest_with_updates.is_empty_chunk())

        manifest_non_empty = Manifest(
            replica_chunk_id="12345",
            unique_id="550e8400-e29b-41d4-a716-446655440000",
            schema_version="1.0",
            exported_at="2025-12-17 07:03:09.991638+00:00",
            last_update_time="1765951277.036",
            compression_format="snappy",
            table_data={
                "DiaObject": {"row_count": 10},
                "DiaSource": {"row_count": 10},
                "DiaForcedSource": {"row_count": 0},
            },
        )
        self.assertFalse(manifest_non_empty.is_empty_chunk())

    def test_parquet_file_stats_roundtrip(self) -> None:
        """Test that checksum and size_bytes are preserved through JSON roundtrip."""
        manifest = Manifest(
            replica_chunk_id="12345",
            unique_id="550e8400-e29b-41d4-a716-446655440000",
            schema_version="1.0",
            exported_at="2025-12-17 07:03:09.991638+00:00",
            last_update_time="1765951277.036",
            compression_format="snappy",
            table_data={
                "DiaObject": {"row_count": 10, "checksum": "a" * 64, "size_bytes": 4096},
                "DiaSource": {"row_count": 1, "checksum": "b" * 64, "size_bytes": 512},
                "DiaForcedSource": {"row_count": 0, "checksum": None, "size_bytes": None},
            },
            update_count=1,
            updates_data={"row_count": 1, "checksum": "c" * 64, "size_bytes": 256},
        )

        loaded = Manifest.from_json_str(manifest.model_dump_json())
        self.assertEqual(loaded.table_data["DiaObject"].checksum, "a" * 64)
        self.assertEqual(loaded.table_data["DiaObject"].size_bytes, 4096)
        self.assertEqual(loaded.table_data["DiaSource"].checksum, "b" * 64)
        self.assertEqual(loaded.table_data["DiaSource"].size_bytes, 512)
        self.assertIsNone(loaded.table_data["DiaForcedSource"].checksum)
        self.assertIsNone(loaded.table_data["DiaForcedSource"].size_bytes)
        assert loaded.updates_data is not None
        self.assertEqual(loaded.updates_data.checksum, "c" * 64)
        self.assertEqual(loaded.updates_data.size_bytes, 256)
