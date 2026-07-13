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

from google.cloud import bigquery

from lsst.dax.ppdb.bigquery import DatasetType
from lsst.dax.ppdb.bigquery.updates import ExpandedUpdateRecord, ExpandedUpdatesTable
from lsst.dax.ppdb.tests import (
    create_datasets,
    drop_datasets,
    have_valid_google_credentials,
    make_bigquery_config,
)
from lsst.dax.ppdb.tests._updates import _create_test_update_records


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class TestExpandedUpdatesTable(unittest.TestCase):
    """Test ExpandedUpdatesTable functionality."""

    dataset_types = (DatasetType.PROMOTION,)

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create BigQuery client.
        self.client = bigquery.Client()

        # Create PPDB BigQuery config for test.
        self.config = make_bigquery_config(test_name="test_expanded_updates_table")

        # Create the BigQuery dataset for the tests.
        create_datasets(self.config, self.dataset_types)

        # Add cleanup for datasets after test.
        self.addCleanup(drop_datasets, self.config, self.dataset_types)

        # Set the FQN of the expanded updates table.
        self.table_fqn = self.config.fqn_for(DatasetType.PROMOTION, "expanded_updates")

        # Create the ExpandedUpdatesTable instance.
        self.expanded_updates_table = ExpandedUpdatesTable(self.client, self.config)

    @staticmethod
    def _expand(apdb_replica_chunk: int) -> list[ExpandedUpdateRecord]:
        """Expand the test update records for the given chunk."""
        update_records = _create_test_update_records(apdb_replica_chunk)
        expanded: list[ExpandedUpdateRecord] = []
        for chunk, record in update_records.records:
            expanded.extend(ExpandedUpdateRecord.from_update_record(record, chunk))
        return expanded

    def test_table_fqn_property(self) -> None:
        """Test the table_fqn property."""
        self.assertEqual(self.expanded_updates_table.expanded_updates_fqn, self.table_fqn)

    def test_create_table(self) -> None:
        """Test creation of the expanded updates table."""
        table = self.expanded_updates_table.create()

        # Verify table was created successfully.
        self.assertEqual(table.table_id, "expanded_updates")
        self.assertEqual(table.dataset_id, self.config.datasets.promotion)

        # Verify schema is correct.
        expected_fields = {
            "table_name": ("STRING", "REQUIRED"),
            "record_id": ("INTEGER", "REPEATED"),
            "record_key": ("STRING", "REQUIRED"),
            "field_name": ("STRING", "REQUIRED"),
            "value_json": ("JSON", "REQUIRED"),
            "apdb_replica_chunk": ("INTEGER", "REQUIRED"),
            "update_order": ("INTEGER", "REQUIRED"),
            "update_time_ns": ("INTEGER", "REQUIRED"),
        }
        actual_fields = {field.name: (field.field_type, field.mode) for field in table.schema}
        self.assertEqual(actual_fields, expected_fields)

    def test_create_table_already_exists(self) -> None:
        """Test creating a table that already exists raises an error."""
        # Create table first time - should succeed.
        self.expanded_updates_table.create()

        # Try to create again - should raise Conflict.
        with self.assertRaises(Exception) as cm:
            self.expanded_updates_table.create()

        # Check that it's a conflict-type error.
        self.assertIn("already exists", str(cm.exception).lower())

    def test_insert_records(self) -> None:
        """Test insertion of expanded records into the table."""
        # Create the table first.
        self.expanded_updates_table.create()

        # Get test update records and expand them.
        expanded_records = self._expand(12345)

        # Insert the records.
        job = self.expanded_updates_table.insert(expanded_records)

        # Verify the job completed successfully.
        self.assertIsNone(job.errors)

        # Verify records were inserted by querying the table.
        query = f"SELECT COUNT(*) as count FROM `{self.table_fqn}`"
        result = list(self.client.query(query).result())
        record_count = result[0].count

        # Should have 10 total expanded records based on the test data.
        self.assertEqual(record_count, 10)

        # Verify some specific data was inserted correctly.
        query = f"""
        SELECT table_name, record_id, field_name, apdb_replica_chunk
        FROM `{self.table_fqn}`
        ORDER BY table_name, field_name, record_key
        """
        results = list(self.client.query(query).result())

        # Verify record counts per table.
        tables = [r.table_name for r in results]
        self.assertEqual(tables.count("DiaSource"), 5)
        self.assertEqual(tables.count("DiaObject"), 4)
        self.assertEqual(tables.count("DiaForcedSource"), 1)

        # All records should have the same apdb_replica_chunk.
        for row in results:
            self.assertEqual(row.apdb_replica_chunk, 12345)

        # Verify the single DiaForcedSource record.
        forced = [r for r in results if r.table_name == "DiaForcedSource"]
        self.assertEqual(len(forced), 1)
        self.assertEqual(list(forced[0].record_id), [200001, 12345, 42])
        self.assertEqual(forced[0].field_name, "timeWithdrawnMjdTai")

        # Verify the DiaSource records.
        reassign = [r for r in results if r.table_name == "DiaSource" and r.field_name == "diaObjectId"]
        self.assertEqual(len(reassign), 2)
        self.assertTrue(all(list(r.record_id) == [100001] for r in reassign))

    def test_insert_empty_records(self) -> None:
        """Test insertion of empty record list."""
        # Create the table first.
        self.expanded_updates_table.create()

        # Insert empty list.
        job = self.expanded_updates_table.insert([])

        # Verify the job completed successfully.
        self.assertIsNone(job.errors)

        # Verify no records were inserted.
        query = f"SELECT COUNT(*) as count FROM `{self.table_fqn}`"
        result = list(self.client.query(query).result())
        record_count = result[0].count
        self.assertEqual(record_count, 0)

    def test_latest_updates_only(self) -> None:
        """Test functionality for getting only the latest updates."""
        # Create the source table.
        self.expanded_updates_table.create()

        # Get test records and expand them.
        expanded_records = self._expand(0)

        # Insert all of the records into the expanded updates table.
        self.expanded_updates_table.insert(expanded_records)

        # Count the original records.
        query = f"SELECT COUNT(*) as count FROM `{self.table_fqn}`"
        original_count = list(self.client.query(query).result())[0].count

        # Create table with only the latest updates.
        self.expanded_updates_table.create_latest_only()

        # Count the new number of records.
        query = f"SELECT COUNT(*) as count FROM `{self.expanded_updates_table.latest_only_fqn}`"
        latest_only_count = list(self.client.query(query).result())[0].count

        # There should be fewer records now.
        self.assertLess(latest_only_count, original_count)

        # Verify specific record has the later update value.
        record_key = ExpandedUpdatesTable._make_record_key([100001])
        query = f"""
        SELECT value_json
        FROM `{self.expanded_updates_table.latest_only_fqn}`
        WHERE record_key = '{record_key}' AND field_name = 'diaObjectId'
        """
        result = list(self.client.query(query).result())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].value_json, 400001)  # Should be the later update


if __name__ == "__main__":
    unittest.main()
