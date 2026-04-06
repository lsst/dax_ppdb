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
import uuid

from google.cloud import bigquery

from lsst.dax.ppdb.bigquery.updates import UpdateRecordExpander, UpdatesTable
from lsst.dax.ppdb.tests._bigquery import have_valid_google_credentials
from lsst.dax.ppdb.tests._updates import _create_test_update_records


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class TestUpdatesTable(unittest.TestCase):
    """Test UpdatesTable functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create BigQuery client
        self.client = bigquery.Client()

        # Create unique dataset name for this test run
        self.dataset_id = f"test_updates_{uuid.uuid4().hex[:8]}"
        self.project_id = self.client.project
        self.table_name = "updates"
        self.table_fqn = f"{self.project_id}.{self.dataset_id}.{self.table_name}"

        # Create the test dataset
        dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
        # Set a short expiration for cleanup safety (1 hour)
        dataset.default_table_expiration_ms = 3600000  # 1 hour
        self.dataset = self.client.create_dataset(dataset)

        # Create UpdatesTable instance
        self.updates_table = UpdatesTable(self.client, self.project_id, self.dataset_id)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Always clean up the test dataset, whether test passed or failed
        try:
            self.client.delete_dataset(self.dataset_id, delete_contents=True, not_found_ok=True)
        except Exception:
            # If deletion fails, at least the expiration will clean it up
            pass

    def test_table_fqn_property(self) -> None:
        """Test the table_fqn property."""
        self.assertEqual(self.updates_table.table_fqn, self.table_fqn)

    def test_create_table(self) -> None:
        """Test creation of the updates table."""
        table = self.updates_table.create()

        # Verify table was created successfully
        self.assertEqual(table.table_id, self.table_name)
        self.assertEqual(table.dataset_id, self.dataset_id)

        # Verify schema is correct
        expected_fields = {
            "table_name": ("STRING", "REQUIRED"),
            "record_id": ("INTEGER", "REPEATED"),
            "record_key": ("STRING", "REQUIRED"),
            "field_name": ("STRING", "REQUIRED"),
            "value_json": ("JSON", "REQUIRED"),
            "replica_chunk_id": ("INTEGER", "REQUIRED"),
            "update_order": ("INTEGER", "NULLABLE"),
            "update_time_ns": ("INTEGER", "NULLABLE"),
        }

        actual_fields = {field.name: (field.field_type, field.mode) for field in table.schema}
        self.assertEqual(actual_fields, expected_fields)

    def test_create_table_already_exists(self) -> None:
        """Test creating a table that already exists raises an error."""
        # Create table first time - should succeed
        self.updates_table.create()

        # Try to create again - should raise Conflict
        with self.assertRaises(Exception) as cm:
            self.updates_table.create()

        # Check that it's a conflict-type error
        self.assertIn("already exists", str(cm.exception).lower())

    def test_insert_records(self) -> None:
        """Test insertion of expanded records into the table."""
        # Create the table first
        self.updates_table.create()

        # Get test update records and expand them
        update_records = _create_test_update_records()
        expanded_records = UpdateRecordExpander.expand_updates(update_records)

        # Insert the records
        job = self.updates_table.insert(expanded_records)

        # Verify the job completed successfully
        self.assertIsNone(job.errors)

        # Verify records were inserted by querying the table
        query = f"SELECT COUNT(*) as count FROM `{self.table_fqn}`"
        result = list(self.client.query(query).result())
        record_count = result[0].count

        # Should have 10 total expanded records based on the test data
        # (1 + 2 + 1 + 1 + 2 + 1 from original records + 2 duplicates)
        self.assertEqual(record_count, 10)

        # Verify some specific data was inserted correctly
        query = f"""
        SELECT table_name, record_id, field_name, replica_chunk_id
        FROM `{self.table_fqn}`
        ORDER BY table_name, field_name, record_key
        """
        results = list(self.client.query(query).result())

        # Verify record counts per table
        tables = [r.table_name for r in results]
        self.assertEqual(tables.count("DiaSource"), 5)
        self.assertEqual(tables.count("DiaObject"), 4)
        self.assertEqual(tables.count("DiaForcedSource"), 1)

        # All records should have the same replica_chunk_id
        for row in results:
            self.assertEqual(row.replica_chunk_id, 12345)

        # Verify the single DiaForcedSource record
        forced = [r for r in results if r.table_name == "DiaForcedSource"]
        self.assertEqual(len(forced), 1)
        self.assertEqual(list(forced[0].record_id), [200001, 12345, 42])
        self.assertEqual(forced[0].field_name, "timeWithdrawnMjdTai")

        # Verify the DiaSource records
        reassign = [r for r in results if r.table_name == "DiaSource" and r.field_name == "diaObjectId"]
        self.assertEqual(len(reassign), 2)
        self.assertTrue(all(list(r.record_id) == [100001] for r in reassign))

    def test_insert_empty_records(self) -> None:
        """Test insertion of empty record list."""
        # Create the table first
        self.updates_table.create()

        # Insert empty list
        job = self.updates_table.insert([])

        # Verify the job completed successfully
        self.assertIsNone(job.errors)

        # Verify no records were inserted
        query = f"SELECT COUNT(*) as count FROM `{self.table_fqn}`"
        result = list(self.client.query(query).result())
        record_count = result[0].count
        self.assertEqual(record_count, 0)

    def test_latest_updates_only(self) -> None:
        """Test functionality for getting only the latest updates."""
        # Create the source table
        self.updates_table.create()

        # Get test records and expand them
        update_records = _create_test_update_records()
        expanded_records = UpdateRecordExpander.expand_updates(update_records)

        # Insert all of the records into the updates table
        self.updates_table.insert(expanded_records)

        # Count the original records
        query = f"SELECT COUNT(*) as count FROM `{self.table_fqn}`"
        original_count = list(self.client.query(query).result())[0].count

        # Create table with only the latest updates
        self.updates_table.create_latest_only()

        # Count the new number of records
        query = f"SELECT COUNT(*) as count FROM `{self.updates_table.latest_only_table_fqn}`"
        latest_only_count = list(self.client.query(query).result())[0].count

        # There should be fewer records now.
        self.assertLess(latest_only_count, original_count)

        # Verify specific record has the later update value
        record_key = UpdatesTable._make_record_key([100001])
        query = f"""
        SELECT value_json
        FROM `{self.updates_table.latest_only_table_fqn}`
        WHERE record_key = '{record_key}' AND field_name = 'diaObjectId'
        """
        result = list(self.client.query(query).result())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].value_json, 400001)  # Should be the later update


if __name__ == "__main__":
    unittest.main()
