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

import datetime
import unittest

import astropy.time

from lsst.dax.apdb import (
    ApdbCloseDiaObjectValidityRecord,
    ApdbReassignDiaSourceToDiaObjectRecord,
    ApdbReassignDiaSourceToSSObjectRecord,
    ApdbUpdateNDiaSourcesRecord,
    ApdbWithdrawDiaForcedSourceRecord,
    ApdbWithdrawDiaSourceRecord,
)
from lsst.dax.ppdb.bigquery.updates import ExpandedUpdateRecord, UpdateRecordExpander, UpdateRecords
from lsst.dax.ppdb.tests._updates import _create_test_update_records


class UpdateRecordExpanderTestCase(unittest.TestCase):
    """Test UpdateRecordExpander functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Test time for consistent timestamps
        self.update_time = astropy.time.Time("2021-03-01T12:00:00", format="isot", scale="tai")
        self.update_time_ns = int(self.update_time.unix_tai * 1e9)

        # Test replica chunk ID
        self.replica_chunk_id = 12345

    def test_reassign_diasource_to_diaobject(self) -> None:
        """Test expand_single_record with
        ApdbReassignDiaSourceToDiaObjectRecord.
        """
        from lsst.dax.ppdb.bigquery.updates import ExpandedUpdateRecord, UpdateRecordExpander

        record = ApdbReassignDiaSourceToDiaObjectRecord(
            update_time_ns=self.update_time_ns,
            update_order=0,
            diaSourceId=100001,
            diaObjectId=300001,
            ra=45.0,
            dec=-30.0,
            midpointMjdTai=60000.0,
        )

        expanded = UpdateRecordExpander.expand_single_record(record, self.replica_chunk_id)

        # Should expand to 1 record (diaObjectId)
        self.assertEqual(len(expanded), 1)

        expanded_record = expanded[0]
        self.assertIsInstance(expanded_record, ExpandedUpdateRecord)
        self.assertEqual(expanded_record.table_name, "DiaSource")
        self.assertEqual(expanded_record.record_id, (100001,))
        self.assertEqual(expanded_record.field_name, "diaObjectId")
        self.assertEqual(expanded_record.field_value, 300001)
        self.assertEqual(expanded_record.replica_chunk_id, self.replica_chunk_id)
        self.assertEqual(expanded_record.update_order, 0)
        self.assertEqual(expanded_record.update_time_ns, self.update_time_ns)

    def test_reassign_diasource_to_ssobject(self) -> None:
        """Test expand_single_record with
        ApdbReassignDiaSourceToSSObjectRecord.
        """
        record = ApdbReassignDiaSourceToSSObjectRecord(
            update_time_ns=self.update_time_ns,
            update_order=0,
            diaSourceId=100001,
            ssObjectId=2001,
            ssObjectReassocTimeMjdTai=float(self.update_time.tai.mjd),
            ra=45.0,
            dec=-30.0,
            midpointMjdTai=60000.0,
        )

        expanded = UpdateRecordExpander.expand_single_record(record, self.replica_chunk_id)

        # Should expand to 2 records (ssObjectId and ssObjectReassocTimeMjdTai)
        self.assertEqual(len(expanded), 2)

        # Check first expanded record (ssObjectId)
        first_record = expanded[0]
        self.assertIsInstance(first_record, ExpandedUpdateRecord)
        self.assertEqual(first_record.table_name, "DiaSource")
        self.assertEqual(first_record.record_id, (100001,))
        self.assertEqual(first_record.field_name, "ssObjectId")
        self.assertEqual(first_record.field_value, 2001)
        self.assertEqual(first_record.replica_chunk_id, self.replica_chunk_id)
        self.assertEqual(first_record.update_order, 0)
        self.assertEqual(first_record.update_time_ns, self.update_time_ns)

        # Check second expanded record (ssObjectReassocTimeMjdTai)
        second_record = expanded[1]
        self.assertEqual(second_record.table_name, "DiaSource")
        self.assertEqual(second_record.record_id, (100001,))
        self.assertEqual(second_record.field_name, "ssObjectReassocTimeMjdTai")
        self.assertEqual(second_record.field_value, float(self.update_time.tai.mjd))

    def test_withdraw_diasource(self) -> None:
        """Test expand_single_record with ApdbWithdrawDiaSourceRecord."""
        record = ApdbWithdrawDiaSourceRecord(
            update_time_ns=self.update_time_ns,
            update_order=2,
            diaSourceId=100003,
            timeWithdrawnMjdTai=self.update_time.tai.mjd,
            ra=45.0,
            dec=-30.0,
            midpointMjdTai=60000.0,
        )

        expanded = UpdateRecordExpander.expand_single_record(record, self.replica_chunk_id)

        # Should expand to 1 record (timeWithdrawnMjdTai)
        self.assertEqual(len(expanded), 1)

        expanded_record = expanded[0]
        self.assertEqual(expanded_record.table_name, "DiaSource")
        self.assertEqual(expanded_record.record_id, (100003,))
        self.assertEqual(expanded_record.field_name, "timeWithdrawnMjdTai")
        self.assertEqual(expanded_record.field_value, self.update_time.tai.mjd)

    def test_update_n_dia_sources(self) -> None:
        """Test expand_single_record with ApdbUpdateNDiaSourcesRecord."""
        record = ApdbUpdateNDiaSourcesRecord(
            update_time_ns=self.update_time_ns,
            update_order=5,
            diaObjectId=200002,
            nDiaSources=10,
            ra=45.0,
            dec=-30.0,
        )

        expanded = UpdateRecordExpander.expand_single_record(record, self.replica_chunk_id)

        # Should expand to 1 record (nDiaSources)
        self.assertEqual(len(expanded), 1)

        expanded_record = expanded[0]
        self.assertEqual(expanded_record.table_name, "DiaObject")
        self.assertEqual(expanded_record.record_id, (200002,))
        self.assertEqual(expanded_record.field_name, "nDiaSources")
        self.assertEqual(expanded_record.field_value, 10)

    def test_close_diaobject_validity(self) -> None:
        """Test expand_single_record with ApdbCloseDiaObjectValidityRecord."""
        record = ApdbCloseDiaObjectValidityRecord(
            update_time_ns=self.update_time_ns,
            update_order=4,
            diaObjectId=200001,
            validityEndMjdTai=self.update_time.tai.mjd,
            nDiaSources=5,
            ra=45.0,
            dec=-30.0,
        )

        expanded = UpdateRecordExpander.expand_single_record(record, self.replica_chunk_id)

        # Should expand to 2 records (validityEndMjdTai and nDiaSources)
        self.assertEqual(len(expanded), 2)

        # Check first expanded record (validityEndMjdTai)
        first_record = expanded[0]
        self.assertIsInstance(first_record, ExpandedUpdateRecord)
        self.assertEqual(first_record.table_name, "DiaObject")
        self.assertEqual(first_record.record_id, (200001,))
        self.assertEqual(first_record.field_name, "validityEndMjdTai")
        self.assertEqual(first_record.field_value, self.update_time.tai.mjd)

        # Check second expanded record (nDiaSources)
        second_record = expanded[1]
        self.assertEqual(second_record.table_name, "DiaObject")
        self.assertEqual(second_record.record_id, (200001,))
        self.assertEqual(second_record.field_name, "nDiaSources")
        self.assertEqual(second_record.field_value, 5)

    def test_withdraw_diaforcedsource(self) -> None:
        """Test expand_single_record with ApdbWithdrawDiaForcedSourceRecord."""
        record = ApdbWithdrawDiaForcedSourceRecord(
            update_time_ns=self.update_time_ns,
            update_order=2,
            diaObjectId=200001,
            visit=12345,
            detector=42,
            timeWithdrawnMjdTai=self.update_time.tai.mjd,
            ra=45.0,
            dec=-30.0,
            midpointMjdTai=60000.0,
        )

        expanded = UpdateRecordExpander.expand_single_record(record, self.replica_chunk_id)

        # Should expand to 1 record (timeWithdrawnMjdTai)
        self.assertEqual(len(expanded), 1)

        expanded_record = expanded[0]
        self.assertEqual(expanded_record.table_name, "DiaForcedSource")
        # The record ID should be a list of the composite key components
        # [diaObjectId, visit, detector] for BigQuery compatibility
        expected_record_id = (200001, 12345, 42)
        self.assertEqual(expanded_record.record_id, expected_record_id)
        self.assertEqual(expanded_record.field_name, "timeWithdrawnMjdTai")
        self.assertEqual(expanded_record.field_value, self.update_time.tai.mjd)

    def test_update_records_all(self) -> None:
        """Test the full expand_updates method with multiple record types."""
        update_records = _create_test_update_records()

        expanded = UpdateRecordExpander.expand_updates(update_records)

        self.assertEqual(len(expanded), 10)

        # Verify all expanded records have correct replica_chunk_id
        for record in expanded:
            self.assertEqual(record.replica_chunk_id, self.replica_chunk_id)
            self.assertIsInstance(record.update_time_ns, int)
            self.assertIsInstance(record.update_order, int)

        # Check that we have the expected table names
        table_names = {record.table_name for record in expanded}
        expected_tables = {"DiaSource", "DiaObject", "DiaForcedSource"}
        self.assertEqual(table_names, expected_tables)

        # Check that we have the expected field names
        field_names = {record.field_name for record in expanded}
        expected_fields = {
            "diaObjectId",  # from reassign to diaobject
            "ssObjectId",
            "ssObjectReassocTimeMjdTai",  # from reassign to ssobject
            "timeWithdrawnMjdTai",  # from withdraw diasource and withdraw forced source
            "validityEndMjdTai",
            "nDiaSources",  # from close validity and update n dia sources
        }
        self.assertEqual(field_names, expected_fields)

    def test_empty_records(self) -> None:
        """Test expand_updates with empty records list."""
        empty_update_records = UpdateRecords(
            replica_chunk_id=self.replica_chunk_id,
            record_count=0,
            records=[],
            file_created_at=datetime.datetime.now(datetime.UTC),
        )

        expanded = UpdateRecordExpander.expand_updates(empty_update_records)
        self.assertEqual(len(expanded), 0)


if __name__ == "__main__":
    unittest.main()
