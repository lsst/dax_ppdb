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

from lsst.dax.apdb import (
    Apdb,
    ApdbReplica,
    apdbUpdateRecord,
)
from lsst.dax.ppdb import Ppdb
from lsst.dax.ppdb.bigquery import PpdbBigQuery
from lsst.dax.ppdb.bigquery.updates import UpdateRecords
from lsst.dax.ppdb.replicator import Replicator
from lsst.dax.ppdb.tests import fill_apdb
from lsst.dax.ppdb.tests._bigquery import (
    PostgresMixin,
    have_valid_google_credentials,
)


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class UpdateRecordsTestCase(PostgresMixin, unittest.TestCase):
    """A test case for the handling of APDB record updates by PpdbBigQuery."""

    def setUp(self):
        super().setUp()

        # Make APDB instance and fill it with test data.
        apdb_config = self.make_apdb_instance()
        apdb = Apdb.from_config(apdb_config)
        fill_apdb(apdb, include_update_records=True)
        apdb_replica = ApdbReplica.from_config(apdb_config)

        # Make PPDB instance.
        self.ppdb_config = self.make_instance()
        self.ppdb = Ppdb.from_config(self.ppdb_config)
        assert isinstance(self.ppdb, PpdbBigQuery)

        # Replicate APDB replica chunks to the PPDB.
        replicator = Replicator(
            apdb_replica, self.ppdb, update=False, min_wait_time=0, max_wait_time=0, check_interval=0
        )
        replicator.run(exit_on_empty=True)

    def test_json_serialization(self) -> None:
        """Test that the APDB update records are correctly saved to a JSON file
        in the replication output and can be read back as valid UpdateRecords
        objects.
        """
        update_records_path = (
            self.ppdb.config.replication_path / "2021/03/01/1614600000" / "update_records.json"
        )
        self.assertTrue(update_records_path.exists(), "Update records file not found in replication output")

        update_records = UpdateRecords.from_json_file(update_records_path)

        self.assertEqual(
            update_records.replica_chunk_id,
            1614600000,
            "Unexpected replica chunk ID in deserialized update records",
        )

        self.assertEqual(len(update_records.records), 3, "Unexpected number of update records deserialized")

        self.assertEqual(
            len(update_records.records), 3, "Unexpected number of update records in the deserialized object"
        )

        for record in update_records.records:
            self.assertIsInstance(
                record,
                apdbUpdateRecord.ApdbUpdateRecord,
                "Deserialized record is not an instance of ApdbUpdateRecord",
            )

        update_record = update_records.records[0]
        self.assertIsInstance(
            update_record,
            apdbUpdateRecord.ApdbReassignDiaSourceToSSObjectRecord,
            "Deserialized record is not an instance of ApdbReassignDiaSourceToSSObjectRecord",
        )
        assert isinstance(update_record, apdbUpdateRecord.ApdbReassignDiaSourceToSSObjectRecord)
        self.assertEqual(
            update_record.diaSourceId,
            700,
            "Unexpected diaSourceId in deserialized ApdbReassignDiaSourceToSSObjectRecord",
        )
        self.assertEqual(
            update_record.ssObjectId,
            1,
            "Unexpected ssObjectId in deserialized ApdbReassignDiaSourceToSSObjectRecord",
        )
        self.assertEqual(
            update_record.update_time_ns,
            1614600037000000000,
            "Unexpected update_time_ns in deserialized ApdbReassignDiaSourceToSSObjectRecord",
        )
        self.assertEqual(
            update_record.update_order,
            0,
            "Unexpected update_order in deserialized ApdbReassignDiaSourceToSSObjectRecord",
        )
        self.assertEqual(
            update_record.midpointMjdTai,
            60000.0,
            "Unexpected midpointMjdTai in deserialized ApdbReassignDiaSourceToSSObjectRecord",
        )
        self.assertEqual(
            update_record.ssObjectReassocTimeMjdTai,
            59274.50042824074,
            "Unexpected ssObjectReassocTimeMjdTai in deserialized ApdbReassignDiaSourceToSSObjectRecord",
        )
        self.assertNotEqual(
            update_record.ra,
            0.0,
            "Unexpected ra in deserialized ApdbReassignDiaSourceToSSObjectRecord, should not be 0.0",
        )
        self.assertNotEqual(
            update_record.dec,
            0.0,
            "Unexpected dec in deserialized ApdbReassignDiaSourceToSSObjectRecord, should not be 0.0",
        )

        update_record = update_records.records[1]
        self.assertIsInstance(
            update_record,
            apdbUpdateRecord.ApdbCloseDiaObjectValidityRecord,
            "Deserialized record is not an instance of ApdbCloseDiaObjectValidityRecord",
        )
        self.assertEqual(
            update_record.diaObjectId,
            200,
            "Unexpected diaObjectId in deserialized ApdbCloseDiaObjectValidityRecord",
        )
        self.assertNotEqual(
            update_record.ra,
            0.0,
            "Unexpected ra in deserialized ApdbCloseDiaObjectValidityRecord, should not be 0.0",
        )
        self.assertNotEqual(
            update_record.dec,
            0.0,
            "Unexpected dec in deserialized ApdbCloseDiaObjectValidityRecord, should not be 0.0",
        )
        self.assertEqual(
            update_record.update_time_ns,
            1614600037000000000,
            "Unexpected update_time_ns in deserialized ApdbCloseDiaObjectValidityRecord",
        )
        self.assertEqual(
            update_record.update_order,
            1,
            "Unexpected update_order in deserialized ApdbCloseDiaObjectValidityRecord",
        )
        self.assertEqual(
            update_record.validityEndMjdTai,
            59274.50042824074,
            "Unexpected validityEndMjdTai in deserialized ApdbCloseDiaObjectValidityRecord",
        )
        self.assertIsNone(
            update_record.nDiaSources,
            "Unexpected nDiaSources in deserialized ApdbCloseDiaObjectValidityRecord, expected None",
        )

        update_record = update_records.records[2]
        self.assertIsInstance(
            update_record,
            apdbUpdateRecord.ApdbWithdrawDiaForcedSourceRecord,
            "Deserialized record is not an instance of ApdbWithdrawDiaForcedSourceRecord",
        )
        self.assertEqual(
            update_record.diaObjectId,
            200,
            "Unexpected diaObjectId in deserialized ApdbWithdrawDiaForcedSourceRecord",
        )
        self.assertEqual(
            update_record.visit,
            7,
            "Unexpected visit in deserialized ApdbWithdrawDiaForcedSourceRecord",
        )
        self.assertEqual(
            update_record.detector,
            1,
            "Unexpected detector in deserialized ApdbWithdrawDiaForcedSourceRecord",
        )
        self.assertNotEqual(
            update_record.ra,
            0.0,
            "Unexpected ra in deserialized ApdbWithdrawDiaForcedSourceRecord, should not be 0.0",
        )
        self.assertNotEqual(
            update_record.dec,
            0.0,
            "Unexpected dec in deserialized ApdbWithdrawDiaForcedSourceRecord, should not be 0.0",
        )
        self.assertEqual(
            update_record.midpointMjdTai,
            60000.0,
            "Unexpected midpointMjdTai in deserialized ApdbWithdrawDiaForcedSourceRecord",
        )
        self.assertEqual(
            update_record.update_time_ns,
            1614600037000000000,
            "Unexpected update_time_ns in deserialized ApdbWithdrawDiaForcedSourceRecord",
        )
        self.assertEqual(
            update_record.update_order,
            2,
            "Unexpected update_order in deserialized ApdbWithdrawDiaForcedSourceRecord",
        )
        self.assertEqual(
            update_record.timeWithdrawnMjdTai,
            59274.50042824074,
            "Unexpected timeWithdrawnMjdTai in deserialized ApdbWithdrawDiaForcedSourceRecord",
        )
        self.assertNotEqual(
            update_record.ra,
            0.0,
            "Unexpected ra in deserialized ApdbWithdrawDiaForcedSourceRecord, should not be 0.0",
        )
        self.assertNotEqual(
            update_record.dec,
            0.0,
            "Unexpected dec in deserialized ApdbWithdrawDiaForcedSourceRecord, should not be 0.0",
        )


if __name__ == "__main__":
    unittest.main()
