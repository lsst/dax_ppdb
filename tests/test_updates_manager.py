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
from collections.abc import Collection, Sequence
from unittest.mock import patch

import astropy
import felis
from google.cloud import bigquery

from lsst.dax.apdb import (
    ApdbTableData,
    ReplicaChunk,
)
from lsst.dax.ppdb import Ppdb
from lsst.dax.ppdb.bigquery import (
    DatasetType,
    PpdbBigQuery,
    PpdbBigQueryConfig,
)
from lsst.dax.ppdb.bigquery.chunk_uploader import ChunkUploader
from lsst.dax.ppdb.bigquery.updates.updates_manager import UpdatesManager
from lsst.dax.ppdb.tests._bigquery import (
    PostgresMixin,
    create_bucket,
    create_datasets,
    delete_bucket,
    drop_datasets,
    have_valid_google_credentials,
    json_rows_to_buf,
)
from lsst.dax.ppdb.tests._updates import _create_test_update_records


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class UpdatesManagerTestCase(PostgresMixin, unittest.TestCase):
    """A test case for the handling of APDB record updates by PpdbBigQuery and
    related classes including the ChunkUploader.
    """

    dataset_types = (DatasetType.INTERNAL, DatasetType.STAGING)

    def setUp(self):
        super().setUp()

        # Setup the Postgres database and create the config instance.
        self.config = self.make_instance(test_name="test_updates_manager")

        # Add cleanup for datasets after test.
        self.addCleanup(drop_datasets, self.config, self.dataset_types)

        # Create the necessary datasets in BigQuery for the test.
        create_datasets(self.config, self.dataset_types)

        # Create the test tables in BigQuery.
        self._create_test_tables(self.config, DatasetType.INTERNAL)

        # Create the test GCS bucket.
        create_bucket(self.config)

        # Add cleanup for the bucket after test.
        self.addCleanup(delete_bucket, self.config.bucket_name)

        # Create the PPDB instance.
        self.ppdb = Ppdb.from_config(self.config)
        assert isinstance(self.ppdb, PpdbBigQuery)

    @staticmethod
    def _create_test_tables(config: PpdbBigQueryConfig, dataset_type: DatasetType) -> None:
        """Create test tables in the specified BigQuery dataset."""
        client = bigquery.Client()

        # Create DiaObject table.
        schema = [
            bigquery.SchemaField("diaObjectId", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("validityEndMjdTai", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("nDiaSources", "INTEGER", mode="NULLABLE"),
        ]
        table_fqn = config.fqn_for(dataset_type, "DiaObject")
        table = bigquery.Table(table_fqn, schema=schema)
        client.create_table(table)
        rows = [
            {"diaObjectId": 200001, "validityEndMjdTai": None, "nDiaSources": 3},
            {"diaObjectId": 200002, "validityEndMjdTai": None, "nDiaSources": 7},
            {"diaObjectId": 200003, "validityEndMjdTai": 59000.0, "nDiaSources": 2},
        ]
        buf = json_rows_to_buf(rows)
        job = client.load_table_from_file(
            buf,
            table_fqn,
            job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON),
        )
        job.result()

        # Create test DiaSource table.
        schema = [
            bigquery.SchemaField("diaSourceId", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("diaObjectId", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ssObjectId", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ssObjectReassocTimeMjdTai", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("timeWithdrawnMjdTai", "FLOAT", mode="NULLABLE"),
        ]
        table_fqn = config.fqn_for(dataset_type, "DiaSource")
        table = bigquery.Table(table_fqn, schema=schema)
        client.create_table(table)
        rows = [
            {
                "diaSourceId": 100001,
                "diaObjectId": 200001,
                "ssObjectId": None,
                "ssObjectReassocTimeMjdTai": None,
                "timeWithdrawnMjdTai": None,
            },
            {
                "diaSourceId": 100002,
                "diaObjectId": 200002,
                "ssObjectId": None,
                "ssObjectReassocTimeMjdTai": None,
                "timeWithdrawnMjdTai": None,
            },
            {
                "diaSourceId": 100003,
                "diaObjectId": 200003,
                "ssObjectId": None,
                "ssObjectReassocTimeMjdTai": None,
                "timeWithdrawnMjdTai": None,
            },
            {
                "diaSourceId": 100004,
                "diaObjectId": 200004,
                "ssObjectId": None,
                "ssObjectReassocTimeMjdTai": None,
                "timeWithdrawnMjdTai": None,
            },
        ]
        job = client.load_table_from_file(
            json_rows_to_buf(rows),
            table_fqn,
            job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON),
        )
        job.result()

        # Create test DiaForcedSource table.
        schema = [
            bigquery.SchemaField("diaObjectId", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("visit", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("detector", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("timeWithdrawnMjdTai", "FLOAT", mode="NULLABLE"),
        ]
        table_fqn = config.fqn_for(dataset_type, "DiaForcedSource")
        table = bigquery.Table(table_fqn, schema=schema)
        client.create_table(table)
        rows = [
            {"diaObjectId": 200001, "visit": 12345, "detector": 42, "timeWithdrawnMjdTai": None},
            {"diaObjectId": 200001, "visit": 12346, "detector": 42, "timeWithdrawnMjdTai": None},
        ]
        job = client.load_table_from_file(
            json_rows_to_buf(rows),
            table_fqn,
            job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON),
        )
        job.result()

    def test_apply_updates(self):
        """Test that the update records are correctly uploaded to Google Cloud
        Storage after replication.
        """

        class DummyApdbTableData(ApdbTableData):
            def column_names(self) -> Sequence[str]:
                return []

            def column_defs(self) -> Sequence[tuple[str, felis.datamodel.DataType]]:
                return []

            def rows(self) -> Collection[tuple]:
                return []

        # Create and store the test update records.
        update_records = _create_test_update_records()
        test_replica_chunk_id = 12345
        self.ppdb.store(
            ReplicaChunk(
                id=test_replica_chunk_id,
                last_update_time=astropy.time.Time("2021-01-01T00:01:00", format="isot", scale="tai"),
                unique_id=uuid.uuid4(),
            ),
            objects=DummyApdbTableData(),
            sources=DummyApdbTableData(),
            forced_sources=DummyApdbTableData(),
            update_records=update_records.records,
            update=True,
        )

        # Configure and run the uploader without publishing to Pub/Sub.
        with patch.object(ChunkUploader, "_post_to_stage_chunk_topic"):
            ChunkUploader(
                self.ppdb,
                wait_interval=0,
                exit_on_empty=True,
                exit_on_error=True,
            ).run()

        # Apply the updates to the target tables using the UpdatesManager.
        updates_manager = UpdatesManager(self.ppdb.config)
        replica_chunks = self.ppdb.query_chunks(
            self.ppdb.chunk_table.columns["apdb_replica_chunk"].in_([test_replica_chunk_id])
        )
        updates_manager.apply_updates(replica_chunks)

        client = bigquery.Client()

        # Verify DiaSource updates.
        dia_source_fqn = self.ppdb.config.fqn_for(DatasetType.INTERNAL, "DiaSource")
        rows = client.query(
            f"""
            SELECT
              diaSourceId,
              diaObjectId,
              ssObjectId,
              ssObjectReassocTimeMjdTai,
              timeWithdrawnMjdTai
            FROM `{dia_source_fqn}`
            WHERE diaSourceId IN (100001, 100002, 100003, 100004)
            """
        ).result()

        dia_sources = {row["diaSourceId"]: row for row in rows}

        self.assertEqual(set(dia_sources), {100001, 100002, 100003, 100004})

        # Latest reassign record should win: 400001, not 300001.
        self.assertEqual(dia_sources[100001]["diaObjectId"], 400001)
        self.assertIsNone(dia_sources[100001]["ssObjectId"])
        self.assertIsNone(dia_sources[100001]["ssObjectReassocTimeMjdTai"])
        self.assertIsNone(dia_sources[100001]["timeWithdrawnMjdTai"])

        # Reassigned to SSObject.
        self.assertEqual(dia_sources[100002]["ssObjectId"], 2001)
        self.assertEqual(
            dia_sources[100002]["ssObjectReassocTimeMjdTai"],
            59580.0,
        )
        self.assertIsNone(dia_sources[100002]["timeWithdrawnMjdTai"])

        # Withdrawn DiaSource.
        self.assertEqual(
            dia_sources[100003]["timeWithdrawnMjdTai"],
            59580.0,
        )

        # Unaffected DiaSource should remain unchanged.
        self.assertEqual(dia_sources[100004]["diaObjectId"], 200004)
        self.assertIsNone(dia_sources[100004]["ssObjectId"])
        self.assertIsNone(dia_sources[100004]["ssObjectReassocTimeMjdTai"])
        self.assertIsNone(dia_sources[100004]["timeWithdrawnMjdTai"])

        # Verify DiaForcedSource updates.
        dia_forced_source_fqn = self.ppdb.config.fqn_for(DatasetType.INTERNAL, "DiaForcedSource")
        rows = client.query(
            f"""
            SELECT
              diaObjectId,
              visit,
              detector,
              timeWithdrawnMjdTai
            FROM `{dia_forced_source_fqn}`
            WHERE diaObjectId = 200001
              AND visit IN (12345, 12346)
              AND detector = 42
            """
        ).result()

        forced_sources = {(row["diaObjectId"], row["visit"], row["detector"]): row for row in rows}

        self.assertEqual(
            set(forced_sources),
            {
                (200001, 12345, 42),
                (200001, 12346, 42),
            },
        )

        # Withdrawn DiaForcedSource.
        self.assertEqual(
            forced_sources[(200001, 12345, 42)]["timeWithdrawnMjdTai"],
            59580.0,
        )

        # Unaffected DiaForcedSource should remain unchanged.
        self.assertIsNone(
            forced_sources[(200001, 12346, 42)]["timeWithdrawnMjdTai"],
        )

        # Verify DiaObject updates.
        dia_object_fqn = self.ppdb.config.fqn_for(DatasetType.INTERNAL, "DiaObject")
        rows = client.query(
            f"""
            SELECT
              diaObjectId,
              validityEndMjdTai,
              nDiaSources
            FROM `{dia_object_fqn}`
            WHERE diaObjectId IN (200001, 200002, 200003)
            """
        ).result()

        dia_objects = {row["diaObjectId"]: row for row in rows}

        self.assertEqual(set(dia_objects), {200001, 200002, 200003})

        # Closed validity interval.
        self.assertEqual(
            dia_objects[200001]["validityEndMjdTai"],
            59580.0,
        )
        self.assertEqual(dia_objects[200001]["nDiaSources"], 5)

        # Latest nDiaSources update should win: 10, not older value 8.
        self.assertIsNone(dia_objects[200002]["validityEndMjdTai"])
        self.assertEqual(dia_objects[200002]["nDiaSources"], 10)

        # Unaffected DiaObject should remain unchanged.
        self.assertEqual(
            dia_objects[200003]["validityEndMjdTai"],
            59000.0,
        )
        self.assertEqual(dia_objects[200003]["nDiaSources"], 2)
