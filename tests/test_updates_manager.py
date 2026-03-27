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

import astropy
import felis
from google.cloud import bigquery, storage

from lsst.dax.apdb import (
    ApdbTableData,
    ReplicaChunk,
)
from lsst.dax.ppdb import Ppdb
from lsst.dax.ppdb.bigquery import PpdbBigQuery
from lsst.dax.ppdb.bigquery.updates.updates_manager import UpdatesManager
from lsst.dax.ppdb.tests._bigquery import (
    ChunkUploaderWithoutPubSub,
    PostgresMixin,
    generate_test_bucket_name,
    have_valid_google_credentials,
    json_rows_to_buf,
)
from lsst.dax.ppdb.tests._updates import _create_test_update_records


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class UpdatesManagerTestCase(PostgresMixin, unittest.TestCase):
    """A test case for the handling of APDB record updates by PpdbBigQuery and
    related classes including the ChunkUploader.
    """

    def setUp(self):
        super().setUp()

        # Set up BigQuery client and test dataset
        self.bq_client = bigquery.Client()

        bucket_name = generate_test_bucket_name("ppdb-updates-manager-test")
        dataset_id = f"test_updates_manager_{uuid.uuid4().hex[:8]}"
        project_id = self.bq_client.project
        config = {
            "db_drop": True,
            "validate_config": False,
            "delete_existing_dirs": True,
            "bucket_name": bucket_name,
            "object_prefix": "data/test",
            "dataset_id": dataset_id,
            "project_id": project_id,
        }

        # Setup the Postgres database and create the config instance
        self.ppdb_config = self.make_instance(config)

        # Create the test dataset and tables in BigQuery
        self.target_dataset_fqn = f"{project_id}.{dataset_id}"
        self._create_test_dataset(self.bq_client, dataset_id)

        # Create the test GCS bucket
        storage_client = storage.Client()
        try:
            bucket = storage_client.bucket(self.ppdb_config.bucket_name)
            bucket.create(location="US")
        except Exception as e:
            self.fail(f"Failed to create test GCS bucket: {e}")

        # Create the PPDB instance
        self.ppdb = Ppdb.from_config(self.ppdb_config)
        assert isinstance(self.ppdb, PpdbBigQuery)

    def tearDown(self):
        # Delete the test dataset
        try:
            self.bq_client.delete_dataset(
                self.ppdb_config.dataset_id, delete_contents=True, not_found_ok=True
            )
        except Exception as e:
            print(f"Failed to delete test dataset: {e}")

        # Delete the test GCS bucket
        storage_client = storage.Client()
        try:
            bucket = storage_client.bucket(self.ppdb_config.bucket_name)
            blobs = list(bucket.list_blobs())
            for blob in blobs:
                blob.delete()
            bucket.delete()
        except Exception as e:
            print(f"Failed to delete test GCS bucket: {e}")

        super().tearDown()

    def _create_test_dataset(self, client: bigquery.Client, dataset_id: str) -> None:
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        client.create_dataset(dataset, exists_ok=False)

        # Create DiaObject table
        schema = [
            bigquery.SchemaField("diaObjectId", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("validityEndMjdTai", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("nDiaSources", "INTEGER", mode="NULLABLE"),
        ]
        table_fqn = f"{self.target_dataset_fqn}.DiaObject"
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

        # Create test DiaSource table
        schema = [
            bigquery.SchemaField("diaSourceId", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("diaObjectId", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ssObjectId", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ssObjectReassocTimeMjdTai", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("timeWithdrawnMjdTai", "FLOAT", mode="NULLABLE"),
        ]
        table_fqn = f"{self.target_dataset_fqn}.DiaSource"
        table = bigquery.Table(table_fqn, schema=schema)
        self.bq_client.create_table(table)
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

        # Create test DiaForcedSource table
        schema = [
            bigquery.SchemaField("diaObjectId", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("visit", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("detector", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("timeWithdrawnMjdTai", "FLOAT", mode="NULLABLE"),
        ]
        table_fqn = f"{self.target_dataset_fqn}.DiaForcedSource"
        table = bigquery.Table(table_fqn, schema=schema)
        self.bq_client.create_table(table)
        rows = [
            {"diaObjectId": 200001, "visit": 12345, "detector": 42, "timeWithdrawnMjdTai": None},
            {"diaObjectId": 200001, "visit": 12346, "detector": 42, "timeWithdrawnMjdTai": None},
        ]
        job = self.bq_client.load_table_from_file(
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

        # Create and store the test update records
        update_records = _create_test_update_records()
        self.ppdb.store(
            ReplicaChunk(
                id=update_records.replica_chunk_id,
                last_update_time=astropy.time.Time("2021-01-01T00:01:00", format="isot", scale="tai"),
                unique_id=uuid.uuid4(),
            ),
            objects=DummyApdbTableData(),
            sources=DummyApdbTableData(),
            forced_sources=DummyApdbTableData(),
            update_records=update_records.records,
            update=True,
        )

        # Configure and run the uploader without publishing to Pub/Sub
        uploader = ChunkUploaderWithoutPubSub(
            self.ppdb,
            wait_interval=0,
            exit_on_empty=True,
            exit_on_error=True,
        )
        uploader.run()

        # Apply the updates to the target tables using the UpdatesManager
        updates_manager = UpdatesManager(self.ppdb.config)
        replica_chunks = self.ppdb.get_replica_chunks_ext_by_ids([update_records.replica_chunk_id])
        updates_manager.apply_updates(replica_chunks)
