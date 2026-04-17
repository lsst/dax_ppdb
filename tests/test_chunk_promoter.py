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
from pathlib import Path

from google.cloud import bigquery, storage

from lsst.dax.apdb import Apdb, ApdbReplica
from lsst.dax.ppdb import Ppdb
from lsst.dax.ppdb.bigquery import (
    ChunkPromoter,
    ChunkStatus,
    Manifest,
    NoPromotableChunksError,
    PpdbBigQuery,
    PpdbReplicaChunkExtended,
)
from lsst.dax.ppdb.replicator import Replicator
from lsst.dax.ppdb.tests import fill_apdb
from lsst.dax.ppdb.tests._bigquery import (
    PostgresMixin,
    delete_test_bucket,
    generate_test_bucket_name,
    have_valid_google_credentials,
)

_TABLE_NAMES = ["DiaObject", "DiaSource", "DiaForcedSource"]


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class ChunkPromoterTestCase(PostgresMixin, unittest.TestCase):
    """Integration test for the ChunkPromoter promotion workflow.

    Uses fill_apdb + Replicator to generate realistic test data, then
    loads parquet files directly into BigQuery prod and staging tables
    (simulating the Dataflow staging step). Only update_records.parquet
    is uploaded to GCS, as required by the ChunkPromoter's UpdatesManager.
    """

    def setUp(self):
        super().setUp()

        # Create APDB and fill with test data including update records.
        apdb_config = self.make_apdb_instance()
        apdb = Apdb.from_config(apdb_config)
        fill_apdb(apdb, include_update_records=True)
        apdb_replica = ApdbReplica.from_config(apdb_config)

        # Create PPDB config with unique BQ dataset and GCS bucket.
        self.bq_client = bigquery.Client()
        dataset_id = f"test_promoter_{uuid.uuid4().hex[:8]}"
        project_id = self.bq_client.project
        bucket_name = generate_test_bucket_name("ppdb-promoter-test")
        config = {
            "db_drop": True,
            "validate_config": False,
            "delete_existing_dirs": True,
            "bucket_name": bucket_name,
            "object_prefix": "data/test",
            "dataset_id": dataset_id,
            "project_id": project_id,
        }
        self.ppdb_config = self.make_instance(config)
        self.target_dataset_fqn = f"{project_id}.{dataset_id}"

        # Create the PPDB instance and replicate APDB data.
        self.ppdb = Ppdb.from_config(self.ppdb_config)
        assert isinstance(self.ppdb, PpdbBigQuery)
        replicator = Replicator(
            apdb_replica,
            self.ppdb,
            update=False,
            min_wait_time=0,
            max_wait_time=0,
            check_interval=0,
        )
        replicator.run(exit_on_empty=True)

        # Create GCS bucket (needed for update_records.parquet, which
        # ChunkPromoter's UpdatesManager downloads from GCS).
        storage_client = storage.Client()
        self._bucket = storage_client.bucket(bucket_name)
        self._bucket.create(location="US")

        # Set chunk statuses and upload only update_records.parquet to
        # GCS. Table data is loaded from local parquet files directly
        # into BQ, bypassing GCS entirely.
        for chunk in self.ppdb.query_chunks():
            manifest = Manifest.from_json_file(chunk.manifest_path)
            status = ChunkStatus.UPLOADED if manifest.has_table_data() else ChunkStatus.STAGED
            gcs_prefix = f"data/test/{chunk.id}"
            gcs_uri = f"gs://{bucket_name}/{gcs_prefix}"

            update_records_path = chunk.directory / "update_records.parquet"
            if update_records_path.exists():
                blob = self._bucket.blob(f"{gcs_prefix}/update_records.parquet")
                blob.upload_from_filename(str(update_records_path))

            self.ppdb.update_chunks([chunk.with_new_status(status).with_new_gcs_uri(gcs_uri)])

        # Create the BQ dataset.
        dataset = bigquery.Dataset(self.target_dataset_fqn)
        self.bq_client.create_dataset(dataset, exists_ok=False)

    def tearDown(self):
        try:
            self.bq_client.delete_dataset(
                self.ppdb_config.dataset_id, delete_contents=True, not_found_ok=True
            )
        except Exception as e:
            print(f"Failed to delete test dataset: {e}")
        try:
            delete_test_bucket(self._bucket)
        except Exception as e:
            print(f"Failed to delete test GCS bucket: {e}")
        super().tearDown()

    def _load_parquet_to_table(self, parquet_path: Path, table_fqn: str) -> None:
        """Load a local parquet file into a BigQuery table, creating the table
        from the parquet schema if it does not exist.
        """
        with open(parquet_path, "rb") as f:
            job = self.bq_client.load_table_from_file(
                f,
                table_fqn,
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                ),
            )
            job.result()

    def _load_chunk_to_prod(self, chunk: PpdbReplicaChunkExtended) -> None:
        """Load a chunk's parquet files into the production tables."""
        for table_name in _TABLE_NAMES:
            parquet_path = chunk.directory / f"{table_name}.parquet"
            if parquet_path.exists():
                self._load_parquet_to_table(parquet_path, f"{self.target_dataset_fqn}.{table_name}")

    def _load_chunk_to_staging(self, chunk: PpdbReplicaChunkExtended) -> None:
        """Load a chunk's parquet files into staging tables and add the
        ``apdb_replica_chunk`` column, simulating the Dataflow staging step.
        """
        for table_name in _TABLE_NAMES:
            parquet_path = chunk.directory / f"{table_name}.parquet"
            staging_fqn = f"{self.target_dataset_fqn}._{table_name}_staging"
            if parquet_path.exists():
                self._load_parquet_to_table(parquet_path, staging_fqn)
                self.bq_client.query(
                    f"ALTER TABLE `{staging_fqn}` ADD COLUMN IF NOT EXISTS apdb_replica_chunk INT64"
                ).result()
                self.bq_client.query(
                    f"UPDATE `{staging_fqn}` SET apdb_replica_chunk = {chunk.id} WHERE "
                    f"apdb_replica_chunk IS NULL"
                ).result()

    def _query_table(self, table_fqn: str) -> list[dict]:
        """Query all rows from a BQ table."""
        rows = list(self.bq_client.query(f"SELECT * FROM `{table_fqn}`").result())
        return [dict(row) for row in rows]

    def _table_exists(self, table_fqn: str) -> bool:
        """Check if a BQ table exists."""
        try:
            self.bq_client.get_table(table_fqn)
            return True
        except Exception:
            return False

    def _count_rows(self, table_fqn: str) -> int:
        """Count rows in a BQ table."""
        rows = list(self.bq_client.query(f"SELECT COUNT(*) as cnt FROM `{table_fqn}`").result())
        return rows[0]["cnt"]

    def test_promote_chunks(self) -> None:
        """Test that promote_chunks moves staged data into prod, applies
        update records, cleans up tmp tables, marks chunks as PROMOTED,
        and does not affect non-promoted chunks.
        """
        # Get all chunks after upload. Data chunks are UPLOADED (need
        # Dataflow staging); the updates-only chunk is already STAGED.
        all_chunks = self.ppdb.query_chunks()
        data_chunks = [c for c in all_chunks if c.status == ChunkStatus.UPLOADED]
        updates_chunks = [c for c in all_chunks if c.status == ChunkStatus.STAGED and c.update_count > 0]

        self.assertGreaterEqual(len(data_chunks), 3, "Need at least 3 data chunks")
        self.assertEqual(len(updates_chunks), 1, "Expected exactly 1 updates-only chunk")

        # The last data chunk contains records targeted by the update records
        # (from the last visit in fill_apdb). Load it into prod so the
        # updates have matching rows to modify.
        prod_chunk = data_chunks[-1]
        self._load_chunk_to_prod(prod_chunk)
        self.ppdb.update_chunks([prod_chunk.with_new_status(ChunkStatus.PROMOTED)])

        # Stage a different data chunk (simulating Dataflow staging).
        staged_chunk = data_chunks[0]
        self._load_chunk_to_staging(staged_chunk)
        self.ppdb.update_chunks([staged_chunk.with_new_status(ChunkStatus.STAGED)])

        # Remaining data chunks stay as UPLOADED (should not be affected).
        uploaded_chunks = data_chunks[1:-1]

        # Record initial prod row counts before promotion.
        initial_prod_counts = {}
        staging_counts = {}
        for table_name in _TABLE_NAMES:
            initial_prod_counts[table_name] = self._count_rows(f"{self.target_dataset_fqn}.{table_name}")
            staging_fqn = f"{self.target_dataset_fqn}._{table_name}_staging"
            if self._table_exists(staging_fqn):
                staging_counts[table_name] = self._count_rows(staging_fqn)

        # Promote the staged data chunk and the updates chunk.
        # Re-query to get the chunks with their current DB state.
        table = self.ppdb.get_table("PpdbReplicaChunk")
        promote_ids = [staged_chunk.id, updates_chunks[0].id]
        chunks_to_promote = self.ppdb.query_chunks(table.columns["apdb_replica_chunk"].in_(promote_ids))

        promoter = ChunkPromoter(self.ppdb)
        promoter.promote_chunks(chunks_to_promote)

        # 1. Verify prod tables grew by the staged row counts.
        for table_name in _TABLE_NAMES:
            expected = initial_prod_counts[table_name] + staging_counts.get(table_name, 0)
            actual = self._count_rows(f"{self.target_dataset_fqn}.{table_name}")
            self.assertEqual(actual, expected, f"Expected {expected} rows in {table_name}, got {actual}")

        # 2. Verify staging tables have promoted rows deleted.
        for table_name in _TABLE_NAMES:
            staging_fqn = f"{self.target_dataset_fqn}._{table_name}_staging"
            if self._table_exists(staging_fqn):
                self.assertEqual(
                    self._count_rows(staging_fqn),
                    0,
                    f"Expected 0 rows in _{table_name}_staging after promotion",
                )

        # 3. Verify promoted tmp tables were cleaned up.
        for table_name in _TABLE_NAMES:
            tmp_fqn = f"{self.target_dataset_fqn}._{table_name}_promoted_tmp"
            self.assertFalse(self._table_exists(tmp_fqn), f"Tmp table {tmp_fqn} should not exist")

        # 4. Verify chunk statuses in the SQL database.
        result_chunks = {c.id: c for c in self.ppdb.query_chunks()}

        for chunk in chunks_to_promote:
            self.assertEqual(
                result_chunks[chunk.id].status,
                ChunkStatus.PROMOTED,
                f"Chunk {chunk.id} should be PROMOTED",
            )
        for chunk in uploaded_chunks:
            self.assertEqual(
                result_chunks[chunk.id].status,
                ChunkStatus.UPLOADED,
                f"Chunk {chunk.id} should still be UPLOADED",
            )

        # 5. Verify that update records were applied. The test update
        #    records include a DiaObject validity closure (sets
        #    validityEndMjdTai) and a DiaForcedSource withdrawal (sets
        #    timeWithdrawnMjdTai).
        dia_objects = self._query_table(f"{self.target_dataset_fqn}.DiaObject")
        closed_objects = [r for r in dia_objects if r.get("validityEndMjdTai") is not None]
        self.assertGreater(
            len(closed_objects),
            0,
            "Expected at least one DiaObject with validityEndMjdTai set by updates",
        )

        dia_forced_sources = self._query_table(f"{self.target_dataset_fqn}.DiaForcedSource")
        withdrawn_fs = [r for r in dia_forced_sources if r.get("timeWithdrawnMjdTai") is not None]
        self.assertGreater(
            len(withdrawn_fs),
            0,
            "Expected at least one DiaForcedSource with timeWithdrawnMjdTai set by updates",
        )

    def test_promote_chunks_empty(self) -> None:
        """Test that promoting an empty list raises NoPromotableChunksError."""
        promoter = ChunkPromoter(self.ppdb)
        with self.assertRaises(NoPromotableChunksError):
            promoter.promote_chunks([])


if __name__ == "__main__":
    unittest.main()
