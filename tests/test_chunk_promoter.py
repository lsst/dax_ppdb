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

import pandas as pd
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
    TableRefs,
)
from lsst.dax.ppdb.bigquery.updates import ExpandedUpdateRecord, UpdateRecordExpander, UpdateRecords
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

    Uses fill_apdb + Replicator to generate test data, then loads parquet files
    directly into BigQuery prod and staging tables (simulating the Dataflow
    staging). Only the parquet file containing update records is uploaded to
    GCS, as it is read directly during the promotion process.
    """

    def setUp(self):
        """Set up the test case.

        This is complicated because we need to perform all steps of the
        workflow up to promotion, including mock staging. The upload to
        cloud storage is done manually as only the updates file is needed
        for promotion. The table data can be read locally for insertion into
        the staging tables.
        """
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
        self._table_refs = TableRefs(
            project_id=project_id,
            dataset_id=dataset_id,
            table_names=tuple(_TABLE_NAMES),
        )

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

        # Create GCS bucket needed for storing parquet with update records.
        storage_client = storage.Client()
        self._bucket = storage_client.bucket(bucket_name)
        self._bucket.create(location="US")

        # Set chunk statuses and upload only the updates file to GCS. Table
        # data is loaded from local parquet files directly into BQ, bypassing
        # GCS entirely. We don't use the standard chunk_promoter because we
        # are not attempting to test that functionality here and the parquet
        # files do not need to be uploaded for the test.
        for chunk in self.ppdb.query_chunks():
            manifest = Manifest.from_json_file(chunk.manifest_path)
            status = ChunkStatus.UPLOADED if manifest.has_table_data() else ChunkStatus.STAGED
            gcs_prefix = f"data/test/{chunk.id}"
            gcs_uri = f"gs://{bucket_name}/{gcs_prefix}"

            update_records_path = chunk.directory / UpdateRecords.PARQUET_FILE_NAME
            if update_records_path.exists():
                blob = self._bucket.blob(f"{gcs_prefix}/{UpdateRecords.PARQUET_FILE_NAME}")
                blob.upload_from_filename(str(update_records_path))

            self.ppdb.update_chunks(
                [chunk.with_new_status(status).with_new_gcs_uri(gcs_uri)], fields={"status", "gcs_uri"}
            )

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

    def _create_empty_prod_tables(self, chunk: PpdbReplicaChunkExtended) -> None:
        """Create empty prod tables by loading a chunk's parquet files
        and then truncating them. This gives BigQuery the correct schema.
        """
        for table_name, prod_ref in zip(_TABLE_NAMES, self._table_refs.prod, strict=True):
            parquet_path = chunk.directory / f"{table_name}.parquet"
            if parquet_path.exists():
                self._load_parquet_to_table(parquet_path, prod_ref)
                self.bq_client.query(f"TRUNCATE TABLE `{prod_ref}`").result()

    def _load_chunk_to_staging(self, chunk: PpdbReplicaChunkExtended) -> None:
        """Load a chunk's parquet files into staging tables and add the
        ``apdb_replica_chunk`` column, simulating the Dataflow staging step.
        This mocks the cloud function which stages the data using Dataflow.
        """
        for table_name, staging_ref in zip(_TABLE_NAMES, self._table_refs.staging, strict=True):
            parquet_path = chunk.directory / f"{table_name}.parquet"
            if parquet_path.exists():
                self._load_parquet_to_table(parquet_path, staging_ref)
                self.bq_client.query(
                    f"ALTER TABLE `{staging_ref}` ADD COLUMN IF NOT EXISTS apdb_replica_chunk INT64"
                ).result()
                self.bq_client.query(
                    f"UPDATE `{staging_ref}` SET apdb_replica_chunk = {chunk.id} WHERE "
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

    def _get_expanded_updates(self) -> list[ExpandedUpdateRecord]:
        """Read update records from chunk parquet files and expand them."""
        expanded: list[ExpandedUpdateRecord] = []
        for chunk in self.ppdb.query_chunks():
            update_path = chunk.directory / UpdateRecords.PARQUET_FILE_NAME
            if update_path.exists():
                update_records = UpdateRecords.from_parquet_file(update_path)
                expanded.extend(UpdateRecordExpander.expand_updates(update_records, chunk.id))
        return expanded

    def _verify_promoted_data(
        self,
        staging_rows: dict[str, list[dict]],
        expanded_updates: list[ExpandedUpdateRecord],
    ) -> None:
        """Verify promoted prod data matches staging data with updates applied.

        For each table, builds an expected DataFrame from the staging snapshot
        with update records applied, then compares it against the actual prod
        DataFrame.
        """
        # Keys used by MERGE SQL to match update records to rows.
        merge_match_keys: dict[str, tuple[str, ...]] = {
            "DiaObject": ("diaObjectId",),
            "DiaSource": ("diaSourceId",),
            "DiaForcedSource": ("diaObjectId", "visit", "detector"),
        }

        # Columns that uniquely identify a row for comparison. DiaObject uses
        # a composite key because the same diaObjectId can appear with
        # different validity intervals.
        unique_row_keys = dict(merge_match_keys)
        unique_row_keys["DiaObject"] = ("diaObjectId", "validityStartMjdTai")

        for table_name, prod_table_ref in zip(
            self._table_refs.table_names, self._table_refs.prod, strict=True
        ):
            sort_columns = list(unique_row_keys[table_name])
            match_columns = list(merge_match_keys[table_name])

            # Build expected rows from staging with update records applied.
            expected_rows = []
            for row in staging_rows.get(table_name, []):
                row = dict(row)  # mutable copy
                row.pop("apdb_replica_chunk", None)
                expected_rows.append(row)
            for update in expanded_updates:
                if update.table_name != table_name or update.field_value is None:
                    continue
                for row in expected_rows:
                    if tuple(row[col] for col in match_columns) == update.record_id:
                        row[update.field_name] = update.field_value

            # Convert to DataFrames, sort, and compare.
            expected = pd.DataFrame(expected_rows).sort_values(sort_columns).reset_index(drop=True)
            actual = (
                pd.DataFrame(self._query_table(prod_table_ref))
                .sort_values(sort_columns)
                .reset_index(drop=True)
            )
            actual = actual[expected.columns]
            pd.testing.assert_frame_equal(actual, expected, atol=1e-5)

    def test_promote_chunks(self) -> None:
        """Test that promote_chunks moves staged data into prod, applies
        update records, cleans up tmp tables, and marks chunks PROMOTED.
        """
        all_chunks = self.ppdb.query_chunks()
        data_chunks = [c for c in all_chunks if c.status == ChunkStatus.UPLOADED]
        self.assertTrue(data_chunks, "Expected at least one data chunk")

        # Create empty prod tables (promoter clones them for the tmp tables).
        self._create_empty_prod_tables(data_chunks[0])

        # Stage all data chunks (simulating the Dataflow staging step).
        for chunk in data_chunks:
            self._load_chunk_to_staging(chunk)
            self.ppdb.update_chunks([chunk.with_new_status(ChunkStatus.STAGED)], fields={"status"})

        # Snapshot the staged rows per table before promotion.
        staging_rows: dict[str, list[dict]] = {}
        for table_name, staging_ref in zip(_TABLE_NAMES, self._table_refs.staging, strict=True):
            if self._table_exists(staging_ref):
                staging_rows[table_name] = self._query_table(staging_ref)

        # Expand update records before promotion for later verification.
        expanded_updates = self._get_expanded_updates()

        # Promote all chunks.
        chunks_to_promote = self.ppdb.get_promotable_chunks()
        promoter = ChunkPromoter(self.ppdb)
        promoter.promote_chunks(chunks_to_promote)

        # Verify staging tables are empty.
        for staging_ref in self._table_refs.staging:
            if self._table_exists(staging_ref):
                rows = self._query_table(staging_ref)
                self.assertEqual(len(rows), 0, f"{staging_ref} should be empty")

        # Verify tmp tables were cleaned up.
        for tmp_ref in self._table_refs.promoted_tmp:
            self.assertFalse(self._table_exists(tmp_ref), f"{tmp_ref} should not exist")

        # Verify all chunks are marked PROMOTED.
        for chunk in self.ppdb.query_chunks():
            self.assertEqual(chunk.status, ChunkStatus.PROMOTED, f"Chunk {chunk.id} should be PROMOTED")

        # Verify promoted data matches staging data with updates applied.
        self._verify_promoted_data(staging_rows, expanded_updates)

        # Verify no promotable chunks remain after promotion.
        self.assertEqual(self.ppdb.get_promotable_chunks(), [])

    def test_promote_chunks_empty(self) -> None:
        """Test that promoting an empty list raises NoPromotableChunksError."""
        promoter = ChunkPromoter(self.ppdb)
        with self.assertRaises(NoPromotableChunksError):
            promoter.promote_chunks([])


if __name__ == "__main__":
    unittest.main()
