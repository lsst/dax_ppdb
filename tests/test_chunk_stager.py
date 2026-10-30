# This file is part of dax_ppdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import json
import logging
import posixpath
import unittest
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from lsst.dax.apdb import Apdb, ApdbReplica
from lsst.dax.ppdb import Ppdb
from lsst.dax.ppdb.bigquery import (
    ChunkStager,
    ChunkStagingError,
    ChunkStagingRetryableError,
    ChunkStatus,
    ChunkStatusTransitionError,
    DatasetType,
    Manifest,
    PpdbBigQuery,
    UpdatableField,
)
from lsst.dax.ppdb.gcp import CloudEventLogger
from lsst.dax.ppdb.replicator import Replicator
from lsst.dax.ppdb.tests import (
    PostgresMixin,
    create_bucket,
    create_datasets,
    delete_bucket,
    drop_datasets,
    fill_apdb,
    have_valid_google_credentials,
)


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class ChunkStagerTestCase(PostgresMixin, unittest.TestCase):
    """Integration test for staging replica chunks with `ChunkStager`.

    Replicates test APDB data into Parquet files, uploads each chunk to GCS,
    creates the staging tables from the Parquet schemas, and stages the chunks
    into BigQuery.
    """

    def setUp(self):
        super().setUp()

        apdb_config = self.make_apdb_instance()
        fill_apdb(Apdb.from_config(apdb_config), include_update_records=True)

        self.config = self.make_instance(test_name="test_chunk_stager")
        self.ppdb = Ppdb.from_config(self.config)
        assert isinstance(self.ppdb, PpdbBigQuery)
        self.bq_client = bigquery.Client(project=self.config.project_id)

        self.addCleanup(drop_datasets, self.config)
        create_datasets(self.config)

        Replicator(
            ApdbReplica.from_config(apdb_config),
            self.ppdb,
            update=False,
            min_wait_time=0,
            max_wait_time=0,
            check_interval=0,
        ).run(exit_on_empty=True)

        bucket = create_bucket(self.config)
        self.addCleanup(delete_bucket, bucket)
        self.bucket = bucket

        # Upload each chunk with data to GCS, mark it uploaded, and create any
        # staging table it needs from its Parquet file's schema.
        self.manifests: dict[int, Manifest] = {}
        self.folders: dict[int, str] = {}
        for chunk in self.ppdb.query_chunks():
            chunk_dir = self.config.chunk_dir(chunk.id)
            manifest = Manifest.from_json_file(chunk_dir / Manifest.FILE_NAME)
            if manifest.is_empty_chunk:
                continue
            gcs_prefix = posixpath.join(self.config.object_prefix, str(chunk.id))
            for path in chunk_dir.iterdir():
                bucket.blob(f"{gcs_prefix}/{path.name}").upload_from_filename(str(path))
                if path.suffix == ".parquet":
                    self._create_staging_table(path)
            folder = f"gs://{self.config.bucket_name}/{gcs_prefix}"
            self.ppdb.update_chunks(
                [chunk.with_new_status(ChunkStatus.UPLOADED).with_new_gcs_uri(folder)],
                fields={UpdatableField.STATUS, UpdatableField.GCS_URI},
            )
            self.manifests[chunk.id] = manifest
            self.folders[chunk.id] = folder
        self.assertGreater(len(self.manifests), 1, "Test needs at least two chunks with data")

    def _create_staging_table(self, parquet_path: Path) -> None:
        """Create an empty staging table with the schema of a Parquet file, if
        it does not exist yet.
        """
        table_fqn = self.config.fqn_for(DatasetType.STAGING, parquet_path.stem)
        try:
            self.bq_client.get_table(table_fqn)
            return
        except NotFound:
            pass
        with open(parquet_path, "rb") as f:
            self.bq_client.load_table_from_file(
                f, table_fqn, job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
            ).result()
        self.bq_client.query(f"TRUNCATE TABLE `{table_fqn}`").result()

    def _staged_row_count(self, table_name: str, chunk_id: int) -> int:
        """Return the number of rows for a chunk in a staging table."""
        table_fqn = self.config.fqn_for(DatasetType.STAGING, table_name)
        sql = f"SELECT COUNT(*) AS n FROM `{table_fqn}` WHERE apdb_replica_chunk = {chunk_id}"
        return next(iter(self.bq_client.query(sql).result())).n

    def test_stage_chunks(self):
        """Stage every chunk, then check that a staged chunk is rejected and
        that a chunk whose data doesn't match its manifest fails without
        touching the staging tables.
        """
        logger = CloudEventLogger(logging.getLogger(__name__))

        # Read the manifest from GCS, as the stage_chunk function does.
        def stage(chunk_id: int) -> dict[str, str]:
            folder = self.folders[chunk_id]
            return ChunkStager(self.ppdb, logger, folder, Manifest.from_uri(folder)).stage_chunk()

        *good_chunk_ids, bad_chunk_id = sorted(self.manifests)

        # Each chunk is staged with exactly the rows its manifest lists, and
        # its status and job IDs are recorded.
        for chunk_id in good_chunk_ids:
            job_ids = stage(chunk_id)
            chunk = self.ppdb.find_chunk_by_id(chunk_id)
            self.assertEqual(chunk.status, ChunkStatus.STAGED)
            self.assertIsNone(chunk.error_message)
            self.assertEqual(chunk.bigquery_job_ids, job_ids)
            self.assertIn("commit", job_ids)
            for file_name, entry in self.manifests[chunk_id].files.items():
                table_name = file_name.removesuffix(".parquet")
                self.assertIn(table_name, job_ids)
                self.assertEqual(self._staged_row_count(table_name, chunk_id), entry.row_count)

        # A staged chunk can't be claimed again, and nothing changes.
        chunk_id = good_chunk_ids[0]
        with self.assertRaises(ChunkStatusTransitionError):
            stage(chunk_id)
        self.assertEqual(self.ppdb.find_chunk_by_id(chunk_id).status, ChunkStatus.STAGED)

        # A manifest row count that doesn't match the data fails the commit's
        # ASSERT, which is not retryable, so the chunk is set to failed.
        manifest = self.manifests[bad_chunk_id]
        file_name = next(iter(manifest.files))
        manifest.files[file_name].row_count += 1
        prefix = posixpath.join(self.config.object_prefix, str(bad_chunk_id))
        self.bucket.blob(f"{prefix}/{Manifest.FILE_NAME}").upload_from_string(
            json.dumps(manifest.model_dump(mode="json"))
        )
        with self.assertRaises(ChunkStagingError) as cm:
            stage(bad_chunk_id)
        self.assertNotIsInstance(cm.exception, ChunkStagingRetryableError)
        self.assertIn("commit", cm.exception.job_ids)
        chunk = self.ppdb.find_chunk_by_id(bad_chunk_id)
        self.assertEqual(chunk.status, ChunkStatus.FAILED)
        self.assertTrue(chunk.error_message.startswith("copying to staging:"))
        self.assertIn("does not match the manifest", chunk.error_message)
        for name in manifest.files:
            self.assertEqual(self._staged_row_count(name.removesuffix(".parquet"), bad_chunk_id), 0)


if __name__ == "__main__":
    unittest.main()
