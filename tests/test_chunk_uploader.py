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
from hashlib import sha256

from google.cloud import storage

from lsst.dax.apdb import Apdb, ApdbReplica
from lsst.dax.ppdb import Ppdb
from lsst.dax.ppdb.bigquery import Manifest, PpdbBigQuery
from lsst.dax.ppdb.bigquery.updates import UpdateRecords
from lsst.dax.ppdb.replicator import Replicator
from lsst.dax.ppdb.tests import fill_apdb
from lsst.dax.ppdb.tests._bigquery import (
    ChunkUploaderWithoutPubSub,
    PostgresMixin,
    delete_test_bucket,
    generate_test_bucket_name,
    have_valid_google_credentials,
)


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class ChunkUploaderTestCase(PostgresMixin, unittest.TestCase):
    """Test that the ChunkUploader correctly uploads update records to GCS."""

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

        # Create a unique test bucket name and set up GCS resources.
        self.ppdb_config.bucket_name = generate_test_bucket_name("ppdb-test-gcs-upload")
        self._storage_client = storage.Client()
        self._bucket = self._storage_client.bucket(self.ppdb_config.bucket_name)
        self._bucket.create(location="US")

    def tearDown(self):
        # Delete the test GCS bucket.
        delete_test_bucket(self._bucket)
        super().tearDown()

    def test_chunk_uploader(self) -> None:
        """Test that the update records are correctly uploaded to Google Cloud
        Storage after replication.
        """
        # Configure and run the uploader.
        uploader = ChunkUploaderWithoutPubSub(
            self.ppdb,
            wait_interval=0,
            exit_on_empty=True,
            exit_on_error=True,
        )
        print(f"Uploader will copy files to {uploader.config.bucket_name}/{uploader.config.object_prefix}/")
        uploader.run()

        # Retrieve the update records file.
        blobs = list(self._bucket.list_blobs(match_glob="**/update_records.parquet"))
        update_records_files = [b.name for b in blobs]
        self.assertEqual(
            len(update_records_files),
            1,
            f"Expected exactly one update_records.parquet file in GCS, found "
            f"{len(update_records_files)}: {update_records_files}",
        )

        # Download the parquet file and deserialize the update records.
        update_records_bytes = blobs[0].download_as_bytes()
        update_records = UpdateRecords.from_parquet_bytes(update_records_bytes)
        self.assertEqual(
            len(update_records.records),
            3,
            f"Expected 3 update records in the file from GCS, found {len(update_records.records)}",
        )

        # Verify uploaded objects and chunk gcs_uri use the simplified
        # prefix/chunk_id target path (without date directories).
        # Find the chunk with update records (there should be at least one).
        chunks_with_updates = [c for c in self.ppdb.query_chunks() if c.update_count > 0]
        self.assertGreater(
            len(chunks_with_updates),
            0,
            "Expected at least one chunk with update records",
        )

        # Verify that the uploaded file is under one of the update chunks.
        found_match = False
        for uploaded_chunk in chunks_with_updates:
            expected_prefix = f"{self.ppdb.config.object_prefix}/{uploaded_chunk.id}"
            self.assertEqual(uploaded_chunk.gcs_uri, f"gs://{self.ppdb.config.bucket_name}/{expected_prefix}")

            for object_name in update_records_files:
                if object_name.startswith(expected_prefix + "/"):
                    found_match = True
                    break

        self.assertTrue(
            found_match,
            f"Expected update_records.parquet to be under one of the update chunks, but got {update_records_files}",
        )

        # Verify that none of the uploaded object paths include date directories.
        for object_name in update_records_files:
            self.assertNotRegex(
                object_name,
                r".*/\d{4}/\d{2}/\d{2}/.*",
                f"Found unexpected date-directory path in object name: {object_name}",
            )

        manifest_blob = self._bucket.blob(f"{expected_prefix}/{Manifest.FILE_NAME}")
        self.assertTrue(manifest_blob.exists())
        manifest = Manifest.from_json_str(manifest_blob.download_as_text())

        local_update_path = self.ppdb.config.chunk_dir(uploaded_chunk.id) / UpdateRecords.PARQUET_FILE_NAME
        expected_checksum = sha256(local_update_path.read_bytes()).hexdigest()
        self.assertIsNotNone(manifest.updates_data)
        assert manifest.updates_data is not None
        self.assertEqual(manifest.updates_data.checksum, expected_checksum)


if __name__ == "__main__":
    unittest.main()
