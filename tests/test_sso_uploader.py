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
import posixpath
import tempfile
import unittest
from collections.abc import Iterable
from pathlib import Path
from unittest.mock import Mock, patch

import google.auth
from google.api_core.exceptions import GoogleAPIError

from lsst.dax.ppdb.bigquery.schema.constants import SSO_TABLES
from lsst.dax.ppdb.bigquery.sso_uploader import SSOUploader, SSOUploaderConfg, SSOUploadError
from lsst.dax.ppdb.tests._bigquery import (
    create_bucket,
    delete_bucket,
    have_valid_google_credentials,
    make_bigquery_config,
)

_TEST_BUCKET_NAME = "test-bucket"
_TEST_OBJECT_PREFIX = "test/prefix"
_TEST_PUBSUB_TOPIC = "test-topic"


def _make_file_map(directory: Path, table_names: Iterable[str]) -> dict[str, Path]:
    """Make a file_map for SSOUploader pointing at empty dummy files.

    The files are empty placeholders since SSOUploader only checks for file
    existence and does not read or parse the file contents.
    """
    file_map = {}
    for table_name in table_names:
        path = directory / f"{table_name}.parquet"
        path.touch()
        file_map[table_name] = path
    return file_map


class SSOUploaderValidationTestCase(unittest.TestCase):
    """Test validation of the arguments passed to the SSOUploader
    constructor.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)

        self.config = make_bigquery_config(test_name="test_sso_uploader_validation")

    def test_invalid_table_name(self) -> None:
        """Test that a file_map with an unrecognized table name is
        rejected.
        """
        file_map = _make_file_map(Path(self.tempdir.name), SSO_TABLES)
        file_map["NotATable"] = Path(self.tempdir.name) / "NotATable.parquet"
        file_map["NotATable"].touch()
        config = SSOUploaderConfg(
            bucket_name=_TEST_BUCKET_NAME,
            object_prefix=_TEST_OBJECT_PREFIX,
            dataset_id=self.config.datasets.internal,
        )
        with self.assertRaises(SSOUploadError):
            SSOUploader(config, file_map)

    def test_missing_file(self) -> None:
        """Test that a file_map pointing at a nonexistent file is
        rejected.
        """
        file_map = _make_file_map(Path(self.tempdir.name), SSO_TABLES)
        file_map[SSO_TABLES[0]] = Path(self.tempdir.name) / "does_not_exist.parquet"
        config = SSOUploaderConfg(
            bucket_name=_TEST_BUCKET_NAME,
            object_prefix=_TEST_OBJECT_PREFIX,
            dataset_id=self.config.datasets.internal,
        )
        with self.assertRaises(SSOUploadError):
            SSOUploader(config, file_map)

    def test_duplicate_paths(self) -> None:
        """Test that a file_map with two tables pointing at the same file
        is rejected.
        """
        file_map = _make_file_map(Path(self.tempdir.name), SSO_TABLES)
        file_map[SSO_TABLES[1]] = file_map[SSO_TABLES[0]]
        config = SSOUploaderConfg(
            bucket_name=_TEST_BUCKET_NAME,
            object_prefix=_TEST_OBJECT_PREFIX,
            dataset_id=self.config.datasets.internal,
        )
        with self.assertRaises(SSOUploadError):
            SSOUploader(config, file_map)

    def test_missing_tables_rejected_when_partial_upload_disallowed(self) -> None:
        """Test that a file_map missing some SSO tables is rejected when
        allow_partial_upload is False.
        """
        file_map = _make_file_map(Path(self.tempdir.name), SSO_TABLES[:-1])
        config = SSOUploaderConfg(
            bucket_name=_TEST_BUCKET_NAME,
            object_prefix=_TEST_OBJECT_PREFIX,
            dataset_id=self.config.datasets.internal,
            allow_partial_upload=False,
        )
        with self.assertRaises(SSOUploadError):
            SSOUploader(config, file_map)

    def test_missing_tables_allowed_when_partial_upload_allowed(self) -> None:
        """Test that a file_map missing some SSO tables is accepted when
        allow_partial_upload is True.
        """
        file_map = _make_file_map(Path(self.tempdir.name), SSO_TABLES[:-1])
        config = SSOUploaderConfg(
            bucket_name=_TEST_BUCKET_NAME,
            object_prefix=_TEST_OBJECT_PREFIX,
            dataset_id=self.config.datasets.internal,
            allow_partial_upload=True,
        )
        uploader = SSOUploader(config, file_map)
        self.assertEqual(uploader._file_map, file_map)


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class SSOUploaderUploadTestCase(unittest.TestCase):
    """Test that SSOUploader correctly uploads SSO parquet files to Google
    Cloud Storage and publishes a Pub/Sub message on completion.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)

        self.config = make_bigquery_config(test_name="test_sso_uploader")
        self.bucket = create_bucket(self.config)
        self.addCleanup(delete_bucket, self.bucket)

        self.file_map = _make_file_map(Path(self.tempdir.name), SSO_TABLES)

    def test_upload_without_pubsub_topic(self) -> None:
        """Test that files are uploaded to GCS and that the publish step is
        skipped when no pubsub_topic is configured.
        """
        config = SSOUploaderConfg(
            bucket_name=self.config.bucket_name,
            object_prefix=_TEST_OBJECT_PREFIX,
            dataset_id=self.config.datasets.internal,
            pubsub_topic="",
        )
        uploader = SSOUploader(config, self.file_map)
        uploader.upload()

        for table_name in SSO_TABLES:
            blob = self.bucket.blob(f"{_TEST_OBJECT_PREFIX}/{table_name}.parquet")
            self.assertTrue(blob.exists(), f"Expected {table_name}.parquet to be uploaded")

    def test_upload_with_pubsub_topic(self) -> None:
        """Test that files are uploaded to GCS and that a Pub/Sub message is
        published when pubsub_topic is configured.

        The Pub/Sub publisher is mocked to avoid depending on a real topic.
        """
        config = SSOUploaderConfg(
            bucket_name=self.config.bucket_name,
            object_prefix=_TEST_OBJECT_PREFIX,
            dataset_id=self.config.datasets.internal,
            pubsub_topic=_TEST_PUBSUB_TOPIC,
        )
        uploader = SSOUploader(config, self.file_map)

        _, project_id = google.auth.default()
        expected_topic_path = f"projects/{project_id}/topics/{_TEST_PUBSUB_TOPIC}"

        mock_future = Mock()
        with patch("lsst.dax.ppdb.bigquery.sso_uploader.pubsub_v1.PublisherClient") as mock_publisher_cls:
            mock_publisher = mock_publisher_cls.return_value
            mock_publisher.topic_path.return_value = expected_topic_path
            mock_publisher.publish.return_value = mock_future

            uploader.upload()

        for table_name in SSO_TABLES:
            blob = self.bucket.blob(f"{_TEST_OBJECT_PREFIX}/{table_name}.parquet")
            self.assertTrue(blob.exists(), f"Expected {table_name}.parquet to be uploaded")

        mock_publisher.topic_path.assert_called_once_with(project_id, _TEST_PUBSUB_TOPIC)
        mock_future.result.assert_called_once()

        published_topic, published_bytes = mock_publisher.publish.call_args[0]
        self.assertEqual(published_topic, expected_topic_path)
        message = json.loads(published_bytes.decode("utf-8"))
        self.assertEqual(message["bucket"], self.config.bucket_name)
        self.assertEqual(message["object_prefix"], _TEST_OBJECT_PREFIX)
        self.assertEqual(set(message["uploaded_tables"]), set(SSO_TABLES))


class SSOUploaderCleanupTestCase(unittest.TestCase):
    """Test that SSOUploader deletes previously uploaded files from Google
    Cloud Storage when an upload or publish operation fails.

    The Google Cloud Storage and Pub/Sub clients are mocked so these tests
    do not require valid Google credentials.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)

        self.config = make_bigquery_config(test_name="test_sso_uploader_cleanup")
        self.file_map = _make_file_map(Path(self.tempdir.name), SSO_TABLES)
        self.object_names = [
            posixpath.join(_TEST_OBJECT_PREFIX, f"{table_name}.parquet") for table_name in self.file_map
        ]
        self.blobs_by_name = {name: Mock() for name in self.object_names}

        bucket = Mock()
        bucket.blob.side_effect = lambda name: self.blobs_by_name[name]
        self.bucket = bucket

    def _make_uploader(self, pubsub_topic: str = "") -> SSOUploader:
        config = SSOUploaderConfg(
            bucket_name=self.config.bucket_name,
            object_prefix=_TEST_OBJECT_PREFIX,
            dataset_id=self.config.datasets.internal,
            pubsub_topic=pubsub_topic,
        )
        return SSOUploader(config, self.file_map)

    def test_upload_failure_cleans_up_previously_uploaded_files(self) -> None:
        """Test that files uploaded before an upload failure are deleted
        from GCS, and that files after the failure are never attempted.
        """
        failing_object_name = self.object_names[1]
        self.blobs_by_name[failing_object_name].upload_from_filename.side_effect = GoogleAPIError("boom")

        uploader = self._make_uploader()
        with patch("lsst.dax.ppdb.bigquery.sso_uploader.Client") as mock_client_cls:
            mock_client_cls.return_value.bucket.return_value = self.bucket
            with self.assertRaises(SSOUploadError):
                uploader.upload()

        self.blobs_by_name[self.object_names[0]].delete.assert_called_once()
        self.blobs_by_name[failing_object_name].delete.assert_not_called()
        for name in self.object_names[2:]:
            self.blobs_by_name[name].upload_from_filename.assert_not_called()
            self.blobs_by_name[name].delete.assert_not_called()

    def test_publish_failure_cleans_up_uploaded_files(self) -> None:
        """Test that all uploaded files are deleted from GCS when
        publishing the Pub/Sub message fails.
        """
        uploader = self._make_uploader(pubsub_topic=_TEST_PUBSUB_TOPIC)

        with (
            patch("lsst.dax.ppdb.bigquery.sso_uploader.Client") as mock_client_cls,
            patch("lsst.dax.ppdb.bigquery.sso_uploader.pubsub_v1.PublisherClient") as mock_publisher_cls,
        ):
            mock_client_cls.return_value.bucket.return_value = self.bucket
            mock_publisher_cls.return_value.publish.side_effect = GoogleAPIError("boom")

            with self.assertRaises(SSOUploadError):
                uploader.upload()

        for name in self.object_names:
            self.blobs_by_name[name].upload_from_filename.assert_called_once()
            self.blobs_by_name[name].delete.assert_called_once()

    def test_cleanup_failure_does_not_mask_original_error(self) -> None:
        """Test that an error while deleting an uploaded file during cleanup
        is logged but does not prevent the original error from being raised.
        """
        failing_object_name = self.object_names[1]
        self.blobs_by_name[failing_object_name].upload_from_filename.side_effect = GoogleAPIError(
            "upload boom"
        )
        self.blobs_by_name[self.object_names[0]].delete.side_effect = GoogleAPIError("delete boom")

        uploader = self._make_uploader()
        with patch("lsst.dax.ppdb.bigquery.sso_uploader.Client") as mock_client_cls:
            mock_client_cls.return_value.bucket.return_value = self.bucket
            with self.assertRaises(SSOUploadError) as cm:
                uploader.upload()

        self.assertIn("upload boom", str(cm.exception.__cause__))
        self.blobs_by_name[self.object_names[0]].delete.assert_called_once()


if __name__ == "__main__":
    unittest.main()
