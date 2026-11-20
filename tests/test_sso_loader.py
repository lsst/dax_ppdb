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

import base64
import json
import logging
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest import mock

import pyarrow
from cloudevents.http import CloudEvent
from google.api_core.exceptions import ServiceUnavailable
from google.cloud import bigquery
from pyarrow import parquet

from lsst.dax.ppdb.bigquery import DatasetType
from lsst.dax.ppdb.bigquery.sso.loader import Loader, LoadError
from lsst.dax.ppdb.bigquery.sso.manifest import FileEntry, Manifest
from lsst.dax.ppdb.bigquery.sso.uploader import Uploader, UploaderConfig
from lsst.dax.ppdb.gcp import CloudEventLogger
from lsst.dax.ppdb.tests import (
    create_bucket,
    create_datasets,
    delete_bucket,
    drop_datasets,
    have_valid_google_credentials,
    make_bigquery_config,
)

_UPLOAD_PREFIX = "20261118T120000000"

# Prefixes of two SSObject-only uploads: one with an extra column, which
# BigQuery ignores, and one with ssObjectId as a string, which doesn't match
# the table.
_EXTRA_COLUMN_PREFIX = "20261118T120001000"
_WRONG_TYPE_PREFIX = "20261118T120002000"

# Schemas of the internal tables used by the tests, a small subset of the real
# SSO columns.
_SCHEMAS = {
    "SSObject": [
        bigquery.SchemaField("ssObjectId", "INT64"),
        bigquery.SchemaField("numObs", "INT64"),
    ],
    "SSObservation": [
        bigquery.SchemaField("ssObjectId", "INT64"),
        bigquery.SchemaField("diaSourceId", "INT64"),
    ],
    "NearbySSO": [
        bigquery.SchemaField("diaSourceId", "INT64"),
        bigquery.SchemaField("ssObjectId", "INT64"),
    ],
}

# The row each internal table holds before a load, in its column order.
_OLD_ROWS = {
    "SSObject": (21165641428800001, 5),
    "SSObservation": (21165641428800001, 170055002004400001),
    "NearbySSO": (170055002004400001, 21165641428800001),
}


def _make_event(data: dict[str, str]) -> CloudEvent:
    """Return a Pub/Sub CloudEvent whose message holds ``data`` as JSON.

    Parameters
    ----------
    data
        The message data.

    Returns
    -------
    `cloudevents.http.CloudEvent`
        The event.
    """
    message = {"data": base64.b64encode(json.dumps(data).encode("utf-8")).decode("ascii")}
    return CloudEvent(
        {
            "id": "17305830183520001",
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/ppdb-test-project/topics/load-sso-topic",
        },
        {"message": message},
    )


class LoaderErrorTestCase(unittest.TestCase):
    """Test Loader with BigQuery and the manifest mocked, including how it
    handles errors.

    These tests need no credentials.
    """

    def setUp(self) -> None:
        # Fake manifest with one SSObject file, returned instead of reading the
        # manifest from GCS.
        manifest = Manifest(
            tables={
                "SSObject": {
                    "part-00000.parquet": FileEntry(
                        row_count=3,
                        checksum="fd61a03af4f77d870fc21e05e7e80678095c92d808cfb3b5c279ee04c74aca13",
                        size_bytes=768,
                        source_path="/data/sso/SSObject.parquet",
                    )
                }
            },
        )
        patcher = mock.patch("lsst.dax.ppdb.bigquery.sso.loader.Manifest.from_uri", return_value=manifest)
        self.from_uri = patcher.start()
        self.addCleanup(patcher.stop)

        # Fake BigQuery client that returns the SSObject schema.
        self.client = mock.MagicMock(spec=bigquery.Client)
        self.client.get_table.return_value = mock.MagicMock(schema=_SCHEMAS["SSObject"])

        # Fake logger and result publisher, and a loader that uses the fakes.
        self.logger = mock.MagicMock(spec=CloudEventLogger)
        patcher = mock.patch("lsst.dax.ppdb.bigquery.sso.loader.ResultPublisher")
        self.result_publisher = patcher.start().return_value
        self.addCleanup(patcher.stop)
        self.loader = Loader(
            make_bigquery_config(test_name="test_sso_loader_errors"),
            self.client,
            job_timeout=timedelta(minutes=1),
            retry_timeout=timedelta(seconds=1),
        )

    def test_non_sso_table_rejected(self) -> None:
        """Test that a manifest naming a table that isn't an SSO table fails
        before anything is loaded.
        """
        # Make the manifest name DiaObject as well as SSObject.
        manifest = self.from_uri.return_value
        manifest.tables["DiaObject"] = manifest.tables["SSObject"]

        # Check that the load fails and touches no table.
        with self.assertRaisesRegex(LoadError, "aren't SSO tables: \\['DiaObject'\\]"):
            self.loader._load(self.logger, "sso-upload-bucket", _UPLOAD_PREFIX)
        self.client.get_table.assert_not_called()
        self.client.load_table_from_uri.assert_not_called()
        self.client.query.assert_not_called()

    def test_load_from_event_bad_message(self) -> None:
        """Test that a message without an object prefix loads nothing and
        publishes no result.
        """
        # Handle a message missing its object prefix.
        event = _make_event({"bucket": "sso-upload-bucket"})
        self.loader.load_from_event(event, logging.getLogger(__name__))

        # Check that nothing was loaded or published.
        self.from_uri.assert_not_called()
        self.result_publisher.publish_succeeded.assert_not_called()
        self.result_publisher.publish_failed.assert_not_called()

    def test_server_error_raises_load_error(self) -> None:
        """Test that a Google Cloud error during the load fails it with
        LoadError and is logged.
        """
        # Make reading a table schema fail with a server error.
        error = ServiceUnavailable("Service unavailable")
        self.client.get_table.side_effect = error

        # Check that the load raises LoadError and logs the failure.
        with self.assertRaisesRegex(LoadError, "Service unavailable"):
            self.loader._load(self.logger, "sso-upload-bucket", _UPLOAD_PREFIX)
        self.logger.log_event.assert_called_with(
            logging.ERROR, "SSO load failed", "sso_load_failed", error=error
        )


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class LoaderTestCase(unittest.TestCase):
    """Integration test of Loader against BigQuery and Cloud Storage."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        directory = Path(self.tempdir.name)

        # Set up the landing and internal datasets and the upload bucket.
        self.config = make_bigquery_config(test_name="test_sso_loader")
        dataset_types = [DatasetType.LANDING, DatasetType.INTERNAL]
        self.addCleanup(drop_datasets, self.config, dataset_types)
        create_datasets(self.config, dataset_types)
        self.bucket = create_bucket(self.config)
        self.addCleanup(delete_bucket, self.bucket)
        self.bq_client = bigquery.Client(project=self.config.project_id)

        # Set up internal tables holding older rows, which the load replaces.
        for table_name, schema in _SCHEMAS.items():
            table_fqn = self.config.fqn_for(DatasetType.INTERNAL, table_name)
            self.bq_client.create_table(bigquery.Table(table_fqn, schema=schema))
            self.bq_client.query(f"INSERT INTO `{table_fqn}` VALUES {_OLD_ROWS[table_name]}").result()

        # Set up new data: SSObject as one file, SSObservation as two.
        ss_object_ids = [21165641428803584, 21165641428803585, 21165641428803586]
        file_map = {
            "SSObject": [directory / "SSObject.parquet"],
            "SSObservation": [directory / "SSObservation" / f"part-{index}.parquet" for index in (0, 1)],
        }
        file_map["SSObservation"][0].parent.mkdir()
        parquet.write_table(
            pyarrow.table({"ssObjectId": ss_object_ids, "numObs": [12, 7, 30]}), file_map["SSObject"][0]
        )
        parquet.write_table(
            pyarrow.table(
                {
                    "ssObjectId": [ss_object_ids[0]] * 4,
                    "diaSourceId": [170055002004447233 + index for index in range(4)],
                }
            ),
            file_map["SSObservation"][0],
        )
        parquet.write_table(
            pyarrow.table(
                {
                    "ssObjectId": [ss_object_ids[1]] * 2,
                    "diaSourceId": [170055002004447237 + index for index in range(2)],
                }
            ),
            file_map["SSObservation"][1],
        )

        # Upload the new data.
        self._upload(file_map, _UPLOAD_PREFIX)

        # Upload two SSObject files that differ from the table: one with an
        # extra pandas index column and its own objects, and one with
        # ssObjectId as a string.
        extra_column_path = directory / "extra_column" / "SSObject.parquet"
        extra_column_path.parent.mkdir()
        parquet.write_table(
            pyarrow.table(
                {
                    "ssObjectId": [21165641428803590, 21165641428803591],
                    "numObs": [4, 9],
                    "__index_level_0__": [0, 1],
                }
            ),
            extra_column_path,
        )
        self._upload({"SSObject": [extra_column_path]}, _EXTRA_COLUMN_PREFIX)
        wrong_type_path = directory / "wrong_type" / "SSObject.parquet"
        wrong_type_path.parent.mkdir()
        parquet.write_table(
            pyarrow.table(
                {"ssObjectId": [str(object_id) for object_id in ss_object_ids], "numObs": [12, 7, 30]}
            ),
            wrong_type_path,
        )
        self._upload({"SSObject": [wrong_type_path]}, _WRONG_TYPE_PREFIX)

        # Put a file in the upload's SSObservation folder that its manifest
        # doesn't list.
        stray_path = directory / "stray.parquet"
        parquet.write_table(
            pyarrow.table({"ssObjectId": [ss_object_ids[2]], "diaSourceId": [170055002004447299]}), stray_path
        )
        self.bucket.blob(f"{_UPLOAD_PREFIX}/SSObservation/stray.parquet").upload_from_filename(
            str(stray_path)
        )

        # Set up the loader, with its result publisher mocked.
        patcher = mock.patch("lsst.dax.ppdb.bigquery.sso.loader.ResultPublisher")
        self.result_publisher = patcher.start().return_value
        self.addCleanup(patcher.stop)
        self.loader = Loader(
            self.config,
            self.bq_client,
            job_timeout=timedelta(minutes=2),
            retry_timeout=timedelta(minutes=5),
        )

    def _upload(self, file_map: dict[str, list[Path]], object_prefix: str) -> None:
        """Upload SSO files under a prefix, with Pub/Sub mocked."""
        uploader_config = UploaderConfig(
            bucket_name=self.config.bucket_name,
            files={table_name: f"{table_name}.parquet" for table_name in file_map},
            allow_partial_upload=True,
        )
        with (
            mock.patch("lsst.dax.ppdb.bigquery.sso.uploader.pubsub_v1.PublisherClient"),
            mock.patch(
                "lsst.dax.ppdb.bigquery.sso.uploader.Uploader._generate_unique_prefix",
                return_value=object_prefix,
            ),
        ):
            Uploader(uploader_config, file_map).upload()

    def _load(self, object_prefix: str) -> None:
        """Load an upload through the loader's entry point."""
        event = _make_event({"bucket": self.config.bucket_name, "object_prefix": object_prefix})
        self.loader.load_from_event(event, logging.getLogger(__name__))

    def _ids(self, table_name: str, column: str) -> set[int]:
        """Return the values of an internal table's ID column."""
        table_fqn = self.config.fqn_for(DatasetType.INTERNAL, table_name)
        return {row[0] for row in self.bq_client.query(f"SELECT `{column}` FROM `{table_fqn}`").result()}

    def test_load(self) -> None:
        """Test that a load with a bad manifest, a missing manifest or a file
        of the wrong type fails and leaves the tables unchanged, that a file
        with an extra column loads, that a load replaces only the uploaded
        tables with only the listed files, and that loading again gives the
        same result.

        Notes
        -----
        The checks are kept in one test because each test runs the full
        setup: creating the datasets, tables and bucket, and uploading the
        files. Separate tests would make the test too expensive.
        """
        # The IDs each table should hold before and after the loads.
        old_objects = {_OLD_ROWS["SSObject"][0]}
        old_sources = {_OLD_ROWS["SSObservation"][1]}
        extra_column_objects = {21165641428803590, 21165641428803591}
        expected_objects = {21165641428803584, 21165641428803585, 21165641428803586}
        expected_sources = {170055002004447233 + index for index in range(6)}

        # Overwrite the manifest with a wrong SSObservation row count, keeping
        # the original.
        manifest_blob = self.bucket.blob(f"{_UPLOAD_PREFIX}/{Manifest.FILE_NAME}")
        original_manifest = manifest_blob.download_as_text()
        manifest = Manifest.model_validate_json(original_manifest)
        manifest.tables["SSObservation"]["part-00000.parquet"].row_count += 1
        manifest_blob.upload_from_string(manifest.model_dump_json())

        # Check that the load fails its ASSERT, publishes the failure, and
        # leaves both tables with their old rows, including SSObject, whose
        # row count was right.
        self._load(_UPLOAD_PREFIX)
        self.assertIn("does not match the manifest", self.result_publisher.publish_failed.call_args.args[2])
        self.assertEqual(self._ids("SSObject", "ssObjectId"), old_objects)
        self.assertEqual(self._ids("SSObservation", "diaSourceId"), old_sources)

        # Restore the original manifest.
        manifest_blob.upload_from_string(original_manifest)

        # Check that loading a file with ssObjectId of the wrong type fails,
        # naming the column, and leaves SSObject with its old rows.
        self.result_publisher.reset_mock()
        self._load(_WRONG_TYPE_PREFIX)
        self.result_publisher.publish_succeeded.assert_not_called()
        self.assertIn("ssObjectId", self.result_publisher.publish_failed.call_args.args[2])
        self.assertEqual(self._ids("SSObject", "ssObjectId"), old_objects)

        # Check that a file with an extra column loads, the column ignored,
        # and replaces only SSObject.
        self.result_publisher.reset_mock()
        self._load(_EXTRA_COLUMN_PREFIX)
        self.result_publisher.publish_succeeded.assert_called_once()
        self.assertEqual(self._ids("SSObject", "ssObjectId"), extra_column_objects)
        self.assertEqual(self._ids("SSObservation", "diaSourceId"), old_sources)

        # Load the upload, and check that the success was published.
        self.result_publisher.reset_mock()
        self._load(_UPLOAD_PREFIX)
        self.result_publisher.publish_succeeded.assert_called_once()

        # Check that the tables were replaced, without the stray file's row,
        # and that NearbySSO, which isn't in the upload, kept its row.
        self.assertEqual(self._ids("SSObject", "ssObjectId"), expected_objects)
        self.assertEqual(self._ids("SSObservation", "diaSourceId"), expected_sources)
        self.assertEqual(self._ids("NearbySSO", "diaSourceId"), old_sources)

        # Check that the landing tables were dropped.
        landing_dataset = self.config.fqn_for(DatasetType.LANDING)
        self.assertEqual(list(self.bq_client.list_tables(landing_dataset)), [])

        # Load the same upload again, as for a redelivered message, and check
        # that it succeeds with the same rows.
        self.result_publisher.reset_mock()
        self._load(_UPLOAD_PREFIX)
        self.result_publisher.publish_succeeded.assert_called_once()
        self.assertEqual(self._ids("SSObject", "ssObjectId"), expected_objects)
        self.assertEqual(self._ids("SSObservation", "diaSourceId"), expected_sources)

        # Check that a missing manifest fails the load, naming the manifest.
        self.result_publisher.reset_mock()
        self._load("20261118T000000000")
        self.assertIn(Manifest.FILE_NAME, self.result_publisher.publish_failed.call_args.args[2])


if __name__ == "__main__":
    unittest.main()
