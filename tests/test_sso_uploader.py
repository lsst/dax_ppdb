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
import os
import posixpath
import re
import tempfile
import unittest
from collections.abc import Iterable
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow
import yaml
from google.api_core.exceptions import DeadlineExceeded, GoogleAPIError
from pyarrow import parquet

from lsst.dax.ppdb.bigquery.schema.constants import SSO_TABLES
from lsst.dax.ppdb.bigquery.sso.manifest import Manifest
from lsst.dax.ppdb.bigquery.sso.uploader import (
    Uploader,
    UploaderConfig,
    UploadError,
)
from lsst.dax.ppdb.cli import ppdb_cli
from lsst.dax.ppdb.tests import (
    create_bucket,
    delete_bucket,
    have_valid_google_credentials,
    make_bigquery_config,
)

_TEST_BUCKET_NAME = "sso-upload-bucket"
_TEST_PROJECT_ID = "ppdb-test-project"
_TEST_UNIQUE_PREFIX = "20260902T000000000"


def _write_parquet(path: Path, row_count: int = 3) -> None:
    """Write a small parquet file with an ``ssObjectId`` column."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pyarrow.table({"ssObjectId": list(range(21165641428803584, 21165641428803584 + row_count))})
    parquet.write_table(table, path)


def _make_file_map(directory: Path, table_names: Iterable[str]) -> dict[str, list[Path]]:
    """Make a file_map for Uploader with one parquet file per table,
    named ``<table>.parquet``.
    """
    file_map = {}
    for table_name in table_names:
        path = directory / f"{table_name}.parquet"
        _write_parquet(path)
        file_map[table_name] = [path]
    return file_map


def _make_config(bucket_name: str, table_names: Iterable[str] = SSO_TABLES) -> UploaderConfig:
    """Make an `UploaderConfig` listing one ``<table>.parquet`` file per
    table.
    """
    return UploaderConfig(
        bucket_name=bucket_name,
        files={table_name: f"{table_name}.parquet" for table_name in table_names},
    )


def _part_name(table_name: str, index: int = 0) -> str:
    """Return the object name of a table's uploaded parquet file."""
    return posixpath.join(_TEST_UNIQUE_PREFIX, table_name, f"part-{index:05d}.parquet")


def _received_result(object_prefix: str, status: str) -> Mock:
    """Return a pulled Pub/Sub message holding a load result.

    Parameters
    ----------
    object_prefix
        Prefix of the result's upload.
    status
        ``succeeded`` or ``failed``.

    Returns
    -------
    `unittest.mock.Mock`
        The received message, with its ack ID and data.
    """
    result = {"object_prefix": object_prefix, "status": status}
    if status == "succeeded":
        result["job_id"] = f"load_sso_replace_{object_prefix}"
    else:
        result["error"] = "row count in internal table SSObject does not match the manifest"
    received = Mock()
    received.ack_id = f"ack-{object_prefix}"
    received.message.data = json.dumps(result).encode("utf-8")
    return received


class UploaderValidationTestCase(unittest.TestCase):
    """Test validation of the file_map passed to the Uploader
    constructor.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.directory = Path(self.tempdir.name)
        self.config = _make_config(_TEST_BUCKET_NAME)

        # Patch the project lookup, which needs Google Cloud credentials.
        patcher = patch("lsst.dax.ppdb.bigquery.sso.uploader.get_project_id", return_value=_TEST_PROJECT_ID)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_invalid_file_map_rejected(self) -> None:
        """Test that a file_map is rejected, with an error saying why, when it
        has no tables, or a table is unknown, has a missing file, a directory
        or no files, shares a file with another table, or is missing.
        """
        # Set up a valid file_map, plus a file for a table that isn't an SSO
        # table.
        valid = _make_file_map(self.directory, SSO_TABLES)
        dia_object_path = self.directory / "DiaObject.parquet"
        _write_parquet(dia_object_path)

        # Make an invalid file_map for each case, with the error it should
        # give.
        cases = {
            "no tables": ({}, "no SSO tables to upload"),
            "unknown table": ({**valid, "DiaObject": [dia_object_path]}, "Invalid SSO table name: DiaObject"),
            "missing file": (
                {**valid, "SSObservation": [self.directory / "SSObservation_missing.parquet"]},
                "Parquet file does not exist",
            ),
            "directory": (
                {**valid, "SSObservation": [self.directory]},
                "Parquet path for SSO table SSObservation is a directory",
            ),
            "no files": (
                {**valid, "SSObservation": []},
                "No parquet files found for SSO table: SSObservation",
            ),
            "file in two tables": (
                {**valid, "SSObject": valid["SSObservation"]},
                "Duplicate parquet file paths",
            ),
            "missing table": (
                {name: paths for name, paths in valid.items() if name != "mpc_orbits"},
                "Missing parquet files for SSO tables",
            ),
        }

        # Check that the uploader rejects each one with its error.
        for case, (file_map, message) in cases.items():
            with self.subTest(case), self.assertRaisesRegex(UploadError, message):
                Uploader(self.config, file_map)

    def test_missing_tables_allowed_when_partial_upload_allowed(self) -> None:
        """Test that a file_map missing some SSO tables is accepted when
        allow_partial_upload is True.
        """
        # Leave out the last table, and allow partial uploads.
        file_map = _make_file_map(self.directory, SSO_TABLES[:-1])
        config = self.config.model_copy(update={"allow_partial_upload": True})

        # Check that the uploader keeps the file_map as given.
        uploader = Uploader(config, file_map)
        self.assertEqual(uploader._file_map, file_map)

    def test_upload_refused_for_bad_parquet_file(self) -> None:
        """Test that the upload fails before writing anything when one of
        its files isn't a valid parquet file, even if it's the last one.
        """
        # Replace the last table's file with one that isn't parquet.
        file_map = _make_file_map(self.directory, SSO_TABLES)
        last_table = list(file_map)[-1]
        file_map[last_table][0].write_text("not a parquet file")

        # Check that the upload fails, naming the table.
        with self.assertRaisesRegex(UploadError, f"Invalid parquet file for SSO table {last_table}"):
            Uploader(self.config, file_map).upload()

    def test_upload_refused_for_empty_parquet_file(self) -> None:
        """Test that the upload fails before writing anything when one of
        its files has no rows, naming the file.
        """
        # Replace SSObject's file with an empty one.
        file_map = _make_file_map(self.directory, SSO_TABLES)
        empty_path = file_map["SSObject"][0]
        _write_parquet(empty_path, row_count=0)

        # Check that the upload fails, naming the empty file.
        with self.assertRaisesRegex(UploadError, re.escape(f"{empty_path}: the file has no rows")):
            Uploader(self.config, file_map).upload()


class UploaderFromDirectoryTestCase(unittest.TestCase):
    """Test that Uploader.from_directory() finds each table's files from
    the configured paths and patterns.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.directory = Path(self.tempdir.name)

        # Patch the project lookup, which needs Google Cloud credentials.
        patcher = patch("lsst.dax.ppdb.bigquery.sso.uploader.get_project_id", return_value=_TEST_PROJECT_ID)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_files_and_patterns(self) -> None:
        """Test that single files, an absolute path, a path under ``~`` and a
        pattern are found, the pattern's files sorted by path, and unrelated
        files ignored.
        """
        # Write one file per table, with SSObservation instead as three files
        # created out of order, plus unrelated files.
        file_map = _make_file_map(self.directory, SSO_TABLES)
        ssobservation_paths = [
            self.directory / "SSObservation" / f"part-{index}.parquet" for index in (2, 0, 1)
        ]
        for path in ssobservation_paths:
            _write_parquet(path)
        _write_parquet(self.directory / "DiaObject.parquet")
        (self.directory / "README.txt").touch()

        # Write mpc_orbits outside the directory, to list by absolute path.
        other_dir = tempfile.TemporaryDirectory()
        self.addCleanup(other_dir.cleanup)
        mpc_orbits_path = Path(other_dir.name) / "mpc_orbits.parquet"
        _write_parquet(mpc_orbits_path)

        # Write current_identifications in a separate home directory, to list
        # under ~.
        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        current_identifications_path = Path(home.name) / "current_identifications.parquet"
        _write_parquet(current_identifications_path)

        # Configure the SSObservation pattern, the absolute mpc_orbits path and
        # the current_identifications path under ~.
        config = _make_config(_TEST_BUCKET_NAME)
        config.files["SSObservation"] = "SSObservation/*.parquet"
        config.files["mpc_orbits"] = str(mpc_orbits_path)
        config.files["current_identifications"] = "~/current_identifications.parquet"

        # Build the uploader from the directory, with HOME set to the home
        # directory.
        with patch.dict(os.environ, {"HOME": home.name}):
            uploader = Uploader.from_directory(self.directory, config)

        # Check that each table has its configured files, SSObservation's in
        # order.
        file_map["SSObservation"] = sorted(ssobservation_paths)
        file_map["mpc_orbits"] = [mpc_orbits_path]
        file_map["current_identifications"] = [current_identifications_path]
        self.assertEqual(uploader._file_map, file_map)

    def test_rejects_non_directory_path(self) -> None:
        """Test that a non-directory path is rejected."""
        # Make a path to a file instead of a directory.
        not_a_directory = self.directory / "SSObservation.parquet"
        not_a_directory.touch()

        # Check that building the uploader fails.
        with self.assertRaises(UploadError):
            Uploader.from_directory(not_a_directory, _make_config(_TEST_BUCKET_NAME))


class UploaderWaitTestCase(unittest.TestCase):
    """Test Uploader.wait_for_load, with the Google Cloud Storage and Pub/Sub
    clients mocked.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)

        # Patch the clients so the upload goes to a fake, empty bucket, and
        # patch the project lookup and the upload prefix.
        patchers = [
            patch("lsst.dax.ppdb.bigquery.sso.uploader.Client"),
            patch("lsst.dax.ppdb.bigquery.sso.uploader.pubsub_v1.PublisherClient"),
            patch("lsst.dax.ppdb.bigquery.sso.uploader.get_project_id", return_value=_TEST_PROJECT_ID),
            patch(
                "lsst.dax.ppdb.bigquery.sso.uploader.Uploader._generate_unique_prefix",
                return_value=_TEST_UNIQUE_PREFIX,
            ),
        ]
        started = []
        for patcher in patchers:
            started.append(patcher.start())
            self.addCleanup(patcher.stop)
        mock_client_cls = started[0]
        mock_client_cls.return_value.bucket.return_value.list_blobs.return_value = []

        # Set up an uploader with one file per table.
        file_map = _make_file_map(Path(self.tempdir.name), SSO_TABLES)
        self.uploader = Uploader(_make_config(_TEST_BUCKET_NAME), file_map)

    def test_wait_for_load(self) -> None:
        """Test that waiting for the load skips results for other uploads
        and returns the transaction's job ID from this upload's result.
        """
        # Upload the files.
        self.uploader.upload()

        # Make the subscription return an old result for another upload,
        # then this upload's result.
        with patch("lsst.dax.ppdb.bigquery.sso.uploader.pubsub_v1.SubscriberClient") as mock_subscriber_cls:
            subscriber = mock_subscriber_cls.return_value
            subscriber.pull.side_effect = [
                Mock(received_messages=[_received_result("20260901T000000000", "succeeded")]),
                Mock(received_messages=[_received_result(_TEST_UNIQUE_PREFIX, "succeeded")]),
            ]

            # Check that the wait returns this upload's job ID, after
            # acknowledging both results.
            job_id = self.uploader.wait_for_load()
        self.assertEqual(job_id, f"load_sso_replace_{_TEST_UNIQUE_PREFIX}")
        self.assertEqual(subscriber.acknowledge.call_count, 2)

    def test_wait_for_failed_load(self) -> None:
        """Test that waiting for a load that failed raises with its
        error.
        """
        # Upload the files.
        self.uploader.upload()

        # Make the subscription return a failed result for this upload.
        with patch("lsst.dax.ppdb.bigquery.sso.uploader.pubsub_v1.SubscriberClient") as mock_subscriber_cls:
            mock_subscriber_cls.return_value.pull.return_value = Mock(
                received_messages=[_received_result(_TEST_UNIQUE_PREFIX, "failed")]
            )

            # Check that the wait raises with the load's error.
            with self.assertRaisesRegex(UploadError, "failed: row count in internal table SSObject"):
                self.uploader.wait_for_load()

    def test_wait_for_load_times_out(self) -> None:
        """Test that waiting raises when no result arrives in time."""
        # Upload the files.
        self.uploader.upload()

        # Make every pull return nothing before its deadline.
        with patch("lsst.dax.ppdb.bigquery.sso.uploader.pubsub_v1.SubscriberClient") as mock_subscriber_cls:
            mock_subscriber_cls.return_value.pull.side_effect = DeadlineExceeded("Deadline exceeded")

            # Check that the wait gives up with an error.
            with self.assertRaisesRegex(UploadError, "No load result"):
                self.uploader.wait_for_load(timedelta(milliseconds=100))

    def test_wait_for_load_read_error(self) -> None:
        """Test that waiting raises when the load results can't be read."""
        # Upload the files.
        self.uploader.upload()

        # Make pulling from the subscription fail, e.g. without permission.
        with patch("lsst.dax.ppdb.bigquery.sso.uploader.pubsub_v1.SubscriberClient") as mock_subscriber_cls:
            mock_subscriber_cls.return_value.pull.side_effect = GoogleAPIError(
                "403 User not authorized to perform this action"
            )

            # Check that the wait raises.
            with self.assertRaisesRegex(UploadError, "Failed to read the load results"):
                self.uploader.wait_for_load()

    def test_wait_without_upload(self) -> None:
        """Test that waiting before anything is uploaded raises."""
        # Check that the wait is refused.
        with self.assertRaisesRegex(UploadError, r"Call upload\(\) first"):
            self.uploader.wait_for_load()


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class UploadSSODataCLITestCase(unittest.TestCase):
    """Integration test for the ``ppdb-cli upload-sso-data`` command,
    against Google Cloud Storage.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)

        # Set up a bucket for the upload.
        self.config = make_bigquery_config(test_name="test_cli_upload_sso_data")
        self.bucket = create_bucket(self.config)
        self.addCleanup(delete_bucket, self.bucket)

        # Set up one file per table, with SSObservation split into two files,
        # and a config listing them.
        directory = Path(self.tempdir.name)
        self.file_map = _make_file_map(
            directory, [table_name for table_name in SSO_TABLES if table_name != "SSObservation"]
        )
        self.file_map["SSObservation"] = [
            directory / "SSObservation" / f"part-{index}.parquet" for index in (0, 1)
        ]
        _write_parquet(self.file_map["SSObservation"][0], row_count=4)
        _write_parquet(self.file_map["SSObservation"][1], row_count=2)
        uploader_config = _make_config(self.config.bucket_name)
        uploader_config.files["SSObservation"] = "SSObservation/*.parquet"
        self.config_path = os.path.join(self.tempdir.name, "sso_uploader_config.yaml")
        with open(self.config_path, "w") as config_file:
            yaml.dump(uploader_config.model_dump(), config_file)

    def test_upload_sso_data(self) -> None:
        """Test that ``ppdb-cli upload-sso-data`` uploads each table's files as
        numbered parts, then a manifest that can be read back from the bucket,
        and publishes a message with the bucket and prefix.

        The Pub/Sub publisher is mocked to avoid depending on a real topic.
        """
        # Run the command.
        argv = ["upload-sso-data", self.config_path, self.tempdir.name, "--no-wait"]
        with (
            patch("lsst.dax.ppdb.bigquery.sso.uploader.pubsub_v1.PublisherClient") as mock_publisher_cls,
            patch(
                "lsst.dax.ppdb.bigquery.sso.uploader.Uploader._generate_unique_prefix",
                return_value=_TEST_UNIQUE_PREFIX,
            ),
        ):
            ppdb_cli.main(argv)

        # Check that each table's parts are in the bucket, SSObservation's as
        # part-00000 and part-00001.
        for table_name, paths in self.file_map.items():
            for index in range(len(paths)):
                self.assertTrue(self.bucket.blob(_part_name(table_name, index)).exists())

        # Check that the manifest reads back from the bucket and lists each
        # part with its source path and row count.
        manifest = Manifest.from_uri(f"gs://{self.config.bucket_name}/{_TEST_UNIQUE_PREFIX}")
        self.assertEqual(set(manifest.tables), set(SSO_TABLES))
        for table_name, paths in self.file_map.items():
            entries = list(manifest.tables[table_name].values())
            self.assertEqual([entry.source_path for entry in entries], [str(path) for path in paths])
        ssobservation = manifest.tables["SSObservation"]
        self.assertEqual([entry.row_count for entry in ssobservation.values()], [4, 2])

        # Check that the message holds only the bucket and the prefix.
        _, published_bytes = mock_publisher_cls.return_value.publish.call_args[0]
        message = json.loads(published_bytes.decode("utf-8"))
        self.assertEqual(message, {"bucket": self.config.bucket_name, "object_prefix": _TEST_UNIQUE_PREFIX})

    def test_upload_refused_when_prefix_holds_objects(self) -> None:
        """Test that ``ppdb-cli upload-sso-data`` fails without writing
        anything when the upload prefix already holds an object in the
        bucket.

        The Pub/Sub publisher is mocked to avoid depending on a real topic.
        """
        # Put an object from another upload under the prefix.
        existing_name = _part_name("SSObject")
        self.bucket.blob(existing_name).upload_from_string(b"other upload")

        # Run the command with the same prefix, and check that it fails.
        argv = ["upload-sso-data", self.config_path, self.tempdir.name, "--no-wait"]
        with (
            patch("lsst.dax.ppdb.bigquery.sso.uploader.pubsub_v1.PublisherClient") as mock_publisher_cls,
            patch(
                "lsst.dax.ppdb.bigquery.sso.uploader.Uploader._generate_unique_prefix",
                return_value=_TEST_UNIQUE_PREFIX,
            ),
            self.assertRaisesRegex(UploadError, "already holds objects"),
        ):
            ppdb_cli.main(argv)

        # Check that only the other upload's object is under the prefix, and
        # that nothing was published.
        names = [blob.name for blob in self.bucket.list_blobs(prefix=f"{_TEST_UNIQUE_PREFIX}/")]
        self.assertEqual(names, [existing_name])
        mock_publisher_cls.return_value.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
