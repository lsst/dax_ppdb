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

import gc
import io
import json
import logging
import shutil
import tempfile
import uuid
from typing import Any

import google.auth
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import Request
from google.cloud import storage

from lsst.dax.apdb import (
    ApdbConfig,
)
from lsst.dax.apdb.sql import ApdbSql
from lsst.dax.ppdb import PpdbConfig
from lsst.dax.ppdb.bigquery import PpdbBigQuery
from lsst.dax.ppdb.bigquery.chunk_uploader import ChunkUploader
from lsst.dax.ppdb.tests._ppdb import TEST_SCHEMA_RESOURCE_PATH

try:
    import testing.postgresql
except ImportError:
    testing = None

_LOG = logging.getLogger(__name__)


TEST_CONFIG = {
    "db_drop": True,
    "validate_config": False,
    "delete_existing_dirs": True,
    "bucket_name": "ppdb-test",
    "object_prefix": "data/test",
    "dataset_id": "test_dataset",
    "project_id": "test_project",
}


def json_rows_to_buf(rows: list[dict]) -> io.StringIO:
    """Convert a list of dict rows to a newline-delimited JSON StringIO
    buffer.
    """
    buf = io.StringIO()
    for row in rows:
        buf.write(json.dumps(row) + "\n")
    buf.seek(0)
    return buf


def generate_test_bucket_name(test_prefix: str = "ppdb-test") -> str:
    """Generate a unique bucket name for testing."""
    test_id = uuid.uuid4().hex[:16]
    return f"{test_prefix}-{test_id}"


def delete_test_bucket(bucket_or_bucket_name: str | storage.Bucket) -> None:
    """Delete a cloud storage bucket that was created for testing.

    Parameters
    ----------
    bucket_or_bucket_name
        The name of the bucket or the actual bucket to delete.
    """
    storage_client = storage.Client()
    try:
        if isinstance(bucket_or_bucket_name, str):
            bucket = storage_client.bucket(bucket_or_bucket_name)
        else:
            bucket = bucket_or_bucket_name
        blobs = list(bucket.list_blobs())
        for blob in blobs:
            blob.delete()
        bucket.delete()
    except Exception as e:
        _LOG.exception("Failed to delete test GCS bucket: %s", e)


class ChunkUploaderWithoutPubSub(ChunkUploader):
    """A dummy implementation of the ChunkUploader that does not actually
    post messages to Pub/Sub.
    """

    def _post_to_stage_chunk_topic(self, gcs_uri: str, chunk_id: int) -> None:
        message = {
            "dataset": None,
            "chunk_id": str(chunk_id),
            "folder": f"gs://{gcs_uri}",
        }
        print(f"Dummy publish to Pub/Sub topic: {message}")


class SqliteMixin:
    """Mixin class to provide Sqlite-specific setup/teardown and instance
    creation.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.apdb_url = f"sqlite:///{self.tempdir}/apdb.sqlite3"
        self.ppdb_url = f"sqlite:///{self.tempdir}/ppdb.sqlite3"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def make_instance(self, **kwargs: Any) -> PpdbConfig:
        """Make config class instance used in all tests."""
        kw = {
            **TEST_CONFIG,
            "db_url": self.ppdb_url,
            "felis_path": TEST_SCHEMA_RESOURCE_PATH,
            "replication_dir": self.tempdir,
        }
        bq_config = PpdbBigQuery.init_bigquery(**kw)  # type: ignore[arg-type]
        return bq_config

    def make_apdb_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make APDB instance for tests."""
        kw = {
            "schema_file": TEST_SCHEMA_RESOURCE_PATH,
            "ss_schema_file": "",
            "db_url": self.apdb_url,
            "enable_replica": True,
        }
        kw.update(kwargs)
        return ApdbSql.init_database(**kw)  # type: ignore[arg-type]


class PostgresMixin:
    """Mixin class to provide Postgres-specific setup/teardown and instance
    creation.
    """

    postgresql: Any

    @classmethod
    def setUpClass(cls) -> None:
        # Create the postgres test server.
        cls.postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)

    @classmethod
    def tearDownClass(cls) -> None:
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.postgresql.clear_cache()

    def setUp(self) -> None:
        self.server = self.postgresql()
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        self.server = self.postgresql()
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def make_instance(self, config_dict: dict[str, Any] = TEST_CONFIG, **kwargs: Any) -> PpdbConfig:
        """Make config class instance used in all tests."""
        kw = {
            **config_dict,
            "db_url": self.server.url(),
            "db_schema": "ppdb_test",
            "felis_path": TEST_SCHEMA_RESOURCE_PATH,
            "replication_dir": self.tempdir,
        }
        bq_config = PpdbBigQuery.init_bigquery(**kw)
        return bq_config

    def make_apdb_instance(self, **kwargs: Any) -> ApdbConfig:
        kw = {
            "schema_file": TEST_SCHEMA_RESOURCE_PATH,
            "ss_schema_file": "",
            "db_url": self.server.url(),
            "namespace": "apdb",
            "enable_replica": True,
        }
        kw.update(kwargs)
        return ApdbSql.init_database(**kw)


def have_valid_google_credentials() -> bool:
    """Check that valid Google credentials are available for testing.

    Returns
    -------
    `bool`
        True if valid Google credentials are available, False if not.

    Raises
    ------
    google.auth.exceptions.RefreshError
        Raised if the credentials cannot be refreshed.
    Exception
        Raised for other transport or configuration failures.
    """
    try:
        credentials, _ = google.auth.default()
    except DefaultCredentialsError:
        return False

    # This will validate the default credentials that were found in the
    # environment.
    credentials.refresh(Request())

    return True
