"""Miscellaneous utilities for BigQuery integration tests."""
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
import os
import shutil
import tempfile
import uuid
from typing import Any

import google.auth
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import Request
from google.cloud import (
    bigquery,
    storage,
)

from lsst.dax.apdb import (
    ApdbConfig,
)
from lsst.dax.apdb.sql import ApdbSql
from lsst.dax.ppdb import PpdbConfig
from lsst.dax.ppdb.bigquery import DatasetType, PpdbBigQuery, PpdbBigQueryConfig
from lsst.dax.ppdb.sql import PpdbSqlBaseConfig
from lsst.dax.ppdb.tests._ppdb import TEST_SCHEMA_RESOURCE_PATH

try:
    import testing.postgresql
except ImportError:
    testing = None

__all__ = [
    "PostgresMixin",
    "SqliteMixin",
    "create_bucket",
    "create_datasets",
    "delete_bucket",
    "drop_datasets",
    "have_valid_google_credentials",
    "init_bigquery_sql",
    "json_rows_to_buf",
    "make_bigquery_config",
    "search_indexes_enabled",
]

_LOG = logging.getLogger(__name__)


_APDB_SCHEMA_RESOURCE_PATH = "resource://lsst.dax.ppdb/resources/config/schemas/test_apdb_schema.yaml"


def make_bigquery_config(
    test_name: str = "test",
    db_url: str = "sqlite:///:memory:",
    schema_name: str | None = None,
    replication_dir: str | None = None,
    delete_existing_dirs: bool = True,
) -> PpdbBigQueryConfig:
    """Make a complete PPDB BigQuery configuration for testing.

    Parameters
    ----------
    test_name
        The name of the test, used to create unique bucket and dataset names.
    db_url
        Optional database URL to use for the test. If not provided, a default
        SQLite in-memory config will be used.
    """
    unique_id = uuid.uuid4().hex[:16]

    credentials, project_id = google.auth.default()

    if replication_dir is None:
        replication_dir = tempfile.mkdtemp()

    config = PpdbBigQueryConfig(
        project_id=project_id,
        bucket_name=f"{test_name.lower().replace('_', '-')}-bucket-{unique_id}",
        object_prefix="data/test",
        replication_dir=replication_dir,
        delete_existing_dirs=delete_existing_dirs,
        datasets={
            "staging": f"{test_name}_staging_{unique_id}",
            "internal": f"{test_name}_internal_{unique_id}",
            "public": f"{test_name}_public_{unique_id}",
        },
        sql=PpdbSqlBaseConfig(
            db_url=db_url,
            schema_name=schema_name,
            felis_path=_APDB_SCHEMA_RESOURCE_PATH,
        ),
    )
    return config


def init_bigquery_sql(config: PpdbBigQueryConfig, db_drop: bool = True) -> None:
    """Initialize the SQL database for the PPDB BigQuery test instance.

    Parameters
    ----------
    config
        Configuration object with BigQuery and SQL database parameters.
    db_drop
        If True then drop existing db tables.
    """
    sa_metadata, schema_version = PpdbBigQuery.read_schema(
        config.sql.felis_path, config.sql.schema_name, config.sql.felis_schema, config.sql.db_url
    )
    password_provider = PpdbBigQuery._make_password_provider(config.project_id)
    engine = PpdbBigQuery.make_engine(config.sql, password_provider=password_provider)
    PpdbBigQuery.make_database(engine, config.sql, sa_metadata, schema_version, db_drop)


def create_datasets(config: PpdbBigQueryConfig, dataset_types: list[DatasetType] | None = None) -> None:
    """Create the BigQuery datasets for testing.

    This will not create any tables, just the empty datasets.

    Parameters
    ----------
    config
        Configuration object with BigQuery and SQL database parameters.
    dataset_types
        List of dataset types to create. If None, all dataset types will be
        created.
    """
    if dataset_types is None:
        dataset_types = [DatasetType.STAGING, DatasetType.INTERNAL, DatasetType.PUBLIC]
    bq_client = bigquery.Client(project=config.project_id)
    for dataset_type in dataset_types:
        dataset_fqn = config.fqn_for(dataset_type)
        try:
            bq_client.create_dataset(bigquery.Dataset(dataset_fqn))
        except Exception as e:
            _LOG.exception("Failed to create dataset %s: %s", dataset_fqn, e)
            raise


def drop_datasets(config: PpdbBigQueryConfig, dataset_types: list[DatasetType] | None = None) -> None:
    """Delete the BigQuery datasets for testing.

    Parameters
    ----------
    config
        Configuration object with BigQuery and SQL database parameters.
    dataset_types
        List of dataset types to delete. If None, all dataset types will be
        deleted.
    """
    if dataset_types is None:
        dataset_types = [DatasetType.STAGING, DatasetType.INTERNAL, DatasetType.PUBLIC]
    bq_client = bigquery.Client(project=config.project_id)
    for dataset_type in dataset_types:
        dataset_fqn = config.fqn_for(dataset_type)
        try:
            bq_client.delete_dataset(dataset_fqn, delete_contents=True, not_found_ok=True)
        except Exception as e:
            _LOG.exception("Failed to delete dataset %s: %s", dataset_fqn, e)
            raise


def create_bucket(config: PpdbBigQueryConfig) -> None:
    """Create the cloud storage bucket for testing.

    Parameters
    ----------
    config
        Configuration object with BigQuery and SQL database parameters.
    """
    storage_client = storage.Client(project=config.project_id)
    try:
        storage_client.create_bucket(config.bucket_name)
    except Exception as e:
        _LOG.exception("Failed to create bucket %s: %s", config.bucket_name, e)
        raise


def json_rows_to_buf(rows: list[dict]) -> io.StringIO:
    """Convert a list of dict rows to a newline-delimited JSON StringIO
    buffer.
    """
    buf = io.StringIO()
    for row in rows:
        buf.write(json.dumps(row) + "\n")
    buf.seek(0)
    return buf


def delete_bucket(bucket_target: str | storage.Bucket | PpdbBigQueryConfig) -> None:
    """Delete a cloud storage bucket that was created for testing.

    Parameters
    ----------
    bucket_target
        The name of the bucket, the actual bucket, or a PpdbBigQueryConfig
        instance with the name to delete.
    """
    storage_client = storage.Client()
    try:
        if isinstance(bucket_target, str):
            bucket = storage_client.bucket(bucket_target)
        elif isinstance(bucket_target, PpdbBigQueryConfig):
            bucket = storage_client.bucket(bucket_target.bucket_name)
        else:
            bucket = bucket_target
        blobs = list(bucket.list_blobs())
        for blob in blobs:
            blob.delete()
        bucket.delete()
    except Exception as e:
        _LOG.exception("Failed to delete test GCS bucket: %s", e)
        raise


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

    def make_instance(self, test_name: str = "test") -> PpdbConfig:
        """Make config class instance used in all tests."""
        config = make_bigquery_config(db_url=self.ppdb_url, replication_dir=self.tempdir, test_name=test_name)
        init_bigquery_sql(config)
        return config

    def make_apdb_instance(self) -> ApdbConfig:
        """Make APDB instance for tests."""
        return ApdbSql.init_database(
            schema_file=_APDB_SCHEMA_RESOURCE_PATH,
            ss_schema_file="",
            db_url=self.apdb_url,
            enable_replica=True,
        )


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

    def make_instance(self, test_name: str = "test") -> PpdbConfig:
        """Make config class instance used in all tests."""
        config = make_bigquery_config(
            test_name=test_name,
            db_url=self.server.url(),
            replication_dir=self.tempdir,
            schema_name="ppdb_test",
        )
        init_bigquery_sql(config)
        return config

    def make_apdb_instance(self) -> ApdbConfig:
        return ApdbSql.init_database(
            schema_file=TEST_SCHEMA_RESOURCE_PATH,
            ss_schema_file="",
            db_url=self.server.url(),
            namespace="apdb",
            enable_replica=True,
        )


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


def search_indexes_enabled() -> bool:
    """Check if search index creation is enabled for tests."""
    return os.getenv(
        "DAX_PPDB_TESTS_SEARCH_INDEXES_ENABLED",
        "",
    ).strip().lower() in {"1", "true", "yes", "on"}
