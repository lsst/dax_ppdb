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

import os
import unittest
import uuid
from unittest import mock

import astropy.time
import sqlalchemy

from lsst.dax.ppdb import Ppdb
from lsst.dax.ppdb.bigquery import ChunkStatus, PpdbBigQuery, PpdbReplicaChunkExtended
from lsst.dax.ppdb.bigquery.ppdb_bigquery import UpdatableField
from lsst.dax.ppdb.sql import PasswordProvider, PpdbSqlBaseConfig
from lsst.dax.ppdb.tests import PpdbTest
from lsst.dax.ppdb.tests._bigquery import PostgresMixin, SqliteMixin

try:
    import testing.postgresql
except ImportError:
    testing = None


class SqliteTestCase(SqliteMixin, PpdbTest, unittest.TestCase):
    """A test case for the PpdbBigQuery class using a SQLite backend."""


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class PostgresTestCase(PostgresMixin, PpdbTest, unittest.TestCase):
    """A test case for the PpdbBigQuery class using a Postgres backend."""


def _make_chunk(
    chunk_id: int,
    status: ChunkStatus,
) -> PpdbReplicaChunkExtended:
    """Create a test chunk with the given ID and status.

    Parameters
    ----------
    chunk_id
        The ID of the chunk to create.
    status
        The status to assign to the chunk.

    Returns
    -------
    `PpdbReplicaChunkExtended`
        A test chunk with the specified ID and status.
    """
    return PpdbReplicaChunkExtended(
        id=chunk_id,
        unique_id=uuid.uuid4(),
        last_update_time=astropy.time.Time("2021-01-01T00:01:00", format="isot", scale="tai"),
        replica_time=astropy.time.Time.now(),
        status=status,
    )


class PpdbBigQueryTestCase(SqliteMixin, unittest.TestCase):
    """Tests for replica chunk database operations."""

    def _make_ppdb(self) -> PpdbBigQuery:
        config = self.make_instance()
        ppdb = Ppdb.from_config(config)
        assert isinstance(ppdb, PpdbBigQuery)
        return ppdb

    def test_insert_and_query_chunks(self) -> None:
        """Test that inserted chunks can be retrieved via query_chunks."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.EXPORTED),
                _make_chunk(2, ChunkStatus.STAGED),
            ]
        )

        result = ppdb.query_chunks()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].id, 1)
        self.assertEqual(result[0].status, ChunkStatus.EXPORTED)
        self.assertEqual(result[1].id, 2)
        self.assertEqual(result[1].status, ChunkStatus.STAGED)

    def test_insert_chunks_empty_raises(self) -> None:
        """Test that inserting an empty list raises ValueError."""
        ppdb = self._make_ppdb()
        with self.assertRaises(ValueError):
            ppdb.insert_chunks([])

    def test_update_chunks(self) -> None:
        """Test that update_chunks updates an existing chunk."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks([_make_chunk(1, ChunkStatus.EXPORTED)])
        ppdb.update_chunks([_make_chunk(1, ChunkStatus.STAGED)], fields={UpdatableField.STATUS})

        result = ppdb.query_chunks()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, ChunkStatus.STAGED)

    def test_update_chunks_gcs_uri(self) -> None:
        """Test that update_chunks can update gcs_uri."""
        ppdb = self._make_ppdb()
        chunk = _make_chunk(1, ChunkStatus.EXPORTED)
        ppdb.insert_chunks([chunk])
        updated = chunk.with_new_gcs_uri("gs://bucket/path")
        ppdb.update_chunks([updated], fields={UpdatableField.GCS_URI})

        result = ppdb.query_chunks()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].gcs_uri, "gs://bucket/path")
        self.assertEqual(result[0].status, ChunkStatus.EXPORTED)

    def test_update_chunks_restricts_fields(self) -> None:
        """Test that update_chunks only updates the specified fields."""
        ppdb = self._make_ppdb()
        chunk = _make_chunk(1, ChunkStatus.EXPORTED)
        ppdb.insert_chunks([chunk])

        updated = chunk.with_new_status(ChunkStatus.STAGED).with_new_gcs_uri("gs://bucket/path")
        ppdb.update_chunks([updated], fields={UpdatableField.STATUS})

        result = ppdb.query_chunks()
        self.assertEqual(result[0].status, ChunkStatus.STAGED)
        self.assertIsNone(result[0].gcs_uri)

    def test_update_chunks_empty_raises(self) -> None:
        """Test that update_chunks raises on empty chunks list."""
        ppdb = self._make_ppdb()
        with self.assertRaises(ValueError):
            ppdb.update_chunks([], fields={UpdatableField.STATUS})

    def test_update_chunks_empty_fields_raises(self) -> None:
        """Test that update_chunks raises on empty fields set."""
        ppdb = self._make_ppdb()
        with self.assertRaises(ValueError):
            ppdb.update_chunks([_make_chunk(1, ChunkStatus.STAGED)], fields=set())

    def test_insert_chunks_duplicate_raises(self) -> None:
        """Test that insert_chunks raises on duplicate chunk ID."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks([_make_chunk(1, ChunkStatus.EXPORTED)])
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            ppdb.insert_chunks([_make_chunk(1, ChunkStatus.STAGED)])

    def test_update_chunks_missing_raises(self) -> None:
        """Test that update_chunks raises on a non-existent chunk ID."""
        ppdb = self._make_ppdb()
        with self.assertRaises(LookupError):
            ppdb.update_chunks([_make_chunk(99, ChunkStatus.STAGED)], fields={UpdatableField.STATUS})

    def test_query_chunks_with_filter(self) -> None:
        """Test query_chunks with a WHERE clause."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.EXPORTED),
                _make_chunk(2, ChunkStatus.STAGED),
                _make_chunk(3, ChunkStatus.PROMOTED),
            ]
        )

        result = ppdb.query_chunks(ppdb.chunk_table.columns["status"] == ChunkStatus.STAGED)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, 2)

    def test_query_chunks_with_order_by(self) -> None:
        """Test query_chunks with a custom order_by."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(10, ChunkStatus.STAGED),
                _make_chunk(5, ChunkStatus.STAGED),
                _make_chunk(20, ChunkStatus.STAGED),
            ]
        )

        result = ppdb.query_chunks(order_by=ppdb.chunk_table.columns["apdb_replica_chunk"])
        self.assertEqual([c.id for c in result], [5, 10, 20])

    def test_get_replica_chunks(self) -> None:
        """Test get_replica_chunks returns all chunks."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.EXPORTED),
                _make_chunk(2, ChunkStatus.STAGED),
            ]
        )

        result = ppdb.get_replica_chunks()
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)

    def test_get_replica_chunks_with_start_id(self) -> None:
        """Test get_replica_chunks filtered by start_chunk_id."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.EXPORTED),
                _make_chunk(2, ChunkStatus.STAGED),
                _make_chunk(3, ChunkStatus.PROMOTED),
            ]
        )

        result = ppdb.get_replica_chunks(start_chunk_id=2)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].id, 2)
        self.assertEqual(result[1].id, 3)

    def test_get_promotable_chunks_all_staged(self) -> None:
        """Test that all staged chunks are promotable when none are
        interrupted.
        """
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.STAGED),
                _make_chunk(2, ChunkStatus.STAGED),
                _make_chunk(3, ChunkStatus.STAGED),
            ]
        )

        result = ppdb.get_promotable_chunks()
        self.assertEqual(len(result), 3)
        self.assertEqual([c.id for c in result], [1, 2, 3])

    def test_get_promotable_chunks_after_promoted(self) -> None:
        """Test that staged chunks after promoted ones are promotable."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.PROMOTED),
                _make_chunk(2, ChunkStatus.STAGED),
                _make_chunk(3, ChunkStatus.STAGED),
            ]
        )

        result = ppdb.get_promotable_chunks()
        self.assertEqual(len(result), 2)
        self.assertEqual([c.id for c in result], [2, 3])

    def test_get_promotable_chunks_interrupted_by_non_staged(self) -> None:
        """Test that only the contiguous staged sequence before a non-staged
        chunk is returned.
        """
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.PROMOTED),
                _make_chunk(2, ChunkStatus.STAGED),
                _make_chunk(3, ChunkStatus.EXPORTED),
                _make_chunk(4, ChunkStatus.STAGED),
            ]
        )

        result = ppdb.get_promotable_chunks()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, 2)

    def test_get_promotable_chunks_none_staged(self) -> None:
        """Test that no promotable chunks are returned when none are staged."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.PROMOTED),
                _make_chunk(2, ChunkStatus.EXPORTED),
                _make_chunk(3, ChunkStatus.UPLOADED),
            ]
        )

        result = ppdb.get_promotable_chunks()
        self.assertEqual(len(result), 0)

    def test_get_promotable_chunks_empty_db(self) -> None:
        """Test that an empty database returns no promotable chunks."""
        ppdb = self._make_ppdb()
        result = ppdb.get_promotable_chunks()
        self.assertEqual(len(result), 0)

    def test_get_promotable_chunks_skipped_ignored(self) -> None:
        """Test that skipped chunks are treated the same as promoted."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.SKIPPED),
                _make_chunk(2, ChunkStatus.PROMOTED),
                _make_chunk(3, ChunkStatus.STAGED),
            ]
        )

        result = ppdb.get_promotable_chunks()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, 3)

    def test_get_promotable_chunks_starts_with_non_staged(self) -> None:
        """Test that an exported chunk at the start blocks all staged ones
        after it.
        """
        ppdb = self._make_ppdb()
        ppdb.insert_chunks(
            [
                _make_chunk(1, ChunkStatus.EXPORTED),
                _make_chunk(2, ChunkStatus.STAGED),
                _make_chunk(3, ChunkStatus.STAGED),
            ]
        )

        result = ppdb.get_promotable_chunks()
        self.assertEqual(len(result), 0)


class _StubPasswordProvider(PasswordProvider):
    """Password provider returning a fixed password for tests.

    Parameters
    ----------
    password
        Password to return from `get_password`.
    """

    def __init__(self, password: str = "secret") -> None:
        self._password = password

    def get_password(self) -> str:
        return self._password


class MakeEngineConnectorTestCase(unittest.TestCase):
    """Tests for Cloud SQL Connector engine creation in `PpdbBigQuery`."""

    # Minimal environment required by the Cloud SQL Connector engine.
    CONNECTOR_ENV = {
        "CLOUDSQL_ENABLED": "true",
        "CLOUDSQL_INSTANCE_CONNECTION_NAME": "my-project:us-central1:my-instance",
        "CLOUDSQL_USER": "db-user",
        "CLOUDSQL_DB_NAME": "db-name",
    }

    def setUp(self) -> None:
        self.config = PpdbSqlBaseConfig(db_url="sqlite:///:memory:")
        # Patch the module-level Connector so the tests do not depend on a real
        # GCP connection being available.
        self.connector = mock.MagicMock(name="connector_instance")
        self.connector_cls = mock.MagicMock(name="Connector", return_value=self.connector)
        connector_patcher = mock.patch("lsst.dax.ppdb.bigquery.ppdb_bigquery.Connector", self.connector_cls)
        connector_patcher.start()
        self.addCleanup(connector_patcher.stop)

    def _make_engine(self, **kwargs: object) -> mock.MagicMock:
        """Call ``make_engine`` for the connector path with a patched
        ``create_engine`` and return the ``create_engine`` mock.
        """
        with mock.patch("sqlalchemy.create_engine") as create_engine:
            PpdbBigQuery.make_engine(self.config, **kwargs)
        return create_engine

    def test_connector_engine(self) -> None:
        """The connector engine is built from the environment using IAM
        auth.
        """
        with mock.patch.dict(os.environ, self.CONNECTOR_ENV, clear=True):
            create_engine = self._make_engine()

        self.connector_cls.assert_called_once_with(refresh_strategy="lazy")
        self.assertEqual(create_engine.call_args.args[0], "postgresql+pg8000://")
        create_engine.call_args.kwargs["creator"]()
        self.connector.connect.assert_called_once_with(
            "my-project:us-central1:my-instance",
            "pg8000",
            user="db-user",
            db="db-name",
            ip_type="private",
            enable_iam_auth=True,
        )

    def test_password_auth(self) -> None:
        """A password provider switches from IAM to password authentication."""
        with mock.patch.dict(os.environ, self.CONNECTOR_ENV, clear=True):
            create_engine = self._make_engine(password_provider=_StubPasswordProvider("hunter2"))

        create_engine.call_args.kwargs["creator"]()
        self.connector.connect.assert_called_once_with(
            "my-project:us-central1:my-instance",
            "pg8000",
            user="db-user",
            db="db-name",
            ip_type="private",
            password="hunter2",
            enable_iam_auth=False,
        )

    def test_invalid_environment_raises(self) -> None:
        """A missing required variable or an invalid IP type raises
        ``OSError``.
        """
        missing = dict(self.CONNECTOR_ENV)
        del missing["CLOUDSQL_INSTANCE_CONNECTION_NAME"]
        bad_ip = dict(self.CONNECTOR_ENV, CLOUDSQL_IP_TYPE="bogus")
        for env in (missing, bad_ip):
            with mock.patch.dict(os.environ, env, clear=True):
                with self.assertRaises(OSError):
                    self._make_engine()

    def test_cloudsql_enabled_dispatch(self) -> None:
        """``make_engine`` uses the connector only when
        ``CLOUDSQL_ENABLED``.
        """
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(PpdbBigQuery.is_cloudsql_enabled())
            engine = PpdbBigQuery.make_engine(self.config)
            self.assertEqual(engine.dialect.name, "sqlite")
        with mock.patch.dict(os.environ, {"CLOUDSQL_ENABLED": "true"}, clear=True):
            self.assertTrue(PpdbBigQuery.is_cloudsql_enabled())
