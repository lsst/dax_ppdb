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
import yaml

from lsst.dax.ppdb import Ppdb
from lsst.dax.ppdb.bigquery import ChunkStatus, PpdbBigQuery, PpdbReplicaChunkExtended
from lsst.dax.ppdb.bigquery.ppdb_bigquery import UpdatableField
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


class DbUrlFromEnvTestCase(unittest.TestCase):
    """Tests for building the database URL from environment variables."""

    def test_db_url_from_env_missing_user_raises(self) -> None:
        """Test that a missing ``DB_USER`` raises ``OSError``."""
        with mock.patch.dict(os.environ, {"DB_NAME": "ppdb-chunk-tracking"}, clear=True):
            with self.assertRaisesRegex(OSError, "DB_USER"):
                PpdbBigQuery._db_url_from_env()

    def test_db_url_from_env_missing_name_raises(self) -> None:
        """Test that a missing ``DB_NAME`` raises ``OSError``."""
        with mock.patch.dict(os.environ, {"DB_USER": "test-user"}, clear=True):
            with self.assertRaisesRegex(OSError, "DB_NAME"):
                PpdbBigQuery._db_url_from_env()

    def test_db_url_from_env_builds_url(self) -> None:
        """Test that the URL is built when all required variables are set."""
        env = {
            "DB_USER": "usdf-replication@my-project.iam",
            "DB_HOST": "db.example.com",
            "DB_PORT": "6543",
            "DB_NAME": "ppdb-chunk-tracking",
            "DB_SSLMODE": "require",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            db_url = PpdbBigQuery._db_url_from_env()

        self.assertEqual(
            db_url,
            "postgresql+psycopg2://usdf-replication%40my-project.iam@db.example.com:6543"
            "/ppdb-chunk-tracking?sslmode=require",
        )


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class FromEnvPostgresTestCase(PostgresMixin, unittest.TestCase):
    """Test ``from_env`` building the database URL from the individual
    ``DB_*`` environment variables against a real Postgres database.
    """

    def test_from_env_use_db_env_vars(self) -> None:
        """Test that ``from_env(use_db_env_vars=True)`` connects using a URL
        built from the environment, overriding the configured ``db_url``.
        """
        # Initialise the PPDB schema in the test Postgres server.
        config = self.make_instance(test_name="from_env")

        # Capture the real connection parameters, then replace the configured
        # URL with a bogus one to prove the environment override is used.
        server_url = sqlalchemy.make_url(self.server.url())
        config.sql.db_url = "postgresql+psycopg://invalid-host:5432/nonexistent"

        # Serialize the configuration to a file for ``PPDB_CONFIG_URI``.
        config_path = os.path.join(self.tempdir, "ppdb_config.yaml")
        config_dict = config.model_dump(exclude_unset=True, exclude_defaults=True)
        config_dict["implementation_type"] = "bigquery"
        with open(config_path, "w") as config_file:
            yaml.dump(config_dict, config_file)

        env = {
            "PPDB_CONFIG_URI": config_path,
            "DB_USER": server_url.username,
            "DB_HOST": server_url.host,
            "DB_PORT": str(server_url.port),
            "DB_NAME": server_url.database,
            "DB_SSLMODE": "disable",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            ppdb = PpdbBigQuery.from_env(use_db_env_vars=True)

        # A successful query proves the environment-derived URL connected to
        # the real database rather than the bogus configured one.
        self.assertEqual(ppdb.get_replica_chunks(), [])
