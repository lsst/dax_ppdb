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

import astropy.time
import sqlalchemy

from lsst.dax.ppdb import Ppdb
from lsst.dax.ppdb.bigquery import PpdbBigQuery
from lsst.dax.ppdb.bigquery.ppdb_replica_chunk_extended import ChunkStatus, PpdbReplicaChunkExtended
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
    directory: str = "/tmp/test",
) -> PpdbReplicaChunkExtended:
    """Create a test chunk with the given ID and status.

    Parameters
    ----------
    chunk_id
        The ID of the chunk to create.
    status
        The status to assign to the chunk.
    directory
        The directory path for the chunk, by default "/tmp/test".

    Returns
    -------
    `PpdbReplicaChunkExtended`
        A test chunk with the specified ID, status, and directory.
    """
    return PpdbReplicaChunkExtended(
        id=chunk_id,
        unique_id=uuid.uuid4(),
        last_update_time=astropy.time.Time("2021-01-01T00:01:00", format="isot", scale="tai"),
        replica_time=astropy.time.Time.now(),
        status=status,
        directory=Path(directory),
    )


class ReplicaChunkTestCase(SqliteMixin, unittest.TestCase):
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

    def test_insert_chunks_empty(self) -> None:
        """Test that inserting an empty list is a no-op."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks([])
        result = ppdb.query_chunks()
        self.assertEqual(len(result), 0)

    def test_update_chunks(self) -> None:
        """Test that update_chunks updates an existing chunk."""
        ppdb = self._make_ppdb()
        ppdb.insert_chunks([_make_chunk(1, ChunkStatus.EXPORTED)])
        ppdb.update_chunks([_make_chunk(1, ChunkStatus.STAGED)])

        result = ppdb.query_chunks()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, ChunkStatus.STAGED)

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
            ppdb.update_chunks([_make_chunk(99, ChunkStatus.STAGED)])

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

        table = ppdb.get_table("PpdbReplicaChunk")
        result = ppdb.query_chunks(table.columns["status"] == ChunkStatus.STAGED.value)
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

        table = ppdb.get_table("PpdbReplicaChunk")
        result = ppdb.query_chunks(order_by=table.columns["apdb_replica_chunk"])
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
