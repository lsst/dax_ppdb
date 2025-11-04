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

from __future__ import annotations

__all__ = ["PpdbTest"]

import unittest
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import astropy.time

from lsst.dax.apdb import (
    Apdb,
    ApdbCloseDiaObjectValidityRecord,
    ApdbConfig,
    ApdbReassignDiaSourceRecord,
    ApdbReplica,
    ApdbUpdateRecord,
    ApdbWithdrawDiaForcedSourceRecord,
    ReplicaChunk,
)
from lsst.dax.apdb.sql import ApdbSql
from lsst.dax.apdb.tests.data_factory import makeForcedSourceCatalog, makeObjectCatalog, makeSourceCatalog
from lsst.sphgeom import Angle, Circle, Region, UnitVector3d

from ..config import PpdbConfig
from ..ppdb import Ppdb, PpdbReplicaChunk
from ..replicator import Replicator

if TYPE_CHECKING:
    import pandas

    class TestCaseMixin(unittest.TestCase):
        """Base class for mixin test classes that use TestCase methods."""

else:

    class TestCaseMixin:
        """Do-nothing definition of mixin base class for regular execution."""


def _make_region(xyz: tuple[float, float, float] = (1.0, 1.0, -1.0)) -> Region:
    """Make a region to use in tests"""
    pointing_v = UnitVector3d(*xyz)
    fov = 0.0013  # radians
    region = Circle(pointing_v, Angle(fov / 2))
    return region


class PpdbTest(TestCaseMixin, ABC):
    """Base class for Ppdb tests that can be specialized for concrete
    implementation.

    This can only be used as a mixin class for a unittest.TestCase and it
    calls various assert methods.
    """

    include_update_records = False
    """If True then test replication of ApdbUpdateRecords."""

    @abstractmethod
    def make_instance(self, **kwargs: Any) -> PpdbConfig:
        """Make database instance and return configuration for it.

        Parameters
        ----------
        **kwargs : `Any`
            Instance-specific parameters for the PPDB database.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_apdb_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make APDB instance and return configuration for it, APDB must have
        replication enabled.

        Parameters
        ----------
        **kwargs : `Any`
            Instance-specific parameters for the APDB.
        """
        raise NotImplementedError()

    def test_empty_db(self) -> None:
        """Test for instantiation a database and making queries on empty
        database.
        """
        config = self.make_instance()
        ppdb = Ppdb.from_config(config)
        chunks = ppdb.get_replica_chunks()
        if chunks is not None:
            self.assertEqual(len(chunks), 0)

    def _fill_apdb(self, apdb: Apdb) -> None:
        """Populate APDB with some data to replicate."""
        visit_time = astropy.time.Time("2021-01-01T00:01:00", format="isot", scale="tai")
        region1 = _make_region((1.0, 1.0, -1.0))
        region2 = _make_region((-1.0, -1.0, -1.0))
        nobj = 100
        objects1 = makeObjectCatalog(region1, nobj)
        objects2 = makeObjectCatalog(region2, nobj, start_id=nobj * 2)

        # With the default 10 minutes replica chunk window we should have 4
        # records. All timestamps are far in the past, means that replication
        # of the last chunk can run without waiting.
        visits = [
            (astropy.time.Time("2021-01-01T00:01:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:02:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-01-01T00:11:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:12:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-01-01T00:45:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:46:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-03-01T00:01:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-03-01T00:02:00", format="isot", scale="tai"), objects2),
        ]

        # Time when apdates are applied.
        update_time = astropy.time.Time("2021-03-01T12:00:00")

        update_records = []
        start_id = 0
        for visit, (visit_time, objects) in enumerate(visits):
            sources = makeSourceCatalog(objects, visit_time, visit=visit, start_id=start_id)
            fsources = makeForcedSourceCatalog(objects, visit_time, visit=visit)
            apdb.store(visit_time, objects, sources, fsources)
            start_id += nobj

            if self.include_update_records and visit == (len(visits) - 1):
                # Generate few update records.
                update_records = self._make_update_records(sources, fsources, update_time)

        if self.include_update_records:
            chunk = ReplicaChunk.make_replica_chunk(update_time, apdb.getConfig().replica_chunk_seconds)
            # All our tests use SQL APDB.
            assert isinstance(apdb, ApdbSql), "Expecting ApdbSql instance"
            apdb._storeUpdateRecords(update_records, chunk, store_chunk=True)

    def _make_update_records(
        self, sources: pandas.DataFrame, fsources: pandas.DataFrame, update_time: astropy.time.Time
    ) -> list[ApdbUpdateRecord]:
        update_time_ns = int(update_time.unix_tai * 1e9)
        records: list[ApdbUpdateRecord] = []

        # Reassign one DIASource to SSObject.
        dia_source = sources.iloc[0]
        records.append(
            ApdbReassignDiaSourceRecord(
                update_time_ns=update_time_ns,
                update_order=0,
                diaSourceId=int(dia_source["diaSourceId"]),
                diaObjectId=int(dia_source["diaObjectId"]),
                ssObjectId=1,
                ssObjectReassocTimeMjdTai=float(update_time.tai.mjd),
                ra=float(dia_source["ra"]),
                dec=float(dia_source["dec"]),
            )
        )

        # Close validity interval for matching DIAObject.
        records.append(
            ApdbCloseDiaObjectValidityRecord(
                update_time_ns=update_time_ns,
                update_order=1,
                diaObjectId=int(dia_source["diaObjectId"]),
                validityEndMjdTai=update_time.tai.mjd,
                nDiaSources=None,
                ra=float(dia_source["ra"]),
                dec=float(dia_source["dec"]),
            )
        )

        # Withdraw one DIAForcedSource.
        dia_fsource = fsources.iloc[0]
        records.append(
            ApdbWithdrawDiaForcedSourceRecord(
                update_time_ns=update_time_ns,
                update_order=2,
                diaObjectId=int(dia_fsource["diaObjectId"]),
                visit=int(dia_fsource["visit"]),
                detector=int(dia_fsource["detector"]),
                timeWithdrawnMjdTai=update_time.tai.mjd,
                ra=float(dia_source["ra"]),
                dec=float(dia_source["dec"]),
            )
        )

        return records

    def _check_chunks(
        self, apdb_chunks: Sequence[ReplicaChunk], ppdb_chunks: Sequence[PpdbReplicaChunk]
    ) -> None:
        """Check PPDB replica chunks against APDB chunks."""
        self.assertLessEqual(len(ppdb_chunks), len(apdb_chunks))
        for i in range(len(ppdb_chunks)):
            self.assertEqual(ppdb_chunks[i].id, apdb_chunks[i].id)
            self.assertEqual(ppdb_chunks[i].last_update_time, apdb_chunks[i].last_update_time)
            self.assertEqual(ppdb_chunks[i].unique_id, apdb_chunks[i].unique_id)

    def test_replication_single(self) -> None:
        """Test replication from APDB to PPDB using a single chunk option."""
        apdb_config = self.make_apdb_instance()
        apdb = Apdb.from_config(apdb_config)

        self._fill_apdb(apdb)

        expected_chunks = 5 if self.include_update_records else 4

        # Get list of chunks from APDB.
        apdb_replica = ApdbReplica.from_config(apdb_config)
        apdb_chunks = apdb_replica.getReplicaChunks()
        assert apdb_chunks is not None
        self.assertEqual(len(apdb_chunks), expected_chunks)

        # Make PPDB instance.
        ppdb_config = self.make_instance()
        ppdb = Ppdb.from_config(ppdb_config)
        ppdb_chunks = ppdb.get_replica_chunks()
        assert ppdb_chunks is not None
        self.assertEqual(len(ppdb_chunks), 0)

        # Replicate those to PPDB.
        replicator = Replicator(
            apdb_replica, ppdb, update=False, min_wait_time=0, max_wait_time=0, check_interval=0
        )

        # Copy single chunk.
        replicator.run(single=True)
        ppdb_chunks = ppdb.get_replica_chunks()
        assert ppdb_chunks is not None
        self.assertEqual(len(ppdb_chunks), 1)
        self._check_chunks(apdb_chunks, ppdb_chunks)

        replicator.run(single=True)
        ppdb_chunks = ppdb.get_replica_chunks()
        assert ppdb_chunks is not None
        self.assertEqual(len(ppdb_chunks), 2)

        replicator.run(single=True)
        ppdb_chunks = ppdb.get_replica_chunks()
        assert ppdb_chunks is not None
        self.assertEqual(len(ppdb_chunks), 3)

        replicator.run(single=True)
        ppdb_chunks = ppdb.get_replica_chunks()
        assert ppdb_chunks is not None
        self.assertEqual(len(ppdb_chunks), 4)
        self._check_chunks(apdb_chunks, ppdb_chunks)

        if expected_chunks > 4:
            replicator.run(single=True)
            ppdb_chunks = ppdb.get_replica_chunks()
            assert ppdb_chunks is not None
            self.assertEqual(len(ppdb_chunks), 5)
            self._check_chunks(apdb_chunks, ppdb_chunks)

        # All is done, this should just return.
        replicator.run(single=True)
        ppdb_chunks = ppdb.get_replica_chunks()
        assert ppdb_chunks is not None
        self.assertEqual(len(ppdb_chunks), expected_chunks)

    def test_replication_all(self) -> None:
        """Test replication from APDB to PPDB with multiple chunks."""
        apdb_config = self.make_apdb_instance()
        apdb = Apdb.from_config(apdb_config)

        self._fill_apdb(apdb)

        expected_chunks = 5 if self.include_update_records else 4

        # Get list of chunks from APDB.
        apdb_replica = ApdbReplica.from_config(apdb_config)
        apdb_chunks = apdb_replica.getReplicaChunks()
        assert apdb_chunks is not None
        self.assertEqual(len(apdb_chunks), expected_chunks)

        # Make PPDB instance.
        ppdb_config = self.make_instance()
        ppdb = Ppdb.from_config(ppdb_config)
        ppdb_chunks = ppdb.get_replica_chunks()
        assert ppdb_chunks is not None
        self.assertEqual(len(ppdb_chunks), 0)

        # Replicate those to PPDB.
        replicator = Replicator(
            apdb_replica, ppdb, update=False, min_wait_time=0, max_wait_time=0, check_interval=0
        )

        # Copy single chunk.
        replicator.run(exit_on_empty=True)
        ppdb_chunks = ppdb.get_replica_chunks()
        assert ppdb_chunks is not None
        self.assertEqual(len(ppdb_chunks), expected_chunks)
        self._check_chunks(apdb_chunks, ppdb_chunks)

        # All is done, this should just return.
        replicator.run(single=True)
        ppdb_chunks = ppdb.get_replica_chunks()
        assert ppdb_chunks is not None
        self.assertEqual(len(ppdb_chunks), expected_chunks)
