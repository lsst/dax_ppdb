# This file is part of dax_ppdb
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

from __future__ import annotations

__all__ = ["Replicator"]

import logging
import time
import warnings
from collections.abc import Iterable
from typing import TYPE_CHECKING

import astropy.time
from lsst.dax.apdb import monitor
from lsst.dax.apdb.timer import Timer

if TYPE_CHECKING:
    from lsst.dax.apdb import ApdbReplica, ReplicaChunk

    from .ppdb import Ppdb, PpdbReplicaChunk

_LOG = logging.getLogger(__name__)
_MON = monitor.MonAgent(__name__)


class Replicator:
    """Implementation of APDB-to-PPDB replication methods.

    Parameters
    ----------
    apdb : `~lsst.dax.apdb.ApdbReplica`
        Object providing access to APDB replica management.
    ppdb : `Ppdb`
        Object providing access to PPD operations.
    single : `bool`
        Copy single bucket and stop.
    update : `bool`
        If `True` then allow updates to previously replicated data.
    min_wait_time : `int`
        Minimum time in seconds to wait for replicating a chunk after a next
        chunk appears.
    max_wait_time : `int`
        Maximum time in seconds to wait for replicating a chunk if no chunk
        appears.
    check_interval : `int`
        Time in seconds to wait before checking for new chunks.
    """

    def __init__(
        self,
        apdb: ApdbReplica,
        ppdb: Ppdb,
        update: bool,
        min_wait_time: int,
        max_wait_time: int,
        check_interval: int,
    ):
        self._apdb = apdb
        self._ppdb = ppdb
        self._update = update
        self._min_wait_time = min_wait_time
        self._max_wait_time = max_wait_time
        self._check_interval = check_interval

    def copy_chunks(
        self,
        apdb_chunks: Iterable[ReplicaChunk],
        ppdb_chunks: Iterable[PpdbReplicaChunk],
        count: int | None = None,
    ) -> list[ReplicaChunk]:
        """Copy chunks of APDB data to PPDB.

        Parameters
        ----------
        apdb_chunks : `~collections.abc.Iterable` [`ReplicaChunk`]
            List of APDB chunks.
        ppdb_chunks : `~collections.abc.Iterable` [`PpdbReplicaChunk`]
            List of PPDB chunks.
        count : `int`, optional
            Maximum number of chunks to copy, if not specified then copy all
            chunks that can be copied.

        Returns
        -------
        count : `list` [`ReplicaChunk`]
            Chunks that were replicated to PPDB.
        """
        existing_ppdb_ids = {ppdb_chunk.id for ppdb_chunk in ppdb_chunks}
        chunks_to_copy = sorted(
            (apdb_chunk for apdb_chunk in apdb_chunks if apdb_chunk.id not in existing_ppdb_ids),
            key=lambda apdb_chunk: apdb_chunk.id,
        )
        _LOG.info("Replica chunks list contains %s chunks.", len(chunks_to_copy))

        copied = []
        while chunks_to_copy:
            apdb_chunk = chunks_to_copy.pop(0)
            if not self._can_replicate(apdb_chunk, bool(chunks_to_copy)):
                break

            _LOG.info("Will replicate chunk %s", apdb_chunk)
            with Timer("replicate_chunk_time", _MON, tags={"chunk_id": apdb_chunk.id}):
                self._replicate_one(apdb_chunk)
                copied.append(apdb_chunk)
                if count is not None and len(copied) >= count:
                    break

        return copied

    def _can_replicate(self, apdb_chunk: ReplicaChunk, more_chunks: bool) -> bool:
        """Decide whether chunk can be copied.

        Parameters
        ----------
        apdb_chunk : `ReplicaChunk`
            APDB chunk to copy.
        more_chunks : `bool`
            If True then there are more chunks to copy after this one.

        Returns
        -------
        can_copy : `bool`
            If True then chunk is OK to copy.
        """
        now = astropy.time.Time.now()
        delta = (now - apdb_chunk.last_update_time).to_value("sec")
        if more_chunks and delta >= self._min_wait_time:
            # There are newer chunks, wait `min_wait_time` before copy.
            _LOG.info(
                "Chunk %s can be copied, it is older than %s seconds and newer chunks exist.",
                apdb_chunk.id,
                self._min_wait_time,
            )
            return True
        if delta >= self._max_wait_time:
            # Otherwise wait `max_wait_time` before copy.
            _LOG.info(
                "Chunk %s can be copied, it is older than %s seconds.",
                apdb_chunk.id,
                self._max_wait_time,
            )
            return True
        return False

    def _replicate_one(self, replica_chunk: ReplicaChunk) -> None:
        """Copy single chcunk from APDB to PPDB."""
        with Timer("get_chunks_time", _MON, tags={"table": "DiaObject"}) as timer:
            dia_objects = self._apdb.getDiaObjectsChunks([replica_chunk.id])
            timer.add_values(row_count=len(dia_objects.rows()))
        _LOG.info("Selected %s DiaObjects for replication", len(dia_objects.rows()))
        with Timer("get_chunks_time", _MON, tags={"table": "DiaSource"}) as timer:
            dia_sources = self._apdb.getDiaSourcesChunks([replica_chunk.id])
            timer.add_values(row_count=len(dia_objects.rows()))
        _LOG.info("Selected %s DiaSources for replication", len(dia_sources.rows()))
        with Timer("get_chunks_time", _MON, tags={"table": "DiaForcedSource"}) as timer:
            dia_forced_sources = self._apdb.getDiaForcedSourcesChunks([replica_chunk.id])
            timer.add_values(row_count=len(dia_objects.rows()))
        _LOG.info("Selected %s DiaForcedSources for replication", len(dia_forced_sources.rows()))

        with Timer("store_chunks_time", _MON):
            self._ppdb.store(replica_chunk, dia_objects, dia_sources, dia_forced_sources, update=self._update)

    def run(self, single=False) -> None:
        """Run the replication loop.

        Parameters
        ----------
        single : `bool`, optional
            If `True` then copy only one chunk and stop. Default is `False`.
        """
        wait_time = 0
        while True:
            if wait_time > 0:
                _LOG.info("Waiting %s seconds before next iteration.", wait_time)
                time.sleep(wait_time)

            # Get existing chunks in APDB.
            apdb_chunks = self._apdb.getReplicaChunks()
            if apdb_chunks is None:
                raise TypeError("APDB implementation does not support replication")
            min_chunk_id = min((chunk.id for chunk in apdb_chunks), default=None)
            if min_chunk_id is None:
                # No chunks in APDB?
                _LOG.info("No replica chunks found in APDB.")
                if single:
                    return
                else:
                    wait_time = self._check_interval
                    continue

            # Get existing chunks in PPDB.
            ppdb_chunks = self._ppdb.get_replica_chunks(min_chunk_id)
            if ppdb_chunks is None:
                raise TypeError("PPDB implementation does not support replication")

            # Check existing PPDB ids for consistency.
            ppdb_id_map = {ppdb_chunk.id: ppdb_chunk for ppdb_chunk in ppdb_chunks}
            for apdb_chunk in apdb_chunks:
                if (ppdb_chunk := ppdb_id_map.get(apdb_chunk.id)) is not None:
                    if ppdb_chunk.unique_id != apdb_chunk.unique_id:
                        # Crash if running in a single-shot mode.
                        message = f"Inconsistent values of unique ID - APDB: {apdb_chunk} PPDB: {ppdb_chunk}"
                        if single:
                            raise ValueError(message)
                        else:
                            warnings.warn(message)

            # Replicate one or many chunks.
            chunks = self.copy_chunks(apdb_chunks, ppdb_chunks, 1 if single else None)
            if single:
                break
            # IF something was copied then start new iteration immediately.
            wait_time = 0 if chunks else self._check_interval
