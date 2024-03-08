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

__all__ = ["replication_run"]

import logging
from typing import TYPE_CHECKING

from lsst.dax.apdb import ApdbReplica

from ..ppdb import Ppdb

if TYPE_CHECKING:
    from lsst.dax.apdb import ReplicaChunk

    from ..ppdb import PpdbReplicaChunk

_LOG = logging.getLogger(__name__)


def replication_run(
    apdb_config: str,
    ppdb_config: str,
    single: bool,
    update: bool,
) -> None:
    """Execute replication process from APDB to PPDB.

    Parameters
    ----------
    apdb_config : `str`
        URL for APDB configuration file.
    ppdb_config : `str`
        URL for PPDB configuration file.
    single : `bool`
        Copy single bucket and stop.
    update : `bool`
        If `True` then allow updates to previously replicated data.
    """
    apdb = ApdbReplica.from_uri(apdb_config)
    ppdb = Ppdb.from_uri(ppdb_config)

    chunks = apdb.getReplicaChunks()
    if chunks is None:
        raise TypeError("APDB implementation does not support replication")
    ppdb_chunks = ppdb.get_replica_chunks()
    if ppdb_chunks is None:
        raise TypeError("PPDB implementation does not support replication")

    ids = _merge_ids(chunks, ppdb_chunks)

    # Check existing PPDB ids for consistency.
    for apdb_chunk, ppdb_chunk in ids:
        if ppdb_chunk is not None:
            if ppdb_chunk.unique_id != apdb_chunk.unique_id:
                raise ValueError(f"Inconsistent values of unique ID - APDB: {apdb_chunk} PPDB: {ppdb_chunk}")

    ids_to_copy = [apdb_chunk for apdb_chunk, ppdb_chunk in ids if ppdb_chunk is None]
    for apdb_chunk in ids_to_copy:
        _LOG.info("Will replicate bucket %s", apdb_chunk)
        _replicate_one(apdb, ppdb, apdb_chunk, update)
        if single:
            break


def _merge_ids(
    chunks: list[ReplicaChunk], ppdb_chunks: list[PpdbReplicaChunk]
) -> list[tuple[ReplicaChunk, PpdbReplicaChunk | None]]:
    """Make a list of pairs (apdb_chunk, ppdb_chunk), if ppdb_chunk does not
    exist for apdb_chunk then it will be None.
    """
    ppdb_id_map = {ppdb_chunk.id: ppdb_chunk for ppdb_chunk in ppdb_chunks}
    apdb_ids = sorted(chunks, key=lambda apdb_chunk: apdb_chunk.id)
    return [(apdb_chunk, ppdb_id_map.get(apdb_chunk.id)) for apdb_chunk in apdb_ids]


def _replicate_one(apdb: ApdbReplica, ppdb: Ppdb, replica_chunk: ReplicaChunk, update: bool) -> None:

    dia_objects = apdb.getDiaObjectsChunks([replica_chunk.id])
    _LOG.info("Selected %s DiaObjects for replication", len(dia_objects.rows()))
    dia_sources = apdb.getDiaSourcesChunks([replica_chunk.id])
    _LOG.info("Selected %s DiaSources for replication", len(dia_sources.rows()))
    dia_forced_sources = apdb.getDiaForcedSourcesChunks([replica_chunk.id])
    _LOG.info("Selected %s DiaForcedSources for replication", len(dia_forced_sources.rows()))

    ppdb.store(replica_chunk, dia_objects, dia_sources, dia_forced_sources, update=update)
