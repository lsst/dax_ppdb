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

__all__ = ["export_chunks_run"]

import logging
import time
import warnings
from pathlib import Path

from lsst.dax.apdb import ApdbReplica

from ..export._chunk_exporter import ChunkExporter
from ..replicator import Replicator
from ..sql._ppdb_sql import PpdbSqlConfig

_LOG = logging.getLogger(__name__)


def export_chunks_run(
    apdb_config: str,
    ppdb_config: str,
    single: bool,
    update: bool,
    min_wait_time: int,
    max_wait_time: int,
    check_interval: int,
    directory: str,
    compression_format: str,
    batch_size: int,
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
    min_wait_time : `int`
        Minimum time in seconds to wait for replicating a chunk after a next
        chunk appears.
    max_wait_time : `int`
        Maximum time in seconds to wait for replicating a chunk if no chunk
        appears.
    check_interval : `int`
        Time in seconds to wait before next check if there was no replicated
        chunks.
    """
    apdb = ApdbReplica.from_uri(apdb_config)

    _ppdb_config = PpdbSqlConfig.from_uri(ppdb_config)
    ppdb = ChunkExporter(
        _ppdb_config, Path(directory), batch_size=batch_size, compression_format=compression_format
    )

    replicator = Replicator(apdb, ppdb, update, min_wait_time, max_wait_time)

    wait_time = 0
    while True:
        if wait_time > 0:
            _LOG.info("Waiting %s seconds before next iteration.", wait_time)
            time.sleep(wait_time)

        # Get existing chunks in APDB.
        apdb_chunks = apdb.getReplicaChunks()
        if apdb_chunks is None:
            raise TypeError("APDB implementation does not support replication")
        min_chunk_id = min((chunk.id for chunk in apdb_chunks), default=None)
        if min_chunk_id is None:
            # No chunks in APDB?
            _LOG.info("No replica chunks found in APDB.")
            if single:
                return
            else:
                wait_time = check_interval
                continue

        # Get existing chunks in PPDB.
        ppdb_chunks = ppdb.get_replica_chunks(min_chunk_id)
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
        chunks = replicator.copy_chunks(apdb_chunks, ppdb_chunks, 1 if single else None)
        if single:
            break
        # IF something was copied then start new iteration immediately.
        wait_time = 0 if chunks else check_interval
