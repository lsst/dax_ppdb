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

from pathlib import Path

from lsst.dax.apdb import ApdbReplica

from ..export._chunk_exporter import ChunkExporter
from ..replicator import Replicator
from ..sql._ppdb_sql import PpdbSqlConfig


def export_chunks_run(
    apdb_config: str,
    ppdb_config: str,
    single: bool,
    update: bool,
    min_wait_time: int,
    max_wait_time: int,
    check_interval: int,
    exit_on_empty: bool,
    directory: str,
    topic: str,
    compression_format: str,
    batch_size: int,
    delete_existing: bool,
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
    directory : `str`
        Directory where the chunks are stored.
    topic_name : `str`
        Pub/Sub topic name for publishing the exported replica chunks.
    compression_format : `str`
        Compression format for the chunks.
    batch_size : `int`
        Number of records to write in each batch for the Parquet files.
    exit_on_empty : `bool`
        Exit if no chunks are found.
    """
    apdb = ApdbReplica.from_uri(apdb_config)
    ppdb_sql_config = PpdbSqlConfig.from_uri(ppdb_config)

    ppdb = ChunkExporter(
        ppdb_sql_config,
        apdb.schemaVersion(),
        Path(directory),
        topic_name=topic,
        batch_size=batch_size,
        compression_format=compression_format,
        delete_existing=delete_existing,
    )

    replicator = Replicator(apdb, ppdb, update, min_wait_time, max_wait_time, check_interval)
    replicator.run(single=single, exit_on_empty=exit_on_empty)
