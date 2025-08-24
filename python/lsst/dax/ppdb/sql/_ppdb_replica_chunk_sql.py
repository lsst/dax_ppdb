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

__all__ = ["PpdbReplicaChunkSql"]

import datetime

import astropy.time
import felis
import sqlalchemy
from lsst.dax.apdb import ReplicaChunk, VersionTuple, schema_model
from sqlalchemy import sql

from ..ppdb import ChunkStatus, PpdbReplicaChunk
from ._ppdb_sql import PpdbSql

VERSION = VersionTuple(0, 1, 1)
"""Version for the code defined in this module. This needs to be updated
(following compatibility rules) when schema produced by this code changes.
"""


class PpdbReplicaChunkSql(PpdbSql):
    """Extension to the `PpdbSql` class that provides additional functionality
    for managing PPDB replica chunks.
    """

    @classmethod
    def _create_replica_chunk_table(cls, table_name: str | None = None) -> schema_model.Table:
        """Create the ``PpdbReplicaChunk`` table with additional fields for
        status and directory.

        Parameters
        ----------
        table_name : `str`, optional
            Name of the table to create. If not provided, defaults to
            "PpdbReplicaChunk".

        Notes
        -----
        This overrides the base method to add additional columns for
        ``status`` and ``directory`` to the replica chunk table schema.
        """
        replica_chunk_table = super()._create_replica_chunk_table()
        replica_chunk_table.columns.extend(
            [
                schema_model.Column(
                    name="status",
                    id=f"#{table_name}.status",
                    datatype=felis.datamodel.DataType.string,
                    nullable=True,
                ),
                schema_model.Column(
                    name="directory",
                    id=f"#{table_name}.directory",
                    datatype=felis.datamodel.DataType.string,
                    nullable=True,
                ),
            ]
        )
        return replica_chunk_table

    def get_replica_chunks_by_status(self, status: ChunkStatus) -> list[PpdbReplicaChunk]:
        """Return collection of replica chunks known to the database with a
        given status.

        Parameters
        ----------
        status : `ChunkStatus`
            Status of the replica chunks to return.

        Returns
        -------
        chunks : `list` [`PpdbReplicaChunk`] or `None`
            List of chunks with the specified status.

        Notes
        -----
        This is an alternative to `get_replica_chunks` that allows retrieving
        chunks by their status. The original method is left for backward
        compatibility, as it is used internally by the replication process.
        """
        table = self._get_table("PpdbReplicaChunk")
        query = sql.select(
            table.columns["apdb_replica_chunk"],
            table.columns["last_update_time"],
            table.columns["unique_id"],
            table.columns["replica_time"],
            table.columns["status"],  # New status field
            table.columns["directory"],  # New directory field
        ).order_by(table.columns["last_update_time"])
        query = query.where(table.columns["status"] == status.value)
        ids: list[PpdbReplicaChunk] = []
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
            for row in result:
                # When we store these timestamps we convert astropy Time to
                # unix_tai and then to `datetime` in UTC. This conversion
                # reverses that process,
                last_update_time = astropy.time.Time(row[1], format="datetime", scale="tai")
                replica_time = astropy.time.Time(row[3], format="datetime", scale="tai")
                ids.append(
                    PpdbReplicaChunk(
                        id=row[0],
                        last_update_time=last_update_time,
                        unique_id=row[2],
                        replica_time=replica_time,
                        status=row[4],
                        directory=row[5],
                    )
                )
        return ids

    def store_chunk(
        self,
        replica_chunk: ReplicaChunk,
        connection: sqlalchemy.engine.Connection,
        update: bool,
        status: ChunkStatus | None = None,
        directory: str | None = None,
    ) -> None:
        """Insert or replace single record in PpdbReplicaChunk table, including
        the status and directory of the replica chunk.
        """
        # `astropy.Time.datetime` returns naive datetime, even though all
        # astropy times are in UTC. Add UTC timezone to timestamp so that
        # the correct value is stored in the database.
        insert_dt = datetime.datetime.fromtimestamp(
            replica_chunk.last_update_time.unix_tai, tz=datetime.timezone.utc
        )
        now = datetime.datetime.fromtimestamp(astropy.time.Time.now().unix_tai, tz=datetime.timezone.utc)

        table = self._get_table("PpdbReplicaChunk")

        values = {"last_update_time": insert_dt, "unique_id": replica_chunk.unique_id, "replica_time": now}
        if status is not None:
            # Add status to the values to be inserted.
            values["status"] = status.value
        if directory is not None:
            # Add directory to the values to be inserted.
            values["directory"] = directory
        row = {"apdb_replica_chunk": replica_chunk.id} | values
        self._upsert(connection, update, table, values, row)
