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

import dataclasses
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path

import astropy.time
import felis
import sqlalchemy
from lsst.dax.apdb import ReplicaChunk, VersionTuple, schema_model
from sqlalchemy import sql

from ..ppdb import PpdbReplicaChunk
from ..sql._ppdb_sql import PpdbSql


class ChunkStatus(StrEnum):
    """Status of a replica chunk in the PPDB."""

    EXPORTED = "exported"
    """Chunk has been exported from the APDB to a local parquet file."""
    UPLOADED = "uploaded"
    """Chunk has been uploaded to cloud storage."""
    FAILED = "failed"
    """Chunk processing failed and an error occurred."""


@dataclass(frozen=True)
class PpdbReplicaChunkExtended(PpdbReplicaChunk):
    """ReplicaChunk with additional PPDB-specific info."""

    status: ChunkStatus
    """Status of the replica chunk. This may be ``None`` in older versions of
    the ``PpdbReplicaChunk`` table."""

    directory: Path
    """Directory where the exported replica chunk data is stored. This may be
    ``None`` in older versions of the ``PpdbReplicaChunk`` table."""

    @property
    def manifest_name(self) -> str:
        """Filename of the manifest file for this chunk."""
        return f"chunk_{self.id}.manifest.json"

    @property
    def manifest_path(self) -> Path:
        """Path to the manifest file for this chunk, or `None` if directory is
        not set.
        """
        if self.directory is None:
            raise ValueError(f"directory for replica chunk {self.id} is not set")
        return Path(self.directory) / self.manifest_name

    @property
    def replica_time_dt_utc(self) -> datetime:
        """Return the replica_time as a `datetime` in UTC."""
        return datetime.fromtimestamp(self.replica_time.unix_tai, tz=timezone.utc)

    @property
    def last_update_time_dt_utc(self) -> datetime:
        """Return the last_update_time as a `datetime` in UTC."""
        return datetime.fromtimestamp(self.last_update_time.unix_tai, tz=timezone.utc)

    @classmethod
    def from_replica_chunk(
        cls, replica_chunk: ReplicaChunk, status: ChunkStatus, directory: Path
    ) -> PpdbReplicaChunkExtended:
        """Create a `PpdbReplicaChunkExtended` from a `ReplicaChunk`.

        Parameters
        ----------
        replica_chunk : `ReplicaChunk`
            The `ReplicaChunk` to convert.
        status : `ChunkStatus`
            Status of the replica chunk.
        directory : `Path`
            Directory where the replica chunk data is stored.

        Returns
        -------
        extended_chunk : `PpdbReplicaChunkExtended`
            The converted `PpdbReplicaChunkExtended`.
        """
        return PpdbReplicaChunkExtended(
            id=replica_chunk.id,
            unique_id=replica_chunk.unique_id,
            last_update_time=replica_chunk.last_update_time,
            replica_time=astropy.time.Time.now(),
            status=status,
            directory=directory,
        )

    def with_new_status(self, new_status: ChunkStatus) -> PpdbReplicaChunkExtended:
        """Create a new `PpdbReplicaChunkExtended` with the same properties as
        this one, but with a different status.

        Parameters
        ----------
        new_status : `ChunkStatus`
            The new status to set.

        Returns
        -------
        new_chunk : `PpdbReplicaChunkExtended`
            The new chunk with the updated status.
        """
        return dataclasses.replace(self, status=new_status)


class PpdbReplicaChunkSql(PpdbSql):
    """Extension to the `PpdbSql` class that provides additional functionality
    for managing PPDB replica chunks.
    """

    def get_schema_version(self) -> VersionTuple:
        """Retrieve version number from given metadata key."""
        version_str = self.metadata.get(self.meta_schema_version_key)
        if version_str is None:
            # Should not happen with existing metadata table.
            raise RuntimeError(
                f"Version key {self.meta_schema_version_key!r} does not exist in metadata table."
            )
        return VersionTuple.fromString(version_str)

    @classmethod
    def create_replica_chunk_table(cls, table_name: str | None = None) -> schema_model.Table:
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
        replica_chunk_table = super().create_replica_chunk_table()
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

    def get_replica_chunks_by_status(self, status: ChunkStatus) -> list[PpdbReplicaChunkExtended]:
        """Return an ordered collection of replica chunks known to the database
        with a given status.

        Parameters
        ----------
        status : `ChunkStatus`
            Status of the replica chunks to return.

        Returns
        -------
        chunks : `list` [`PpdbReplicaChunkExtended`]
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
        ids: list[PpdbReplicaChunkExtended] = []
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
            for row in result:
                # When we store these timestamps we convert astropy Time to
                # unix_tai and then to `datetime` in UTC. This conversion
                # reverses that process,
                last_update_time = astropy.time.Time(row[1], format="datetime", scale="tai")
                replica_time = astropy.time.Time(row[3], format="datetime", scale="tai")
                ids.append(
                    PpdbReplicaChunkExtended(
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
        self, replica_chunk: PpdbReplicaChunkExtended, connection: sqlalchemy.engine.Connection, update: bool
    ) -> None:
        """Insert or replace single record in PpdbReplicaChunk table, including
        the status and directory of the replica chunk.
        """
        table = self._get_table("PpdbReplicaChunk")

        values = {
            "last_update_time": replica_chunk.last_update_time_dt_utc,
            "unique_id": replica_chunk.unique_id,
            "replica_time": replica_chunk.replica_time_dt_utc,
            "status": replica_chunk.status,
            "directory": str(replica_chunk.directory),
        }
        row = {"apdb_replica_chunk": replica_chunk.id} | values
        self.upsert(connection, update, table, values, row)
