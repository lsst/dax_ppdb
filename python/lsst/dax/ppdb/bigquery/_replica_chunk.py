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

__all__ = ["ChunkStatus", "PpdbReplicaChunkExtended", "PpdbReplicaChunkSql"]

import dataclasses
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

import astropy.time
import sqlalchemy
from lsst.dax.apdb import ApdbMetadata, ReplicaChunk, VersionTuple
from lsst.dax.apdb.sql import ApdbMetadataSql
from sqlalchemy import Table, sql

from ..ppdb import PpdbReplicaChunk
from ..sql.ppdb_sql_utils import PpdbSqlUtils
from ._config import PpdbSqlConfig

_LOG = logging.getLogger(__name__)


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
    """Status of the replica chunk (`ChunkStatus`)."""

    directory: Path
    """Directory where the exported replica chunk data is stored (`Path`)."""

    @property
    def manifest_name(self) -> str:
        """Name of the manifest file for this chunk (`str`)."""
        return f"chunk_{self.id}.manifest.json"

    @property
    def manifest_path(self) -> Path:
        """Path to the manifest file for this chunk (`str`)."""
        if self.directory is None:
            raise ValueError(f"directory for replica chunk {self.id} is not set")
        return Path(self.directory) / self.manifest_name

    @property
    def replica_time_dt_utc(self) -> datetime:
        """The replica_time in UTC (`datetime`)."""
        return datetime.fromtimestamp(self.replica_time.unix_tai, tz=timezone.utc)

    @property
    def last_update_time_dt_utc(self) -> datetime:
        """The last_update_time in UTC (`datetime`)."""
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


class PpdbReplicaChunkSql:
    """Provides replica chunk management functionality with the
    ``PpdbReplicaChunk`` table, using a SQL database as the backend.

    This is needed by the data processing pipeline for BigQuery to manage the
    export and upload of replica chunks. It can be used to implement the
    `Ppdb` interface but does not sub-class it directly.
    """

    def __init__(self, config: PpdbSqlConfig):
        # Check for required schema name, which is defined as optional in the
        # config.
        if config.schema_name is None:
            raise ValueError("schema_name must be provided in config")

        # Set the replica chunk table name.
        self._replica_chunk_table_name = config.replica_chunk_table

        # Read the APDB schema to get the version and SQA metadata.
        self._sa_metadata, self._schema_version = PpdbSqlUtils.read_schema(
            config, include_tables=[PpdbSqlUtils.metadata_table_name]
        )

        # Make the SQA engine.
        self._engine = PpdbSqlUtils.make_engine(config)

        # Initialize the APDB metadata interface.
        meta_table = sqlalchemy.schema.Table(
            PpdbSqlUtils.metadata_table_name,
            sqlalchemy.MetaData(schema=config.schema_name),
            autoload_with=self._engine,
        )
        self._metadata = ApdbMetadataSql(self._engine, meta_table)

    @property
    def schema_version(self) -> VersionTuple:
        """Version of the APDB schema used by this PPDB (`VersionTuple`)."""
        return self._schema_version

    @property
    def replica_chunk_table(self) -> Table:
        """The `~sqlalchemy.Table` used to track replica chunks."""
        return PpdbSqlUtils.find_table_by_name(self._sa_metadata, self._replica_chunk_table_name)

    @property
    def metadata(self) -> ApdbMetadata:
        """The `ApdbMetadata` object for satisfying the `Ppdb` interface."""
        return self._metadata

    @classmethod
    def _to_astropy_tai(cls, obj: Any) -> astropy.time.Time:
        """Convert a database object to `astropy.time.Time` in TAI scale.

        Parameters
        ----------
        obj : `Any`
            The object to convert, expected to be a `datetime` in UTC.
            The type signature is generic to match astropy's typing.
        """
        return astropy.time.Time(obj, format="datetime", scale="tai")

    def get_replica_chunks(self, start_chunk_id: int | None = None) -> Sequence[PpdbReplicaChunk] | None:
        # docstring is inherited from a base class
        return self.get_replica_chunks_ext(start_chunk_id=start_chunk_id)

    def get_replica_chunks_ext(
        self, status: ChunkStatus | None = None, start_chunk_id: int | None = None
    ) -> Sequence[PpdbReplicaChunkExtended]:
        """Find replica chunks having the specified status with the option to
        start from a specific chunk ID.

        If neither argument is provided, all chunks are returned.

        Parameters
        ----------
        status : `ChunkStatus`
            Status of the replica chunks to return.
        start_chunk_id : `int`, optional
            If provided, only return chunks with ID greater than or equal to
            this value.

        Returns
        -------
        chunks : `list` [ `PpdbReplicaChunkExtended` ]
            List of chunks with the specified status. Chunks are ordered by
            their ``last_update_time`` and include the ``directory`` and
            ``status`` fields.
        """
        table = self.replica_chunk_table
        query = sql.select(
            table.columns["apdb_replica_chunk"],
            table.columns["last_update_time"],
            table.columns["unique_id"],
            table.columns["replica_time"],
            table.columns["status"],  # Extended column
            table.columns["directory"],  # Extended column
        ).order_by(table.columns["last_update_time"])
        if start_chunk_id is not None:
            query = query.where(table.columns["apdb_replica_chunk"] >= start_chunk_id)
        if status is not None:
            query = query.where(table.columns["status"] == status.value)
        ids: list[PpdbReplicaChunkExtended] = []
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
            for row in result:
                last_update_time = self._to_astropy_tai(row[1])
                replica_time = self._to_astropy_tai(row[3])
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

    def store_chunk(self, replica_chunk: PpdbReplicaChunkExtended, update: bool) -> None:
        """Insert or replace single record in PpdbReplicaChunk table, including
        the status and directory of the replica chunk.

        Parameters
        ----------
        replica_chunk : `PpdbReplicaChunkExtended`
            The replica chunk to store.
        update : `bool`
            If `True` then perform an UPSERT operation to update existing
            records. If `False` then only INSERT is performed and an error is
            raised if the record already exists.
        """
        with self._engine.begin() as connection:
            table = self.replica_chunk_table

            row = {
                "apdb_replica_chunk": replica_chunk.id,
                "last_update_time": replica_chunk.last_update_time_dt_utc,
                "unique_id": replica_chunk.unique_id,
                "replica_time": replica_chunk.replica_time_dt_utc,
                "status": replica_chunk.status,
                "directory": str(replica_chunk.directory),
            }
            if update:
                PpdbSqlUtils.upsert(connection, table, row, "apdb_replica_chunk")
            else:
                insert = table.insert()
                connection.execute(insert, row)

    @classmethod
    def init_database(
        cls,
        config: PpdbSqlConfig,
        drop: bool = False,
    ) -> None:
        """Initialize the database by creating the necessary tables.

        Parameters
        ----------
        config : `PpdbBigQueryConfig`
            Configuration for the PPDB.
        drop : `bool`, optional
            If `True` then drop existing tables before creating new ones.
        """
        sa_metadata, version = PpdbSqlUtils.read_schema(
            config, include_tables=[PpdbSqlUtils.metadata_table_name]
        )
        PpdbSqlUtils.make_database(config, sa_metadata, version, drop=drop)
