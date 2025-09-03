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
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path

import astropy.time
import felis
import sqlalchemy
from lsst.dax.apdb import ApdbMetadata, ReplicaChunk, VersionTuple, schema_model
from lsst.dax.apdb.sql import ApdbMetadataSql, ModelToSql
from sqlalchemy import Table, sql

from ..ppdb import PpdbReplicaChunk
from ..sql._ppdb_sql import PpdbSql
from ._config import PpdbBigQueryConfig


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
    """Status of the replica chunk."""

    directory: Path
    """Directory where the exported replica chunk data is stored."""

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


_DEFAULT_VERSION = VersionTuple(0, 1, 0)


class PpdbReplicaChunkSql:
    """Implementation of `Ppdb` to provide only replica chunk management
    functionality, using a SQL database as the backend.

    This is needed by the data processing pipeline for BigQuery to manage the
    export and upload of replica chunks.
    """

    def __init__(self, config: PpdbBigQueryConfig):
        # Set the config
        if config.schema_name is None:
            raise ValueError("schema_name must be provided in config")
        self._config = config

        # Create the SQLAlchemy engine and metadata
        self._engine = PpdbSql._make_engine(config)
        self._sa_metadata = sqlalchemy.MetaData(schema=self._config.schema_name)

        # Initialize the APDB metadata table
        self._init_apdb_metadata()

        # Read the APDB schema to get its version
        self._read_schema()

        # Create the replica chunk table
        self._init_replica_chunk_table()

    @property
    def schema_version(self) -> VersionTuple:
        """Version of the APDB schema used by this PPDB (`VersionTuple`)."""
        return self._schema_version

    @property
    def replica_chunk_table(self) -> Table:
        """The `~sqlalchemy.Table` used to track replica chunks."""
        return self._get_table(self._config.replica_chunk_table)

    @property
    def metadata(self) -> ApdbMetadata:
        """The `ApdbMetadata` object for satisfying the `Ppdb` interface."""
        return self._apdb_metadata

    @classmethod
    def _get_schema_version(cls, schema: schema_model.Schema) -> VersionTuple:
        return VersionTuple.fromString(schema.version.current) if schema.version else _DEFAULT_VERSION

    def _init_apdb_metadata(self) -> ApdbMetadataSql:
        meta_table = sqlalchemy.schema.Table("metadata", self._sa_metadata, autoload_with=self._engine)
        self._apdb_metadata = ApdbMetadataSql(self._engine, meta_table)

    def _read_schema(self) -> None:
        felis_schema = felis.Schema.from_uri(self._config.apdb_schema_uri)
        schema = schema_model.Schema.from_felis(felis_schema)
        self._schema_version = self._get_schema_version(schema)

    def _init_replica_chunk_table(self) -> None:
        converter = ModelToSql(metadata=self._sa_metadata)
        replica_chunk_table = self._create_replica_chunk_table()
        converter.make_tables([replica_chunk_table])

    def _get_table(self, name: str) -> sqlalchemy.schema.Table:
        for table in self._sa_metadata.tables.values():
            if table.name == name:
                return table
        raise LookupError(f"Unknown table {name}")

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
        This adds the ``status`` and ``directory`` columns to the standard
        ``PpdbReplicaChunk`` table definition.
        """
        # Create the base table
        replica_chunk_table = PpdbSql.create_replica_chunk_table()

        # Extend the table with additional columns
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
            List of chunks with the specified status. Chunks are ordered by
            their ``last_update_time``.

        Notes
        -----
        This is an alternative to `get_replica_chunks` that allows retrieving
        chunks by their status. The original method is left for backward
        compatibility, as it is used internally by the replication process.
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

    def get_replica_chunks(self, start_chunk_id: int | None = None) -> list[PpdbReplicaChunk] | None:
        # docstring is inherited from a base class
        table = self.replica_chunk_table
        query = sql.select(
            table.columns["apdb_replica_chunk"],
            table.columns["last_update_time"],
            table.columns["unique_id"],
            table.columns["replica_time"],
        ).order_by(table.columns["last_update_time"])
        if start_chunk_id is not None:
            query = query.where(table.columns["apdb_replica_chunk"] >= start_chunk_id)
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
            ids = []
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
                    )
                )
            return ids

    def store_chunk(self, replica_chunk: PpdbReplicaChunkExtended, update: bool) -> None:
        """Insert or replace single record in PpdbReplicaChunk table, including
        the status and directory of the replica chunk.
        """
        # DM-52173: This method was copied and modified from the
        # ``_store_insert_id_`` method in `PpdbSql`. Refactoring should be done
        # to avoid this code duplication.
        with self._engine.begin() as connection:
            table = self.replica_chunk_table

            values = {
                "last_update_time": replica_chunk.last_update_time_dt_utc,
                "unique_id": replica_chunk.unique_id,
                "replica_time": replica_chunk.replica_time_dt_utc,
                "status": replica_chunk.status,
                "directory": str(replica_chunk.directory),
            }
            row = {"apdb_replica_chunk": replica_chunk.id} | values
            if update:
                # We need UPSERT which is dialect-specific construct
                if connection.dialect.name == "sqlite":
                    insert_sqlite = sqlalchemy.dialects.sqlite.insert(table)
                    insert_sqlite = insert_sqlite.on_conflict_do_update(
                        index_elements=table.primary_key, set_=values
                    )
                    connection.execute(insert_sqlite, row)
                elif connection.dialect.name == "postgresql":
                    insert_pg = sqlalchemy.dialects.postgresql.dml.insert(table)
                    insert_pg = insert_pg.on_conflict_do_update(constraint=table.primary_key, set_=values)
                    connection.execute(insert_pg, row)
                else:
                    raise TypeError(f"Unsupported dialect {connection.dialect.name} for upsert.")
            else:
                insert = table.insert()
                connection.execute(insert, row)
