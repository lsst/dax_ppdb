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

__all__ = ("PpdbSqlUtils",)

import logging
import sqlite3
from collections.abc import MutableMapping
from contextlib import closing
from typing import Any

import felis
import sqlalchemy
from lsst.dax.apdb import (
    VersionTuple,
    schema_model,
)
from lsst.dax.apdb.sql import ApdbMetadataSql, ModelToSql
from sqlalchemy.pool import NullPool

from .ppdb_sql_config import PpdbSqlConfig

_LOG = logging.getLogger(__name__)


def _onSqlite3Connect(
    dbapiConnection: sqlite3.Connection, connectionRecord: sqlalchemy.pool._ConnectionRecord
) -> None:
    # Enable foreign keys
    with closing(dbapiConnection.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys=ON;")


class PpdbSqlUtils:
    """Miscellaneous, standalone utility functions for PPDB SQL operations,
    primarily related to the ``PpdbReplicaChunk`` table.

    Notes
    -----
    These utilities were extracted from the ``PpdbSql`` class to avoid
    dependencies on it. That class was not modified to use these functions to
    avoid changing its behavior.
    """

    meta_schema_version_key = "version:schema"
    """Name of the metadata key to store schema version number (`str`)."""

    metadata_table_name = "metadata"
    """Name of the metadata table in the database (`str`)."""

    @classmethod
    def make_engine(cls, config: PpdbSqlConfig) -> sqlalchemy.engine.Engine:
        """Make SQLALchemy engine based on configured parameters.

        Parameters
        ----------
        config : `PpdbSqlConfig`
            Configuration object.
        """
        kw: MutableMapping[str, Any] = {}
        conn_args: dict[str, Any] = dict()
        if not config.use_connection_pool:
            kw["poolclass"] = NullPool
        if config.isolation_level is not None:
            kw.update(isolation_level=config.isolation_level)
        elif config.db_url.startswith("sqlite"):
            # Use READ_UNCOMMITTED as default value for sqlite.
            kw.update(isolation_level="READ_UNCOMMITTED")
        if config.connection_timeout is not None:
            if config.db_url.startswith("sqlite"):
                conn_args.update(timeout=config.connection_timeout)
            elif config.db_url.startswith(("postgresql", "mysql")):
                conn_args.update(connect_timeout=config.connection_timeout)
        kw = {"connect_args": conn_args}
        engine = sqlalchemy.create_engine(config.db_url, **kw)

        if engine.dialect.name == "sqlite":
            # Need to enable foreign keys on every new connection.
            sqlalchemy.event.listen(engine, "connect", _onSqlite3Connect)

        return engine

    @classmethod
    def make_database(
        cls,
        config: PpdbSqlConfig,
        sa_metadata: sqlalchemy.schema.MetaData,
        schema_version: VersionTuple | None,
        drop: bool,
    ) -> None:
        """Initialize database schema.

        Parameters
        ----------
        db_url : `str`
            SQLAlchemy database connection URI.
        schema_name : `str` or `None`
            Database schema name, if `None` then default schema is used.
        sa_metadata : `sqlalchemy.schema.MetaData`
            Schema definition.
        schema_version : `lsst.dax.apdb.VersionTuple` or `None`
            Schema version defined in schema or `None` if not defined.
        drop : `bool`
            If `True` then drop existing tables before creating new ones.
        """
        engine = cls.make_engine(config)

        if config.schema_name is not None:
            dialect = engine.dialect
            quoted_schema = dialect.preparer(dialect).quote_schema(config.schema_name)
            create_schema = sqlalchemy.DDL(
                "CREATE SCHEMA IF NOT EXISTS %(schema)s", context={"schema": quoted_schema}
            ).execute_if(dialect="postgresql")
            sqlalchemy.event.listen(sa_metadata, "before_create", create_schema)

        if drop:
            _LOG.info("dropping all tables")
            sa_metadata.drop_all(engine)
        _LOG.info("creating all tables")
        sa_metadata.create_all(engine)

        # Find the metadata table to use for storing schema version.
        meta_table = cls.find_table_by_name(sa_metadata, cls.metadata_table_name)

        apdb_meta = ApdbMetadataSql(engine, meta_table)
        # Fill schema version number in the metadata table, overwriting if it
        # already exists.
        if schema_version is not None:
            _LOG.info("Store metadata %s = %s", cls.meta_schema_version_key, schema_version)
            apdb_meta.set(cls.meta_schema_version_key, str(schema_version), force=True)

    @classmethod
    def make_replica_chunk_table(cls, table_name: str | None = None) -> schema_model.Table:
        """Create the default ``PpdbReplicaChunk`` table in its APDB
        `schema_model` representation.

        This includes fields for the chunk's status and the directory
        containing its exported data, both declared as nullable.

        Parameters
        ----------
        table_name : `str` or `None`
            Name of the table to create. If not provided, defaults to
            "PpdbReplicaChunk".
        """
        table_name = table_name or "PpdbReplicaChunk"
        columns = [
            schema_model.Column(
                name="apdb_replica_chunk",
                id=f"#{table_name}.apdb_replica_chunk",
                datatype=felis.datamodel.DataType.long,
            ),
            schema_model.Column(
                name="last_update_time",
                id=f"#{table_name}.last_update_time",
                datatype=felis.datamodel.DataType.timestamp,
                nullable=False,
            ),
            schema_model.Column(
                name="unique_id",
                id=f"#{table_name}.unique_id",
                datatype=schema_model.ExtraDataTypes.UUID,
                nullable=False,
            ),
            schema_model.Column(
                name="replica_time",
                id=f"#{table_name}.replica_time",
                datatype=felis.datamodel.DataType.timestamp,
                nullable=False,
            ),
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
        indices = [
            schema_model.Index(
                name="PpdbInsertId_idx_last_update_time",
                id="#PpdbInsertId_idx_last_update_time",
                columns=[columns[1]],
            ),
            schema_model.Index(
                name="PpdbInsertId_idx_replica_time",
                id="#PpdbInsertId_idx_replica_time",
                columns=[columns[3]],
            ),
        ]

        # Add table for replication support.
        chunks_table = schema_model.Table(
            name=table_name,
            id=f"#{table_name}",
            columns=columns,
            primary_key=[columns[0]],
            indexes=indices,
            constraints=[],
        )
        return chunks_table

    @classmethod
    def find_table_by_name(
        cls, sa_metadata: sqlalchemy.schema.MetaData, name: str
    ) -> sqlalchemy.schema.Table:
        """Find a table from SQLAlchemy metadata by name, without including the
        schema name.

        Parameters
        ----------
        sa_metadata : `sqlalchemy.schema.MetaData`
            Schema definition.
        name : `str`
            Name of the table to get.

        Returns
        -------
        table : `sqlalchemy.schema.Table`
            Table object.

        Raises
        ------
        LookupError
            If table with given name does not exist.
        """
        for table in sa_metadata.tables.values():
            if table.name == name:
                return table
        raise LookupError(f"Table does not exist: {name}")

    @classmethod
    def upsert(
        cls,
        connection: sqlalchemy.engine.Connection,
        table: sqlalchemy.schema.Table,
        row: dict,
        key_column_name: str,
    ) -> None:
        """Perform an UPSERT operation on the replica chunk table.

        Parameters
        ----------
        connection : `sqlalchemy.engine.Connection`
            Active database connection.
        table : `sqlalchemy.schema.Table`
            Table object for the replica chunk table.
        values : `dict`
            Dictionary of column values to insert or update.

        Raises
        ------
        TypeError
            If the database dialect does not support UPSERT.
        """
        if key_column_name not in row:
            raise KeyError(f"Key column {key_column_name} not in provided row.")
        values = {k: v for k, v in row.items() if k != key_column_name}
        if connection.dialect.name == "sqlite":
            insert_sqlite = sqlalchemy.dialects.sqlite.insert(table)
            insert_sqlite = insert_sqlite.on_conflict_do_update(index_elements=table.primary_key, set_=values)
            connection.execute(insert_sqlite, row)
        elif connection.dialect.name == "postgresql":
            insert_pg = sqlalchemy.dialects.postgresql.dml.insert(table)
            insert_pg = insert_pg.on_conflict_do_update(constraint=table.primary_key, set_=values)
            connection.execute(insert_pg, row)
        else:
            raise TypeError(f"Unsupported dialect {connection.dialect.name} for upsert.")

    @classmethod
    def read_schema(
        cls, config: PpdbSqlConfig, include_tables: list[str] | None = None
    ) -> tuple[sqlalchemy.schema.MetaData, VersionTuple]:
        """Read the APDB schema and returns its metadata and version.

        Parameters
        ----------
        config : `PpdbSqlConfig`
            Configuration object.
        include_tables : `list` of `str` or `None`
            If provided, only tables with names in this list will be included
            in the returned metadata. If `None`, all tables are included.
        """
        felis_schema = felis.Schema.from_uri(config.apdb_schema_uri)
        schema = schema_model.Schema.from_felis(felis_schema)

        # Determine the version of the schema.
        if schema.version is not None:
            version = VersionTuple.fromString(schema.version.current)
        else:
            raise ValueError(f"Schema version is not defined in {config.apdb_schema_uri}")

        # Keep only the requested tables, if any. Otherwise keep all tables.
        if include_tables is not None:
            schema.tables = [table for table in schema.tables if table.name in include_tables]

        # Add the PpdbReplicaChunk table to the schema.
        replica_chunk_table = PpdbSqlUtils.make_replica_chunk_table()
        schema.tables.append(replica_chunk_table)

        # Create SQA metadata from the schema.
        sa_metadata = sqlalchemy.MetaData(schema=config.schema_name)

        # Create the tables in the SQA metadata.
        converter = ModelToSql(metadata=sa_metadata)
        converter.make_tables(schema.tables)

        return (sa_metadata, version)
