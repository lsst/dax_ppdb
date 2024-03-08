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

__all__ = ["PpdbSql", "PpdbSqlConfig"]

import logging
import os
import sqlite3
from collections.abc import MutableMapping
from contextlib import closing, suppress
from typing import Any

import lsst.daf.base as dafBase
import sqlalchemy
import yaml
from felis import DEFAULT_FRAME
from felis.sql import Schema, SQLVisitor
from lsst.dax.apdb import ApdbMetadata, IncompatibleVersionError, VersionTuple
from lsst.dax.apdb.apdbMetadataSql import ApdbMetadataSql
from lsst.dax.apdb.apdbSqlSchema import GUID
from lsst.resources import ResourcePath
from sqlalchemy import sql
from sqlalchemy.pool import NullPool

from ..config import PpdbConfig
from ..ppdb import Ppdb, PpdbInsertId

_LOG = logging.getLogger(__name__)

VERSION = VersionTuple(0, 1, 0)
"""Version for the code defined in this module. This needs to be updated
(following compatibility rules) when schema produced by this code changes.
"""


def _onSqlite3Connect(
    dbapiConnection: sqlite3.Connection, connectionRecord: sqlalchemy.pool._ConnectionRecord
) -> None:
    # Enable foreign keys
    with closing(dbapiConnection.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys=ON;")


class PpdbSqlConfig(PpdbConfig):

    db_url: str
    """SQLAlchemy database connection URI."""

    schema_name: str | None = None
    """Database schema name, if `None` then default schema is used."""

    felis_path: str | None = None
    """Name of YAML file with ``felis`` schema, if `None` then default schema
    file is used.
    """

    felis_schema: str | None = None
    """Name of the schema in YAML file, if `None` then file has to contain
    single schema.
    """

    use_connection_pool: bool = True
    """If True then allow use of connection pool."""

    isolation_level: str | None = None
    """Transaction isolation level, if unset then backend-default value is
    used.
    """

    connection_timeout: float | None = None
    """Maximum connection timeout in seconds."""


class PpdbSql(Ppdb):

    default_felis_schema_file = "${SDM_SCHEMAS_DIR}/yml/apdb.yaml"

    meta_schema_version_key = "version:schema"
    """Name of the metadata key to store schema version number."""

    meta_code_version_key = "version:PpdbSql"
    """Name of the metadata key to store code version number."""

    def __init__(self, config: PpdbConfig):
        if not isinstance(config, PpdbSqlConfig):
            raise TypeError("Expecting PpdbSqlConfig instance")
        self.config = config

        self._schema, schema_version = self._read_schema(
            config.felis_path, config.schema_name, config.felis_schema
        )

        self._engine = self._make_engine(config)
        sa_metadata = sqlalchemy.MetaData(schema=config.schema_name)

        meta_table: sqlalchemy.schema.Table | None = None
        with suppress(sqlalchemy.exc.NoSuchTableError):
            meta_table = sqlalchemy.schema.Table("metadata", sa_metadata, autoload_with=self._engine)

        self._metadata = ApdbMetadataSql(self._engine, meta_table)

        # Check schema version compatibility
        if self._metadata.table_exists():
            self._versionCheck(self._metadata, schema_version)

    @classmethod
    def init_database(
        cls,
        db_url: str,
        schema_name: str | None,
        schema_file: str | None,
        felis_schema: str | None,
        use_connection_pool: bool,
        isolation_level: str | None,
        connection_timeout: float | None,
        drop: bool,
    ) -> PpdbConfig:
        """Initialize PPDB database.

        Parameters
        ----------
        db_url : `str`
            SQLAlchemy database connection URI.
        schema_name : `str` or `None`
            Database schema name, if `None` then default schema is used.
        schema_file : `str` or `None`
            Name of YAML file with ``felis`` schema, if `None` then default
            schema file is used.
        felis_schema : `str` or `None`
            Name of the schema in YAML file, if `None` then file has to contain
            single schema.
        use_connection_pool : `bool`
            If True then allow use of connection pool.
        isolation_level : `str` or `None`
            Transaction isolation level, if unset then backend-default value is
            used.
        connection_timeout: `float` or `None`
            Maximum connection timeout in seconds.
        drop : `bool`
            If `True` then drop existing tables.
        """
        ppdb_schema, schema_version = cls._read_schema(schema_file, schema_name, felis_schema)
        config = PpdbSqlConfig(
            db_url=db_url,
            schema_name=schema_name,
            felis_path=schema_file,
            felis_schema=felis_schema,
            use_connection_pool=use_connection_pool,
            isolation_level=isolation_level,
            connection_timeout=connection_timeout,
        )
        cls._make_database(config, ppdb_schema, schema_version, drop)
        return config

    @classmethod
    def _read_schema(
        cls, schema_file: str | None, schema_name: str | None, felis_schema: str | None
    ) -> tuple[Schema, VersionTuple]:
        """Read felis schema definitions for PPDB.

        Parameters
        ----------
        schema_file : `str` or `None`
            Name of YAML file with ``felis`` schema, if `None` then default
            schema file is used.
        schema_name : `str` or `None`
            Database schema name, if `None` then default schema is used.
        felis_schema : `str`, optional
            Name of the schema in YAML file, if `None` then file has to contain
            single schema.

        Returns
        -------
        schema : `felis.sql.Schema`
            Database schema in felis format.
        version : `lsst.dax.apdb.VersionTuple` or `None`
            Schema version defined in schema or `None` if not defined.
        """
        if schema_file is None:
            schema_file = os.path.expandvars(cls.default_felis_schema_file)

        res = ResourcePath(schema_file)
        schemas_list = list(yaml.load_all(res.read(), Loader=yaml.SafeLoader))
        if not schemas_list:
            raise ValueError(f"Schema file {schema_file!r} does not define any schema")
        if felis_schema is not None:
            schemas_list = [schema for schema in schemas_list if schema.get("name") == felis_schema]
            if not schemas_list:
                raise ValueError(f"Schema file {schema_file!r} does not define schema {felis_schema!r}")
        elif len(schemas_list) > 1:
            raise ValueError(f"Schema file {schema_file!r} defines multiple schemas")
        schema_dict = schemas_list[0]
        schema_dict.update(DEFAULT_FRAME)

        # In case we use APDB schema drop tables that are not needed in PPDB.
        filtered_tables = [
            table for table in schema_dict["tables"] if table["name"] not in ("DiaObjectLast",)
        ]
        schema_dict["tables"] = filtered_tables

        # Replace schema name with a configured one, this helps in case we
        # want to use default schema on database side.
        schema_dict["name"] = schema_name
        visitor = SQLVisitor(schema_name=schema_name)
        schema = visitor.visit_schema(schema_dict)
        if (version_str := schema_dict.get("version")) is not None:
            version = VersionTuple.fromString(version_str)
        else:
            # Missing schema version is identical to 0.1.0
            version = VersionTuple(0, 1, 0)

        extra_table = sqlalchemy.schema.Table(
            "PpdbInsertId",
            schema.metadata,
            sqlalchemy.schema.Column("insert_id", sqlalchemy.BigInteger, primary_key=True),
            sqlalchemy.schema.Column("insert_time", sqlalchemy.types.TIMESTAMP, nullable=False),
            sqlalchemy.schema.Column("unique_id", GUID, nullable=False),
            sqlalchemy.schema.Column("replica_time", sqlalchemy.types.TIMESTAMP, nullable=False),
            sqlalchemy.schema.Index("PpdbInsertId_idx_insert_time", "insert_time"),
            sqlalchemy.schema.Index("PpdbInsertId_idx_replica_time", "replica_time"),
            schema=schema_name,
        )
        schema.tables.append(extra_table)

        return schema, version

    @classmethod
    def _make_database(
        cls, config: PpdbSqlConfig, ppdb_schema: Schema, schema_version: VersionTuple | None, drop: bool
    ) -> None:
        """Initialize database schema.

        Parameters
        ----------
        db_url : `str`
            SQLAlchemy database connection URI.
        schema_name : `str` or `None`
            Database schema name, if `None` then default schema is used.
        ppdb_schema : `felis.sql.Schema`
            Schema definition.
        schema_version : `lsst.dax.apdb.VersionTuple` or `None`
            Schema version defined in schema or `None` if not defined.
        """
        engine = cls._make_engine(config)

        if config.schema_name is not None:
            dialect = engine.dialect
            quoted_schema = dialect.preparer(dialect).quote_schema(config.schema_name)
            create_schema = sqlalchemy.DDL(
                "CREATE SCHEMA IF NOT EXISTS %(schema)s", context={"schema": quoted_schema}
            ).execute_if(dialect="postgresql")
            sqlalchemy.event.listen(ppdb_schema.metadata, "before_create", create_schema)

        if drop:
            _LOG.info("dropping all tables")
            ppdb_schema.metadata.drop_all(engine)
        _LOG.info("creating all tables")
        ppdb_schema.metadata.create_all(engine)

        # Need metadata table to store few items in it, if table exists.
        meta_table: sqlalchemy.schema.Table | None = None
        for table in ppdb_schema.metadata.tables.values():
            if table.name == "metadata":
                meta_table = table
                break

        apdb_meta = ApdbMetadataSql(engine, meta_table)
        if apdb_meta.table_exists():
            # Fill version numbers, overwrite if they are already there.
            if schema_version is not None:
                _LOG.info("Store metadata %s = %s", cls.meta_schema_version_key, schema_version)
                apdb_meta.set(cls.meta_schema_version_key, str(schema_version), force=True)
            _LOG.info("Store metadata %s = %s", cls.meta_code_version_key, VERSION)
            apdb_meta.set(cls.meta_code_version_key, str(VERSION), force=True)

    @classmethod
    def _make_engine(cls, config: PpdbSqlConfig) -> sqlalchemy.engine.Engine:
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
        elif config.db_url.startswith("sqlite"):  # type: ignore
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

    def _versionCheck(self, metadata: ApdbMetadataSql, schema_version: VersionTuple) -> None:
        """Check schema version compatibility."""

        def _get_version(key: str, default: VersionTuple) -> VersionTuple:
            """Retrieve version number from given metadata key."""
            if metadata.table_exists():
                version_str = metadata.get(key)
                if version_str is None:
                    # Should not happen with existing metadata table.
                    raise RuntimeError(f"Version key {key!r} does not exist in metadata table.")
                return VersionTuple.fromString(version_str)
            return default

        # For old databases where metadata table does not exist we assume that
        # version of both code and schema is 0.1.0.
        initial_version = VersionTuple(0, 1, 0)
        db_schema_version = _get_version(self.meta_schema_version_key, initial_version)
        db_code_version = _get_version(self.meta_code_version_key, initial_version)

        # For now there is no way to make read-only APDB instances, assume that
        # any access can do updates.
        if not schema_version.checkCompatibility(db_schema_version, True):
            raise IncompatibleVersionError(
                f"Configured schema version {schema_version} "
                f"is not compatible with database version {db_schema_version}"
            )
        if not VERSION.checkCompatibility(db_code_version, True):
            raise IncompatibleVersionError(
                f"Current code version {VERSION} "
                f"is not compatible with database version {db_code_version}"
            )

    def _get_table(self, name: str) -> sqlalchemy.schema.Table:
        for table in self._schema.tables:
            if table.name == name:
                return table
        raise LookupError(f"Unknown table {name}")

    @property
    def metadata(self) -> ApdbMetadata:
        # docstring is inherited from a base class
        return self._metadata

    def get_insert_ids(self) -> list[PpdbInsertId] | None:
        # docstring is inherited from a base class
        table = self._get_table("PpdbInsertId")
        query = sql.select(
            table.columns["insert_id"],
            table.columns["insert_time"],
            table.columns["unique_id"],
            table.columns["replica_time"],
        ).order_by(table.columns["insert_time"])
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
            ids = []
            for row in result:
                insert_time = dafBase.DateTime(int(row[1].timestamp() * 1e9))
                replica_time = dafBase.DateTime(int(row[3].timestamp() * 1e9))
                ids.append(
                    PpdbInsertId(
                        id=row[0], insert_time=insert_time, unique_id=row[2], replica_time=replica_time
                    )
                )
            return ids
