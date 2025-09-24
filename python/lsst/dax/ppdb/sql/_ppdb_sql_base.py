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

__all__ = ["PpdbSqlBase"]

import logging
import os
import sqlite3
from collections.abc import Iterable, MutableMapping
from contextlib import closing
from typing import Any

import astropy
import felis.datamodel
import sqlalchemy
import yaml
from felis.datamodel import Schema as FelisSchema
from lsst.dax.apdb import (
    IncompatibleVersionError,
    VersionTuple,
    schema_model,
)
from lsst.dax.apdb.sql import ApdbMetadataSql, ModelToSql
from lsst.dax.ppdb.ppdb import PpdbConfig
from lsst.resources import ResourcePath
from pydantic import BaseModel
from sqlalchemy.pool import NullPool

_LOG = logging.getLogger(__name__)


class MissingSchemaVersionError(RuntimeError):
    """Exception raised when schema version is not defined in the schema."""

    def __init__(self, schema_name: str):
        super().__init__(f"Version is missing from the '{schema_name}' schema.")


class PpdbSqlBaseConfig(BaseModel):
    """SQL configuration for the PPDB."""

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


def _onSqlite3Connect(
    dbapiConnection: sqlite3.Connection, connectionRecord: sqlalchemy.pool._ConnectionRecord
) -> None:
    # Enable foreign keys
    with closing(dbapiConnection.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys=ON;")


class PpdbSqlBase:
    """Abstract base class for SQL-based PPDB implementations.

    Parameters
    ----------
    config : `PpdbSqlBaseConfig`
        Configuration object.

    Notes
    -----
    This class does not implement the `~lsst.dax.ppdb.ppdb.Ppdb` interface.
    Sub-classes which need to do that should use multiple inheritance.
    """

    default_felis_schema_file = "${SDM_SCHEMAS_DIR}/yml/apdb.yaml"
    """Default location of the YAML file defining APDB schema."""

    meta_schema_version_key = "version:schema"
    """Name of the metadata key to store Felis schema version number."""

    def __init__(self, config: PpdbSqlBaseConfig) -> None:
        self._sa_metadata, self._schema_version = self.read_schema(
            config.felis_path, config.schema_name, config.felis_schema, config.db_url
        )

        self._engine = self.make_engine(config)
        sa_metadata = sqlalchemy.MetaData(schema=config.schema_name)

        meta_table = sqlalchemy.schema.Table("metadata", sa_metadata, autoload_with=self._engine)
        self._metadata = ApdbMetadataSql(self._engine, meta_table)

        # Check schema amd code version compatibility.
        self._check_schema_version(self._schema_version)
        self._check_code_version()

    @classmethod
    def make_engine(cls, config: PpdbSqlBaseConfig) -> sqlalchemy.engine.Engine:
        """Make SQLALchemy engine based on configured parameters.

        Parameters
        ----------
        config : `PpdbSqlBaseConfig`
            Configuration object with SQL parameters.
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
    def init_database(
        cls,
        db_url: str,
        schema_file: str | None = None,
        schema_name: str | None = None,
        felis_schema: str | None = None,
        use_connection_pool: bool = True,
        isolation_level: str | None = None,
        connection_timeout: float | None = None,
        drop: bool = False,
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
        # FIXME: This method should not be polymorphic. Sub-classes should
        # manage their own initialization. (DM-52460)
        sa_metadata, schema_version = cls.read_schema(schema_file, schema_name, felis_schema, db_url)
        config = cls.make_sql_config(
            db_url=db_url,
            schema_name=schema_name,
            felis_path=schema_file,
            felis_schema=felis_schema,
            use_connection_pool=use_connection_pool,
            isolation_level=isolation_level,
            connection_timeout=connection_timeout,
        )
        cls.make_database(config, sa_metadata, schema_version, drop)
        return config

    @classmethod
    def make_sql_config(
        cls,
        db_url: str,
        schema_name: str | None = None,
        felis_path: str | None = None,
        felis_schema: str | None = None,
        use_connection_pool: bool = True,
        isolation_level: str | None = None,
        connection_timeout: float | None = None,
    ) -> PpdbSqlBaseConfig:
        """Create a `PpdbSqlBaseConfig` object.

        Parameters
        ----------
        db_url : `str`
            SQLAlchemy database connection URI.
        schema_name : `str` or `None`
            Database schema name, if `None` then default schema is used.
        felis_path : `str` or `None`
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
        """
        # FIXME: This method should not be polymorphic. Sub-classes should
        # manage their own initialization. (DM-52460)
        return PpdbSqlBaseConfig(
            db_url=db_url,
            schema_name=schema_name,
            felis_path=felis_path,
            felis_schema=felis_schema,
            use_connection_pool=use_connection_pool,
            isolation_level=isolation_level,
            connection_timeout=connection_timeout,
        )

    @classmethod
    def make_database(
        cls,
        config: PpdbSqlBaseConfig,
        sa_metadata: sqlalchemy.schema.MetaData,
        schema_version: VersionTuple,
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

        # Need metadata table to store few items in it, if table exists.
        meta_table: sqlalchemy.schema.Table
        for table in sa_metadata.tables.values():
            if table.name == "metadata":
                meta_table = table
                break
        else:
            raise LookupError("Metadata table does not exist.")

        apdb_meta = ApdbMetadataSql(engine, meta_table)

        # Store schema and code version in metadata table.
        cls._set_apdb_meta_value(apdb_meta, cls.meta_schema_version_key, str(schema_version))
        cls._set_apdb_meta_value(apdb_meta, cls.get_meta_code_version_key(), str(cls.get_code_version()))

    @classmethod
    def _set_apdb_meta_value(cls, metadata: ApdbMetadataSql, key: str, value: str) -> None:
        """Set a metadata key/value pair in the APDB metadata table.

        Parameters
        ----------
        metadata : `ApdbMetadataSql`
            Metadata table object.
        key : `str` or `None`
            Metadata key.
        value : `str` or `None`
            Metadata value.

        Notes
        -----
        The function signature allows `None` values for key and value because
        sub-classes may fail to override the methods to provide them. We check
        for `None` values and raise if they are not set.
        """
        _LOG.info("Store metadata %s = %s", key, value)
        metadata.set(key, value, force=True)

    @classmethod
    def get_meta_code_version_key(cls) -> str:
        """Name of the metadata key to store the code version number.

        Returns
        -------
        key : `str`
            Name of the metadata key for storing the code version.

        Raises
        ------
        NotImplementedError
            Raised if the method is not overridden by sub-classes.
        """
        raise NotImplementedError()

    @classmethod
    def get_code_version(cls) -> VersionTuple:
        """Get the current version of the code.

        Returns
        -------
        version : `~lsst.dax.apdb.VersionTuple`
            Current version of the code.

        Raises
        ------
        NotImplementedError
            Raised if the method is not overridden by sub-classes.
        """
        raise NotImplementedError()

    def _get_apdb_meta_version(self, version_key: str) -> VersionTuple:
        """Get a metadata version value from the APDB metadata table.

        Parameters
        ----------
        key : `str`
            Metadata key.

        Returns
        -------
        value : `str` or `None`
            Metadata value or `None` if key does not exist.
        """
        version_str = self._metadata.get(version_key)
        if version_str is None:
            raise RuntimeError(f"Version key {version_key!r} does not exist in metadata table.")
        return VersionTuple.fromString(version_str)

    def _check_schema_version(self, expected_version: VersionTuple) -> None:
        """Check that the schema version in the database is compatible with
        the expected version that we have read from the Felis YAML file.

        Parameters
        ----------
        expected_version : `lsst.dax.apdb.VersionTuple`
            Expected schema version.

        Raises
        ------
        IncompatibleVersionError
            Raised if the schema version in the database is not compatible
            with the expected version.
        """
        db_schema_version = self._get_apdb_meta_version(self.meta_schema_version_key)
        if not expected_version.checkCompatibility(db_schema_version):
            raise IncompatibleVersionError(
                f"Configured schema version {expected_version} "
                f"is not compatible with database version {db_schema_version}"
            )

    def _check_code_version(self) -> None:
        """Check that the code (module) version is compatible with the version
        in the database.

        Raises
        ------
        IncompatibleVersionError
            Raised if the code version is not compatible with the version in
            the database.
        RuntimeError
            Raised if code version or code version key is not defined.
        """
        db_code_version = self._get_apdb_meta_version(self.get_meta_code_version_key())
        code_version = self.get_code_version()
        if not code_version.checkCompatibility(db_code_version):
            raise IncompatibleVersionError(
                f"Current code version {code_version} is incompatible with database version {db_code_version}"
            )

    @classmethod
    def read_schema(
        cls, schema_file: str | None, schema_name: str | None, felis_schema: str | None, db_url: str
    ) -> tuple[sqlalchemy.schema.MetaData, VersionTuple]:
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
        db_url : `str`
            Database URL.

        Returns
        -------
        metadata : `sqlalchemy.schema.MetaData`
            SQLAlchemy metadata instance containing information for all tables.
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

        # Filter out unwanted tables. By default, all tables will be included.
        table_names = [table["name"] for table in schema_dict.get("tables", [])]
        filtered_table_names = list(cls.filter_table_names(table_names))
        schema_dict["tables"] = [
            table for table in schema_dict.get("tables", []) if table["name"] in filtered_table_names
        ]

        dm_schema: FelisSchema = felis.datamodel.Schema.model_validate(schema_dict)
        schema = schema_model.Schema.from_felis(dm_schema)

        # Replace schema name with a configured one, just in case it may be
        # used by someone.
        if schema_name:
            schema.name = schema_name

        # Create the PpdbReplicaChunk table and add it to the schema.
        replica_chunk_table = cls.create_replica_chunk_table()
        schema.tables.append(replica_chunk_table)

        if schema.version is not None:
            version = VersionTuple.fromString(schema.version.current)
        else:
            raise MissingSchemaVersionError(schema.name)

        metadata = sqlalchemy.schema.MetaData(schema=schema_name)

        converter = ModelToSql(metadata=metadata)
        converter.make_tables(schema.tables)

        return metadata, version

    @classmethod
    def create_replica_chunk_table(cls, table_name: str | None = None) -> schema_model.Table:
        """Create the ``PpdbReplicaChunk`` table which will be added to the
        schema.

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

    def get_table(self, name: str) -> sqlalchemy.schema.Table:
        for table in self._sa_metadata.tables.values():
            if table.name == name:
                return table
        raise LookupError(f"Unknown table {name}")

    @property
    def schema_version(self) -> VersionTuple:
        """Version of the APDB database schema (`VersionTuple`)."""
        return self._schema_version

    @classmethod
    def filter_table_names(cls, original_table_names: Iterable[str]) -> Iterable[str]:
        """Return list of filtered table names needed for this PPDB which by
        default returns the original list.

        Parameters
        ----------
        original_table_names : list[ `str` ]
            List of table names from the schema on which is filter.

        Returns
        -------
        tables : `list` [`Any`]
            List of tables from ``schema_dict`` on which to filter.

        Notes
        -----
        The default implementation does not filter any tables. Sub-classes
        may override this method to filter out unwanted tables.
        """
        return original_table_names

    @classmethod
    def to_astropy_tai(cls, obj: Any) -> astropy.time.Time:
        """Convert a database object to `astropy.time.Time` in TAI scale.

        Parameters
        ----------
        obj : `Any`
            The object to convert, expected to be a `datetime` in UTC.
            The type signature is generic to match astropy's typing.
        """
        return astropy.time.Time(obj, format="datetime", scale="tai")

    @classmethod
    def upsert(
        cls,
        connection: sqlalchemy.engine.Connection,
        table: sqlalchemy.schema.Table,
        row: dict[str, Any],
        key_column_name: str,
    ) -> None:
        """Perform an UPSERT operation on the given table.

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
            Raised if the database dialect does not support UPSERT.
        """
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
