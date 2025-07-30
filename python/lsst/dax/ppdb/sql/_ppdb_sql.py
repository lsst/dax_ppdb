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

import datetime
import logging
import os
import sqlite3
from collections.abc import MutableMapping
from contextlib import closing
from typing import Any

import astropy.time
import felis.datamodel
import sqlalchemy
import yaml
from felis.datamodel import Schema as FelisSchema
from lsst.dax.apdb import (
    ApdbMetadata,
    ApdbTableData,
    IncompatibleVersionError,
    ReplicaChunk,
    VersionTuple,
    monitor,
    schema_model,
)
from lsst.dax.apdb.sql import ApdbMetadataSql, ModelToSql
from lsst.dax.apdb.timer import Timer
from lsst.resources import ResourcePath
from lsst.utils.iteration import chunk_iterable
from sqlalchemy import sql
from sqlalchemy.pool import NullPool

from ..config import PpdbConfig
from ..ppdb import ChunkStatus, Ppdb, PpdbReplicaChunk
from .bulk_insert import make_inserter

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


VERSION = VersionTuple(0, 1, 1)
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

        self._sa_metadata, schema_version = self._read_schema(
            config.felis_path, config.schema_name, config.felis_schema, config.db_url
        )

        self._engine = self._make_engine(config)
        sa_metadata = sqlalchemy.MetaData(schema=config.schema_name)

        meta_table = sqlalchemy.schema.Table("metadata", sa_metadata, autoload_with=self._engine)
        self._metadata = ApdbMetadataSql(self._engine, meta_table)

        # Check schema version compatibility
        self._versionCheck(self._metadata, schema_version)

        # Check if schema uses MJD TAI for timestamps (DM-52215).
        self._use_mjd_tai = False
        for table in self._sa_metadata.tables.values():
            if table.name == "DiaObject":
                self._use_mjd_tai = "validityStartMjdTai" in table.columns

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
        sa_metadata, schema_version = cls._read_schema(schema_file, schema_name, felis_schema, db_url)
        config = PpdbSqlConfig(
            db_url=db_url,
            schema_name=schema_name,
            felis_path=schema_file,
            felis_schema=felis_schema,
            use_connection_pool=use_connection_pool,
            isolation_level=isolation_level,
            connection_timeout=connection_timeout,
        )
        cls._make_database(config, sa_metadata, schema_version, drop)
        return config

    @classmethod
    def _read_schema(
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

        # In case we use APDB schema drop tables that are not needed in PPDB.
        filtered_tables = [
            table for table in schema_dict["tables"] if table["name"] not in ("DiaObjectLast",)
        ]
        schema_dict["tables"] = filtered_tables
        dm_schema: FelisSchema = felis.datamodel.Schema.model_validate(schema_dict)
        schema = schema_model.Schema.from_felis(dm_schema)

        # Replace schema name with a configured one, just in case it may be
        # used by someone.
        if schema_name:
            schema.name = schema_name

        # Add replica chunk table.
        table_name = "PpdbReplicaChunk"
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
        schema.tables.append(chunks_table)

        if schema.version is not None:
            version = VersionTuple.fromString(schema.version.current)
        else:
            # Missing schema version is identical to 0.1.0
            version = VersionTuple(0, 1, 0)

        metadata = sqlalchemy.schema.MetaData(schema=schema_name)

        converter = ModelToSql(metadata=metadata)
        converter.make_tables(schema.tables)

        # Check if schema uses MJD TAI for timestamps (DM-52215). This is not
        # super-efficient, but I do not want to improve dax_apdb at this point.
        use_mjd_tai = False
        for schema_table in schema.tables:
            if schema_table.name == "DiaObject":
                for column in schema_table.columns:
                    if column.name == "validityStartMjdTai":
                        use_mjd_tai = True
                        break
                break

        if use_mjd_tai:
            validity_end_column = "validityEndMjdTai"
        else:
            validity_end_column = "validityEnd"

        # Add an additional index to DiaObject table to speed up replication.
        # This is a partial index (Postgres-only), we do not have support for
        # partial indices in ModelToSql, so we have to do it using sqlalchemy.
        url = sqlalchemy.engine.make_url(db_url)
        if url.get_backend_name() == "postgresql":
            table: sqlalchemy.schema.Table | None = None
            for table in metadata.tables.values():
                if table.name == "DiaObject":
                    name = f"IDX_DiaObject_diaObjectId_{validity_end_column}_IS_NULL"
                    sqlalchemy.schema.Index(
                        name,
                        table.columns["diaObjectId"],
                        postgresql_where=table.columns[validity_end_column].is_(None),
                    )
                    break
            else:
                # Cannot find table, odd, but what do I know.
                pass

        return metadata, version

    @classmethod
    def _make_database(
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
        engine = cls._make_engine(config)

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

    def _versionCheck(self, metadata: ApdbMetadataSql, schema_version: VersionTuple) -> None:
        """Check schema version compatibility."""

        def _get_version(key: str) -> VersionTuple:
            """Retrieve version number from given metadata key."""
            version_str = metadata.get(key)
            if version_str is None:
                # Should not happen with existing metadata table.
                raise RuntimeError(f"Version key {key!r} does not exist in metadata table.")
            return VersionTuple.fromString(version_str)

        db_schema_version = _get_version(self.meta_schema_version_key)
        db_code_version = _get_version(self.meta_code_version_key)

        # For now there is no way to make read-only APDB instances, assume that
        # any access can do updates.
        if not schema_version.checkCompatibility(db_schema_version):
            raise IncompatibleVersionError(
                f"Configured schema version {schema_version} "
                f"is not compatible with database version {db_schema_version}"
            )
        if not VERSION.checkCompatibility(db_code_version):
            raise IncompatibleVersionError(
                f"Current code version {VERSION} "
                f"is not compatible with database version {db_code_version}"
            )

    def _get_table(self, name: str) -> sqlalchemy.schema.Table:
        for table in self._sa_metadata.tables.values():
            if table.name == name:
                return table
        raise LookupError(f"Unknown table {name}")

    @property
    def metadata(self) -> ApdbMetadata:
        # docstring is inherited from a base class
        return self._metadata

    def get_replica_chunks(self, start_chunk_id: int | None = None) -> list[PpdbReplicaChunk] | None:
        # docstring is inherited from a base class
        table = self._get_table("PpdbReplicaChunk")
        query = sql.select(
            table.columns["apdb_replica_chunk"],
            table.columns["last_update_time"],
            table.columns["unique_id"],
            table.columns["replica_time"],
            table.columns["status"],
            table.columns["directory"],
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
                        status=row[4],
                    )
                )
            return ids

    def store(
        self,
        replica_chunk: ReplicaChunk,
        objects: ApdbTableData,
        sources: ApdbTableData,
        forced_sources: ApdbTableData,
        *,
        update: bool = False,
    ) -> None:
        # docstring is inherited from a base class

        # We want to run all inserts in one transaction.
        with self._engine.begin() as connection:
            # Check for existing InsertId first, if it does not exist we can
            # run more optimal queries.
            if update:
                table = self._get_table("PpdbReplicaChunk")
                query = sql.select(sql.expression.literal(1)).where(
                    table.columns["apdb_replica_chunk"] == replica_chunk.id
                )
                if connection.execute(query).one_or_none() is None:
                    update = False

            self._store_insert_id(replica_chunk, connection, update)
            self._store_objects(objects, connection, update)
            self._store_table_data(sources, connection, update, "DiaSource", 100)
            self._store_table_data(forced_sources, connection, update, "DiaForcedSource", 1000)

    def _store_insert_id(
        self,
        replica_chunk: ReplicaChunk,
        connection: sqlalchemy.engine.Connection,
        update: bool,
        status: ChunkStatus | None = None,
    ) -> None:
        """Insert or replace single record in PpdbReplicaChunk table"""
        # `astropy.Time.datetime` returns naive datetime, even though all
        # astropy times are in UTC. Add UTC timezone to timestampt so that
        # database can store a correct value.
        insert_dt = datetime.datetime.fromtimestamp(
            replica_chunk.last_update_time.unix_tai, tz=datetime.timezone.utc
        )
        now = datetime.datetime.fromtimestamp(astropy.time.Time.now().unix_tai, tz=datetime.timezone.utc)

        table = self._get_table("PpdbReplicaChunk")

        values = {"last_update_time": insert_dt, "unique_id": replica_chunk.unique_id, "replica_time": now}
        if status is not None:
            values["status"] = status.value
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

    def _store_objects(
        self, objects: ApdbTableData, connection: sqlalchemy.engine.Connection, update: bool
    ) -> None:
        """Store or replace DiaObjects."""
        # Store all records.
        self._store_table_data(objects, connection, update, "DiaObject", 100)

        table = self._get_table("DiaObject")

        if self._use_mjd_tai:
            validity_start_column = "validityStartMjdTai"
            validity_end_column = "validityEndMjdTai"
        else:
            validity_start_column = "validityStart"
            validity_end_column = "validityEnd"

        with Timer("update_validity_time", _MON, tags={"table": table.name}) as timer:
            # We need to fill validityEnd column for the previously stored
            # objects that have new records. Window function is used here to
            # find records with validityEnd=NULL, order them and update
            # validityEnd of older records from validityStart of newer records.
            idx = objects.column_names().index("diaObjectId")
            ids = sorted(set(row[idx] for row in objects.rows()))
            count = 0
            for chunk in chunk_iterable(ids, 1000):
                select_cte = sqlalchemy.cte(
                    sqlalchemy.select(
                        table.columns["diaObjectId"],
                        table.columns[validity_start_column],
                        table.columns[validity_end_column],
                        sqlalchemy.func.rank()
                        .over(
                            partition_by=table.columns["diaObjectId"],
                            order_by=table.columns[validity_start_column],
                        )
                        .label("rank"),
                    ).where(
                        sqlalchemy.and_(
                            table.columns["diaObjectId"].in_(chunk),
                            table.columns[validity_end_column] == None,  # noqa: E711
                        )
                    )
                )
                sub1 = select_cte.alias("s1")
                sub2 = select_cte.alias("s2")
                new_end = sql.select(sub2.columns[validity_start_column]).select_from(
                    sub1.join(
                        sub2,
                        sqlalchemy.and_(
                            sub1.columns["diaObjectId"] == sub2.columns["diaObjectId"],
                            sub1.columns["rank"] + sqlalchemy.literal(1) == sub2.columns["rank"],
                            sub1.columns["diaObjectId"] == table.columns["diaObjectId"],
                            sub1.columns[validity_start_column] == table.columns[validity_start_column],
                        ),
                    )
                )
                stmt = (
                    table.update()
                    .values(**{validity_end_column: new_end.scalar_subquery()})
                    .where(
                        sqlalchemy.and_(
                            table.columns["diaObjectId"].in_(chunk),
                            table.columns[validity_end_column] == None,  # noqa: E711
                        )
                    )
                )
                result = connection.execute(stmt)
                count += result.rowcount

            timer.add_values(row_count=count)
            _LOG.info("Updated %d rows in DiaObject table with new validityEnd values", count)

    def _store_table_data(
        self,
        table_data: ApdbTableData,
        connection: sqlalchemy.engine.Connection,
        update: bool,
        table_name: str,
        chunk_size: int,
    ) -> None:
        """Store or replace DiaSources."""
        with Timer("store_data_time", _MON, tags={"table": table_name}) as timer:
            table = self._get_table(table_name)
            inserter = make_inserter(connection)
            count = inserter.insert(table, table_data, chunk_size=chunk_size)
            timer.add_values(row_count=count)
            _LOG.info("Inserted %d rows into %s table", count, table_name)
