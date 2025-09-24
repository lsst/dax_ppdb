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
from collections.abc import Iterable

import astropy.time
import sqlalchemy
from lsst.dax.apdb import (
    ApdbMetadata,
    ApdbTableData,
    ReplicaChunk,
    VersionTuple,
    monitor,
)
from lsst.dax.apdb.timer import Timer
from lsst.utils.iteration import chunk_iterable
from sqlalchemy import sql

from ..ppdb import Ppdb, PpdbConfig, PpdbReplicaChunk
from ._ppdb_sql_base import PpdbSqlBase, PpdbSqlBaseConfig
from .bulk_insert import make_inserter

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


VERSION = VersionTuple(0, 1, 1)
"""Version for the code defined in this module. This needs to be updated
(following compatibility rules) when schema produced by this code changes.
"""


class PpdbSqlConfig(PpdbConfig, PpdbSqlBaseConfig):
    """SQL configuration for the PPDB. This class is currently identical to
    `PpdbSqlBaseConfig`.
    """


class PpdbSql(Ppdb, PpdbSqlBase):
    """Implementation of `Ppdb` using a SQL database.

    Parameters
    ----------
    config : `PpdbSqlConfig`
        Configuration object with SQL database parameters.
    """

    def __init__(self, config: PpdbSqlConfig) -> None:
        PpdbSqlBase.__init__(self, config)

        # Check if schema uses MJD TAI for timestamps (DM-52215).
        self._use_mjd_tai = False
        for table in self._sa_metadata.tables.values():
            if table.name == "DiaObject":
                self._use_mjd_tai = "validityStartMjdTai" in table.columns

    @property
    def metadata(self) -> ApdbMetadata:
        # docstring is inherited from a base class
        return self._metadata

    def get_replica_chunks(self, start_chunk_id: int | None = None) -> list[PpdbReplicaChunk] | None:
        # docstring is inherited from a base class
        table = self.get_table("PpdbReplicaChunk")
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
                last_update_time = self.to_astropy_tai(row[1])
                replica_time = self.to_astropy_tai(row[3])
                ids.append(
                    PpdbReplicaChunk(
                        id=row[0],
                        last_update_time=last_update_time,
                        unique_id=row[2],
                        replica_time=replica_time,
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
                table = self.get_table("PpdbReplicaChunk")
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
        self, replica_chunk: ReplicaChunk, connection: sqlalchemy.engine.Connection, update: bool
    ) -> None:
        """Insert or replace single record in PpdbReplicaChunk table"""
        # `astropy.Time.datetime` returns naive datetime, even though all
        # astropy times are in UTC. Add UTC timezone to timestampt so that
        # database can store a correct value.
        insert_dt = datetime.datetime.fromtimestamp(
            replica_chunk.last_update_time.unix_tai, tz=datetime.timezone.utc
        )
        now = datetime.datetime.fromtimestamp(astropy.time.Time.now().unix_tai, tz=datetime.timezone.utc)

        table = self.get_table("PpdbReplicaChunk")

        row = {
            "apdb_replica_chunk": replica_chunk.id,
            "last_update_time": insert_dt,
            "unique_id": replica_chunk.unique_id,
            "replica_time": now,
        }
        if update:
            self.upsert(connection, table, row, "apdb_replica_chunk")
        else:
            insert = table.insert()
            connection.execute(insert, row)

    def _store_objects(
        self, objects: ApdbTableData, connection: sqlalchemy.engine.Connection, update: bool
    ) -> None:
        """Store or replace DiaObjects."""
        # Store all records.
        self._store_table_data(objects, connection, update, "DiaObject", 100)

        table = self.get_table("DiaObject")

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
            table = self.get_table(table_name)
            inserter = make_inserter(connection)
            count = inserter.insert(table, table_data, chunk_size=chunk_size)
            timer.add_values(row_count=count)
            _LOG.info("Inserted %d rows into %s table", count, table_name)

    @classmethod
    def read_schema(
        cls, schema_file: str | None, schema_name: str | None, felis_schema: str | None, db_url: str
    ) -> tuple[sqlalchemy.schema.MetaData, VersionTuple]:
        # Docstring is inherited from a base class.
        metadata, version = super().read_schema(schema_file, schema_name, felis_schema, db_url)

        # Check if schema uses MJD TAI for timestamps (DM-52215). This is not
        # super-efficient, but I do not want to improve dax_apdb at this point.
        use_mjd_tai = False
        for schema_table in metadata.tables.values():
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
    def filter_table_names(cls, original_table_names: Iterable[str]) -> Iterable[str]:
        # Docstring is inherited.
        return [table_name for table_name in original_table_names if table_name not in ("DiaObjectLast",)]

    @classmethod
    def get_meta_code_version_key(cls) -> str:
        # Docstring is inherited.
        return "version:PpdbSql"

    @classmethod
    def get_code_version(cls) -> VersionTuple:
        # Docstring is inherited.
        return VERSION

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
    ) -> PpdbSqlConfig:
        """Initialize PPDB SQL database.

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
        sa_metadata, schema_version = cls.read_schema(schema_file, schema_name, felis_schema, db_url)
        config = PpdbSqlConfig(
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
