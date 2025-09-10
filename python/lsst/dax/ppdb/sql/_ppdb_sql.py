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

import astropy.time
import sqlalchemy
from sqlalchemy import sql

from lsst.dax.apdb import (
    ApdbMetadata,
    ApdbTableData,
    ReplicaChunk,
    monitor,
)
from lsst.dax.apdb.timer import Timer
from lsst.utils.iteration import chunk_iterable

from ..ppdb import Ppdb, PpdbConfig, PpdbReplicaChunk
from ._base import SqlBase
from .bulk_insert import make_inserter
from .config import PpdbSqlConfig

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


class PpdbSql(Ppdb, SqlBase):
    """Implementation of `Ppdb` using a SQL database.

    Parameters
    ----------
    config : `PpdbSqlConfig`
        Configuration object, which must be of type `PpdbSqlConfig`.
    """

    def __init__(self, config: PpdbConfig) -> None:
        if type(config) is not PpdbSqlConfig:
            raise TypeError("config is not of type PpdbSqlConfig")
        SqlBase.__init__(self, config)

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

        values = {"last_update_time": insert_dt, "unique_id": replica_chunk.unique_id, "replica_time": now}
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
