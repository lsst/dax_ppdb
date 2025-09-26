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

__all__ = ["BulkInserter", "make_inserter"]

import logging
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

import sqlalchemy

from lsst.dax.apdb import ApdbTableData, monitor
from lsst.dax.apdb.timer import Timer
from lsst.utils.iteration import chunk_iterable

from .pg_dump import PgBinaryDumper

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


class BulkInserter(ABC):
    """Interface for bulk insert operations into a table."""

    @abstractmethod
    def insert(self, table: sqlalchemy.schema.Table, data: ApdbTableData, *, chunk_size: int = 1000) -> int:
        """Insert multiple rows into a single table.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table to insert data into.
        data : `ApdbTableData`
            Data to insert into the table.
        chunk_size : `int`, optional
            Number of rows for a single chunk for insertion.

        Returns
        -------
        count : `int`
            Total number of rows inserted.
        """
        raise NotImplementedError()


class _DefaultBulkInserter(BulkInserter):
    def __init__(self, connection: sqlalchemy.engine.Connection):
        self.connection = connection

    def insert(self, table: sqlalchemy.schema.Table, data: ApdbTableData, *, chunk_size: int = 1000) -> int:
        # Docstring inherited.
        table_columns = {column.name for column in table.columns}
        data_columns = set(data.column_names())
        drop_columns = data_columns - table_columns
        insert = table.insert()
        count = 0
        with Timer("pg_bulk_insert_time", _MON, tags={"table": table.name}) as timer:
            for chunk in chunk_iterable(data.rows(), chunk_size):
                insert_data = [self._row_to_dict(data.column_names(), row, drop_columns) for row in chunk]
                result = self.connection.execute(insert.values(insert_data))
                count += result.rowcount
            timer.add_values(row_count=count)
        return count

    @staticmethod
    def _row_to_dict(column_names: Sequence[str], row: tuple, drop_columns: set[str]) -> dict[str, Any]:
        """Convert TableData row into dict."""
        data = dict(zip(column_names, row, strict=True))
        for column in drop_columns:
            del data[column]
        return data


class _Psycopg2BulkInserter(BulkInserter):
    def __init__(self, connection: sqlalchemy.engine.Connection):
        self.connection = connection
        self.ident_prepare = sqlalchemy.sql.compiler.IdentifierPreparer(connection.dialect)

    def insert(self, table: sqlalchemy.schema.Table, data: ApdbTableData, *, chunk_size: int = 1000) -> int:
        # Docstring inherited.

        # To oavoid potential mismatch or ambiguity in column definitions I
        # want to reflect actual table definition from database.
        meta = sqlalchemy.schema.MetaData()
        reflected = sqlalchemy.schema.Table(
            table.name, meta, autoload_with=self.connection, resolve_fks=False, schema=table.schema
        )

        conn = self.connection.connection.dbapi_connection
        assert conn is not None, "Connection cannot be None"
        cursor = conn.cursor()

        with tempfile.TemporaryFile() as stream:
            _LOG.info("Writing %s data to a temporary file", table.name)

            with Timer("pg_bindump_time", _MON, tags={"table": table.name}) as timer:
                dumper = PgBinaryDumper(stream, reflected)
                columns = dumper.dump(data)
                file_size = stream.tell()
                timer.add_values(file_size=file_size)

            # Build COPY query, may need to quote some column names.
            columns_str = ", ".join(self.ident_prepare.quote(column) for column in columns)
            table_name = self.ident_prepare.format_table(table)
            sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH BINARY"
            _LOG.debug("COPY query: %s", sql)

            # Rewind the file so that reading from it can work.
            _LOG.info("Ingesting %s data to Postgres table", table.name)
            stream.seek(0)
            with Timer("pg_bulk_insert_time", _MON, tags={"table": table.name}) as timer:
                cursor.copy_expert(sql, stream, 1024 * 1024)
                timer.add_values(file_size=file_size)
            _LOG.info("Successfully ingested %s data", table.name)

        return len(data.rows())


def make_inserter(connection: sqlalchemy.engine.Connection) -> BulkInserter:
    """Make instance of `BulkInserter` suitable for a given connection."""
    if connection.dialect.driver == "psycopg2":
        return _Psycopg2BulkInserter(connection)
    return _DefaultBulkInserter(connection)
