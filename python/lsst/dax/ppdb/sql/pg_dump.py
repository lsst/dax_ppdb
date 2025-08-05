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

__all__ = ["PgBinaryDumper"]

import logging
import struct
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, BinaryIO, NamedTuple

import sqlalchemy
import sqlalchemy.dialects.postgresql.types as pg_types
from lsst.dax.apdb import ApdbTableData
from sqlalchemy.sql import sqltypes

_LOG = logging.getLogger(__name__)


class StructData(NamedTuple):

    size: int
    format: str
    value: Any


class _ColumnDataHandler(ABC):

    @abstractmethod
    def to_struct(self, column_value: Any) -> StructData:
        raise NotImplementedError()


class PgBinaryDumper:
    """Class that knows how to dump ApdbTableData to binary PostgreSQL file."""

    _HEADER = b"PGCOPY\n\377\r\n\0"

    def __init__(self, stream: BinaryIO, table: sqlalchemy.schema.Table):
        self._stream = stream
        self._table = table

    def dump(self, data: ApdbTableData) -> list[str]:
        """Dump the whole contents of table data to a file."""
        # Only care about columns that exists in both table and data.
        data_column_names = data.column_names()
        table_column_names = set(column.name for column in self._table.columns)
        _LOG.debug("table_column_names: %s", table_column_names)

        column_indices = [idx for idx, name in enumerate(data_column_names) if name in table_column_names]
        types = [self._table.columns[data_column_names[idx]].type for idx in column_indices]
        handlers = [_TYPE_MAP[column_type.__class__] for column_type in types]

        # Write PGDUMP header and flags (two 32-bit integers)
        self._stream.write(self._HEADER + b"\0\0\0\0\0\0\0\0")

        # Dump all rows.
        for row in data.rows():

            # Buld row struct, it starts with the number of columns as 16-bit
            # integer, all data is in network order.
            fmt = ["!h"]
            args = [len(column_indices)]
            for idx, handler in zip(column_indices, handlers):
                struct_data = handler.to_struct(row[idx])
                if struct_data.value is None:
                    # Null is encoded as size=-1, without data
                    fmt.append("i")
                    args.append(-1)
                else:
                    fmt.extend(["i", struct_data.format])
                    args.extend([struct_data.size, struct_data.value])

            row_bytes = struct.pack("".join(fmt), *args)
            self._stream.write(row_bytes)

        return [data_column_names[idx] for idx in column_indices]


class _FixedColumnDataHandler(_ColumnDataHandler):

    def __init__(self, size: int, format: str):
        self._size = size
        self._format = format

    def to_struct(self, column_value: Any) -> StructData:
        return StructData(size=self._size, format=self._format, value=column_value)


class _ByteArrayColumnDataHandler(_ColumnDataHandler):

    def __init__(self, format: str):
        self._format = format

    def to_struct(self, column_value: Any) -> StructData:
        if column_value is None:
            return StructData(size=-1, format=self._format, value=None)
        format = f"{len(column_value)}{self._format}"
        return StructData(size=len(column_value), format=format, value=column_value)


class _StringColumnDataHandler(_ColumnDataHandler):

    def __init__(self, format: str):
        self._format = format

    def to_struct(self, column_value: Any) -> StructData:
        if column_value is None:
            return StructData(size=-1, format=self._format, value=None)
        # Assume that utf8 is OK for all string data
        assert isinstance(column_value, str), "Expect string instance"
        value = column_value.encode()
        format = f"{len(value)}{self._format}"
        return StructData(size=len(value), format=format, value=value)


class _TimestampColumnDataHandler(_ColumnDataHandler):

    epoch_utc = datetime(2000, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    epoch_naive = datetime(2000, 1, 1, 0, 0, 0)

    def to_struct(self, column_value: Any) -> StructData:
        if column_value is None:
            return StructData(size=-1, format="q", value=None)
        assert isinstance(column_value, datetime), "Expect datetime instance"
        # Timestamps are stored internally as microseconds since epoch
        # (Jan 1, 2000)
        if column_value.tzinfo is None:
            delta = column_value - self.epoch_naive
        else:
            delta = column_value - self.epoch_utc
        delta_usec = int(delta / timedelta(microseconds=1))
        return StructData(size=8, format="q", value=delta_usec)


# We need to know how to convert SQLAlchemy types to binary data. There are
# many SQLAlchemy types and we do not need to support all of them. Below is the
# list of types that exist in SQLAlchemy and mapping of some of those to a
# _ColumnDataHandler instance. More types can be added later if needed.
#
# List of type names in postgres dialect:
#     BIT
#     BYTEA         # done
#     CIDR
#     CITEXT
#     INET
#     INTERVAL
#     MACADDR
#     MACADDR8
#     MONEY
#     OID
#     PGBit
#     PGCidr
#     PGInet
#     PGInterval
#     PGMacAddr
#     PGMacAddr8
#     PGUuid
#     REGCLASS
#     REGCONFIG
#     TIME
#     TIMESTAMP     # done
#     TSQUERY
#     TSVECTOR
#
# List of type names common to all dialects.
#     ARRAY
#     BIGINT        # done
#     BINARY
#     BLOB
#     BOOLEAN
#     BigInteger
#     Boolean
#     CHAR          # done
#     CLOB
#     DATE
#     DATETIME
#     DATETIME_TIMEZONE
#     DECIMAL
#     DOUBLE
#     DOUBLE_PRECISION
#     Date
#     DateTime
#     Double
#     FLOAT
#     Float
#     INT           # done
#     INTEGER       # done
#     Integer
#     Interval
#     JSON
#     LargeBinary
#     MATCHTYPE
#     MatchType
#     NCHAR
#     NULLTYPE
#     NUMERIC
#     NUMERICTYPE
#     NVARCHAR
#     Numeric
#     REAL          # done
#     SMALLINT      # done
#     STRINGTYPE
#     SmallInteger
#     String
#     TEXT
#     TIME
#     TIMESTAMP
#     TIME_TIMEZONE
#     Text
#     Time
#     Tuple
#     TupleType
#     Type
#     UUID
#     Unicode
#     UnicodeText
#     Uuid
#     VARBINARY
#     VARCHAR

_TYPE_MAP = {
    sqltypes.SMALLINT: _FixedColumnDataHandler(2, "h"),
    sqltypes.INT: _FixedColumnDataHandler(4, "i"),
    sqltypes.INTEGER: _FixedColumnDataHandler(4, "i"),
    sqltypes.BIGINT: _FixedColumnDataHandler(8, "q"),
    sqltypes.DOUBLE: _FixedColumnDataHandler(8, "d"),
    sqltypes.DOUBLE_PRECISION: _FixedColumnDataHandler(8, "d"),
    sqltypes.BOOLEAN: _FixedColumnDataHandler(1, "?"),
    sqltypes.REAL: _FixedColumnDataHandler(4, "f"),
    sqltypes.FLOAT: _FixedColumnDataHandler(4, "f"),
    sqltypes.CHAR: _StringColumnDataHandler("s"),
    sqltypes.VARCHAR: _StringColumnDataHandler("s"),
    pg_types.BYTEA: _ByteArrayColumnDataHandler("s"),
    pg_types.TIMESTAMP: _TimestampColumnDataHandler(),
}


def _dump_pgdump(filename: str) -> None:
    """Dump content of pgdump file, column values are not printed."""
    with open(filename, "rb") as pgdump:
        header = pgdump.read(11 + 4 + 4)
        print("header:", header)
        buffer = pgdump.read(2)
        (count,) = struct.unpack("!h", buffer)
        print("column count:", count)
        for i in range(count):
            buffer = pgdump.read(4)
            (size,) = struct.unpack("!i", buffer)
            print(f"  {i}: {size}")
            if size > 0:
                buffer = pgdump.read(size)


if __name__ == "__main__":
    import sys

    # If called as executable script dump content of a file in the first
    # argument.
    _dump_pgdump(sys.argv[1])
