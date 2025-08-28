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

from collections.abc import Sequence
from pathlib import Path

import pyarrow
from felis.datamodel import DataType
from lsst.dax.apdb import ApdbTableData
from pyarrow import parquet

_FELIS_TYPE_MAP = {
    DataType.long: pyarrow.int64(),
    DataType.boolean: pyarrow.bool_(),
    DataType.char: pyarrow.string(),
    DataType.double: pyarrow.float64(),
    DataType.int: pyarrow.int32(),
    DataType.float: pyarrow.float32(),
    DataType.short: pyarrow.int16(),
    DataType.timestamp: pyarrow.timestamp("ms", tz="UTC"),
    DataType.string: pyarrow.string(),
}


def _felis_to_arrow_type(felis_type: DataType) -> pyarrow.DataType:
    """Convert a Felis data type to an Arrow data type.

    Parameters
    ----------
    felis_type : `felis.datamodel.DataType`
        Felis data type to convert.

    Returns
    -------
    arrow_type : `pyarrow.DataType`
        Corresponding Arrow data type.
    """
    if arrow_type := _FELIS_TYPE_MAP.get(felis_type):
        return arrow_type
    raise ValueError(f"Unsupported Felis type: {felis_type}")


def create_arrow_schema(
    column_defs: Sequence[tuple[str, DataType]], exclude_columns: set[str] | None = None
) -> pyarrow.Schema:
    """Create a PyArrow schema from column definitions, optionally excluding
    columns.

    Parameters
    ----------
    column_defs : `Sequence` [ `tuple` [ `str`, `felis.datamodel.DataType` ] ]
        Column name and type pairs.
    exclude_columns : `set` [`str`], optional
        Column names to exclude from the schema.

    Returns
    -------
    schema : `pyarrow.Schema`
        The resulting schema.
    """
    if exclude_columns is None:
        exclude_columns = set()
    return pyarrow.schema(
        [(name, _felis_to_arrow_type(dtype)) for name, dtype in column_defs if name not in exclude_columns]
    )


def write_parquet(
    table_name: str,
    table_data: ApdbTableData,
    file_path: Path,
    batch_size: int | None = None,
    compression_format: str | None = None,
    exclude_columns: set[str] | None = None,
) -> int:
    """Batch write a table of APDB data to Parquet, excluding specified
    columns.

    Parameters
    ----------
    table_name : `str`
        Logical table name (for logging and error messages).
    table_data : `lsst.dax.apdb.ApdbTableData`
        The APDB table data to write.
    file_path : `pathlib.Path`
        Destination Parquet file path.
    excluded_columns : `set` [ `str` ], optional
        Set of column names to exclude from the Parquet file. These
        exclusions apply to all of the tables. Default is an empty set,
        meaning no columns are excluded.
    batch_size : `int`, optional
        Number of rows to write in each batch. If `None`, defaults to 1000
    compression_format : `str`, optional
        Compression format to use for the Parquet file. If `None`, defaults
        to "snappy".

    Returns
    -------
    total : `int`
        Total number of rows written to the Parquet file.
    """
    rows = list(table_data.rows())
    if not rows:
        return 0

    if exclude_columns is None:
        exclude_columns = set()

    if compression_format is None:
        compression_format = "snappy"

    if batch_size is None:
        batch_size = 1000

    # Create Arrow schema from the table data column definitions, excluding
    # unwanted columns.
    schema = create_arrow_schema(table_data.column_defs(), exclude_columns=exclude_columns)
    schema_names = [f.name for f in schema]
    field_types = {f.name: f.type for f in schema}

    # Map column names to their indices in the rows.
    col_names = list(table_data.column_names())
    col_index_by_name = {name: i for i, name in enumerate(col_names)}

    # Check if all schema names are present in the column names.
    missing = [n for n in schema_names if n not in col_index_by_name]
    if missing:
        raise ValueError(f"{table_name}: missing columns in data: {missing}")

    total = len(rows)
    batch_size = max(1, batch_size)

    with parquet.ParquetWriter(file_path, schema, compression=compression_format) as writer:
        for start in range(0, total, batch_size):
            stop = min(start + batch_size, total)

            # Create a batch of Arrow arrays for the current slice of rows.
            batch_arrays: list[pyarrow.Array] = []
            for name in schema_names:
                idx = col_index_by_name[name]
                gen = (rows[r][idx] for r in range(start, stop))
                batch_arrays.append(pyarrow.array(gen, type=field_types[name]))

            batch_table = pyarrow.Table.from_arrays(batch_arrays, schema=schema)
            writer.write_table(batch_table)
    return total
