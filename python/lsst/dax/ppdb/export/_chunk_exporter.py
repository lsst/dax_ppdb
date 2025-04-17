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

import logging
import pprint
from datetime import datetime
from pathlib import Path

import pyarrow
import sqlalchemy
from lsst.dax.apdb import ApdbTableData, ReplicaChunk
from pyarrow import parquet

from ..config import PpdbConfig
from ..sql._ppdb_sql import PpdbSql

__all__ = ["ChunkExporter"]

_LOG = logging.getLogger(__name__)

_DEFAULT_COMPRESSION_FORMAT = "snappy"

_APDB_TABLES = ("DiaObject", "DiaSource", "DiaForcedSource")

_SQLTYPE = sqlalchemy.sql.sqltypes

_PYARROW_TYPE = {
    _SQLTYPE.BigInteger: pyarrow.int64(),
    _SQLTYPE.Boolean: pyarrow.bool_(),
    _SQLTYPE.CHAR: pyarrow.string(),
    _SQLTYPE.Double: pyarrow.float64(),
    _SQLTYPE.Integer: pyarrow.int32(),
    _SQLTYPE.REAL: pyarrow.float64(),
    _SQLTYPE.SmallInteger: pyarrow.int16(),
    _SQLTYPE.TIMESTAMP: pyarrow.timestamp("ms", tz="UTC"),
    _SQLTYPE.VARCHAR: pyarrow.string(),
}


class ChunkExporter(PpdbSql):
    """Exports data from Cassandra to local Parquet files.

    Parameters
    ----------
    config : `PpdbConfig`
        Configuration object for PPDB.
    directory : `Path`
        Directory where the exported chunks will be stored.
    batch_size : `int`, optional
        Number of rows to process in each batch. Default is 1000.
    compression_format : `str`, optional
        Compression format for Parquet files. Default is "snappy".
    """

    def __init__(
        self,
        config: PpdbConfig,
        directory: Path,
        batch_size: int = 1000,
        compression_format: str = _DEFAULT_COMPRESSION_FORMAT,
    ):
        super().__init__(config)
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        _LOG.info("Directory for chunk export: %s", self.directory)
        self.batch_size = batch_size
        self.compression_format = compression_format
        self.schema_version = self._metadata.get(self.meta_schema_version_key)
        self.column_type_map = self._make_column_type_map(self._sa_metadata)

    @classmethod
    def _make_column_type_map(cls, metadata: sqlalchemy.MetaData) -> dict[str, dict[str, pyarrow.DataType]]:
        """Create a mapping of column names to SQLAlchemy types."""
        column_type_map = {}
        for table in metadata.tables.values():
            column_types = {}
            if table.name in _APDB_TABLES:
                for column in table.columns.values():
                    arrow_type = _PYARROW_TYPE.get(type(column.type), None)
                    if arrow_type is None:
                        raise ValueError(
                            f'"{table.name}"."{column.name}" has an unsupported column type: {column.type}'
                        )
                    column_types[column.name] = arrow_type
                column_type_map[table.name] = column_types
        _LOG.debug("Column type map:\n%s", pprint.pformat(column_type_map))
        return column_type_map

    def store(
        self,
        replica_chunk: ReplicaChunk,
        objects: ApdbTableData,
        sources: ApdbTableData,
        forced_sources: ApdbTableData,
        *,
        update: bool = False,
    ) -> None:
        # Docstring is inherited.
        try:
            chunk_dir = self._make_path(replica_chunk.id)
            _LOG.debug("Temporary directory for chunk %s: %s", replica_chunk.id, chunk_dir)
            for table_name, table_data in zip(
                _APDB_TABLES,
                [objects, sources, forced_sources],
            ):
                _LOG.info("Processing %s", table_name)
                if len(table_data.rows()) == 0:
                    _LOG.info("Skipping %s: table is empty", table_name)
                    continue

                try:
                    arrow_table = self._convert_to_arrow(table_name, table_data)
                except Exception as e:
                    _LOG.error("Failed to convert table to Arrow: %s", e)
                    raise

                _LOG.info(
                    "Created Arrow Table with %d rows and %d columns",
                    arrow_table.num_rows,
                    arrow_table.num_columns,
                )
                memory_usage_mb = arrow_table.nbytes / 1_048_576
                _LOG.debug("Estimated memory usage: %.2f MB", memory_usage_mb)

                file_path = chunk_dir / f"{table_name}.parquet"
                parquet.write_table(arrow_table, file_path, compression=self.compression_format)

        except Exception as e:
            _LOG.error("Failed to store replica chunk: %s", e)
            raise

        # Mark the chunk as ready for upload by creating a ".ready" file.
        self._set_ready(chunk_dir)

        # Update the database to indicate that the chunk has been exported.
        with self._engine.begin() as connection:
            self._store_insert_id(replica_chunk, connection, update)

    def _set_ready(self, directory: Path) -> None:
        ready_file = directory / ".ready"
        if not ready_file.exists():
            ready_file.touch()
            _LOG.info("Marked chunk %s as ready", directory)

    def _make_path(self, chunk_id: int) -> Path:
        path = Path(self.directory, datetime.today().strftime("%Y/%m/%d"), str(chunk_id))
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _convert_to_arrow(self, table_name: str, table_data: ApdbTableData) -> pyarrow.Table:
        expected_types = self.column_type_map.get(table_name)
        if expected_types is None:
            raise ValueError(f"No column type map found for table: {table_name}")

        rows = list(table_data.rows())
        input_column_names = list(table_data.column_names())

        # Only keep columns present in the expected schema
        selected_column_names = [name for name in input_column_names if name in expected_types]
        if not selected_column_names:
            raise ValueError(f"No matching columns found for table: {table_name}")

        # Reorder and filter the columns to match selected_column_names
        zipped_columns = list(zip(*rows))
        selected_columns = [
            col for name, col in zip(input_column_names, zipped_columns) if name in expected_types
        ]

        arrays = [
            pyarrow.array(column, type=expected_types[column_name])
            for column, column_name in zip(selected_columns, selected_column_names)
        ]
        schema = pyarrow.schema(
            [(column_name, expected_types[column_name]) for column_name in selected_column_names]
        )

        return pyarrow.table(arrays, schema=schema)
