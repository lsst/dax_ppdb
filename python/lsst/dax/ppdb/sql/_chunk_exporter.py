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
import pyarrow as pa
import pyarrow.parquet as pq

from ._ppdb_sql import PpdbSql
from lsst.dax.apdb import ApdbTableData, ReplicaChunk

__all__ = ["ChunkExporter"]

_LOG = logging.getLogger(__name__)


class ChunkExporter(PpdbSql):
    """ChunkExporter is a class that handles chunk exporting for PPDB.

    This class is responsible for exporting data in chunks from the
    PPDB database. It inherits from the PpdbSql class, which provides
    the necessary SQL functionality.
    """

    def __init__(self, *args, **kwargs):
        _LOG.info("Initializing ChunkExporter")
        super().__init__(*args, **kwargs)

    def store(
        self,
        replica_chunk: ReplicaChunk,
        objects: ApdbTableData,
        sources: ApdbTableData,
        forced_sources: ApdbTableData,
        *,
        update: bool = False,
    ) -> None:
        for table_name, table_data in zip(
            ["objects", "sources", "forced_sources"], [objects, sources, forced_sources]
        ):
            _LOG.info("Processing %s", table_name)
            if len(table_data.rows()) == 0:
                _LOG.info("Skipping %s: table is empty", table_name)
                continue
            arrow_table = self._convert_to_arrow(table_data)
            _LOG.info(
                "Created Arrow Table with %d rows and %d columns",
                arrow_table.num_rows,
                arrow_table.num_columns,
            )
            memory_usage_mb = arrow_table.nbytes / 1_048_576
            _LOG.info("Estimated memory usage: %.2f MB", memory_usage_mb)
            self._write_parquet(table_name, arrow_table, replica_chunk)

    @classmethod
    def _convert_to_arrow(cls, table_data: ApdbTableData) -> pa.Table:
        rows = list(table_data.rows())
        column_names = list(table_data.column_names())

        if not rows:
            _LOG.warning("No rows provided; creating empty Arrow Table with schema only")
            schema = pa.schema([(name, pa.null()) for name in column_names])
            return pa.table([], schema=schema)

        _LOG.info("Converting %d rows with %d columns to Arrow Table", len(rows), len(column_names))

        # Transpose list of row tuples to column-wise lists
        columns = list(zip(*rows))
        arrays = [pa.array(col) for col in columns]
        return pa.table(dict(zip(column_names, arrays)))

    @classmethod
    def _write_parquet(cls, table_name: str, table: pa.Table, replica_chunk: ReplicaChunk) -> str:
        chunk_id = replica_chunk.id
        filename = f"{table_name}_{chunk_id}.parquet"
        _LOG.info("Writing Arrow Table to %s", filename)
        pq.write_table(table, filename, compression="snappy")
        return filename
