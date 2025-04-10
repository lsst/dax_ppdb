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
import os
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime

from ._ppdb_sql import PpdbSql
from lsst.dax.apdb import ApdbTableData, ReplicaChunk

try:
    from google.cloud import storage
except ImportError:
    storage = None

__all__ = ["ChunkExporter"]

_LOG = logging.getLogger(__name__)

if storage is None:
    _LOG.warning(
        "Google Cloud Storage client library is not available. "
        "GCS operations will not be performed. "
        "Please install the 'google-cloud-storage' library to enable."
    )


class ChunkExporter(PpdbSql):
    """Exports data from Cassandra to in-memory Parquet and uploads to GCS."""

    def __init__(self, *args, **kwargs):
        _LOG.info("Initializing ChunkExporter")
        super().__init__(*args, **kwargs)
        self.compression_format = "snappy"
        if storage:
            if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
                raise RuntimeError("Environment variable GOOGLE_APPLICATION_CREDENTIALS is not set.")
            self.bucket_name = "rubin-ppdb-test-bucket-1"
            self.folder_name = "tmp"
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)

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
            ["objects", "sources", "forced_sources"],
            [objects, sources, forced_sources],
        ):
            _LOG.info("Processing %s", table_name)
            if len(table_data.rows()) == 0:
                _LOG.info("Skipping %s: table is empty", table_name)
                continue

            try:
                arrow_table = self._convert_to_arrow(table_data)
            except Exception as e:
                _LOG.error("Failed to convert table to Arrow: %s", e)
                raise

            _LOG.info(
                "Created Arrow Table with %d rows and %d columns",
                arrow_table.num_rows,
                arrow_table.num_columns,
            )
            memory_usage_mb = arrow_table.nbytes / 1_048_576
            _LOG.info("Estimated memory usage: %.2f MB", memory_usage_mb)

            if storage:
                gcs_path = self._get_gcs_path(table_name, replica_chunk.id)
                _LOG.info("Uploading %s to GCS path: %s", table_name, gcs_path)
                self._upload_table_to_gcs(arrow_table, gcs_path)

    @classmethod
    def _convert_to_arrow(cls, table_data: ApdbTableData) -> pa.Table:
        rows = list(table_data.rows())
        column_names = list(table_data.column_names())

        if not rows:
            _LOG.warning("No rows provided; creating empty Arrow Table with schema only")
            schema = pa.schema([(name, pa.null()) for name in column_names])
            return pa.table([], schema=schema)

        _LOG.info("Converting %d rows with %d columns to Arrow Table", len(rows), len(column_names))

        columns = list(zip(*rows))
        arrays = [pa.array(col) for col in columns]
        return pa.table(dict(zip(column_names, arrays)))

    def _get_gcs_path(self, table_name: str, chunk_id: int) -> str:
        today = datetime.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")
        return f"{self.folder_name}/{year}/{month}/{day}/{str(chunk_id)}/{table_name}.parquet"

    def _upload_table_to_gcs(self, table: pa.Table, gcs_path: str) -> None:
        with BytesIO() as buf:
            try:
                pq.write_table(table, buf, compression=self.compression_format)
                buf.seek(0)
                blob = self.bucket.blob(gcs_path)
                blob.upload_from_file(buf, content_type="application/octet-stream")
            except Exception as e:
                _LOG.error("Failed to upload in-memory table to GCS: %s", e)
                raise
        _LOG.info("Successfully uploaded to GCS path: %s", gcs_path)
