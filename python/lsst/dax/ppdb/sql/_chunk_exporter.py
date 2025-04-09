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

from datetime import datetime
from pathlib import Path

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
    """Dumps Parquet files from the APDB Cassandra database to local files and
    then uploads them to Google Cloud Storage (GCS).
    """

    def __init__(self, *args, **kwargs):
        _LOG.info("Initializing ChunkExporter")
        super().__init__(*args, **kwargs)
        self.local_dir = Path(os.getcwd())  # TODO: Make this configurable
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.compression_format = "snappy"  # TODO: Make this configurable
        if storage:
            if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
                raise RuntimeError(
                    "Environment variable GOOGLE_APPLICATION_CREDENTIALS is not set. "
                    "Please set it to the path of your GCP credentials file."
                )
            self.bucket_name = "rubin-ppdb-test-bucket-1"  # TODO: Make this configurable
            self.folder_name = "tmp"  # TODO: Make this configurable
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
        # Docstring is inherited.
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
            filename = self._write_parquet(table_name, arrow_table, replica_chunk)
            if storage:
                gcs_path = self._get_gcs_path(table_name, replica_chunk.id)
                _LOG.info("Uploading %s to GCS path: %s", filename, gcs_path)
                self._upload_to_gcs(str(filename), gcs_path)

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

    def _write_parquet(self, table_name: str, table: pa.Table, replica_chunk: ReplicaChunk) -> str:
        chunk_id = replica_chunk.id
        filename = self.local_dir / Path(f"{table_name}_{chunk_id}.parquet")
        _LOG.info("Writing Arrow Table to %s", filename)
        try:
            pq.write_table(table, filename, compression=self.compression_format)
        except Exception as e:
            _LOG.error("Failed to write table to Parquet: %s", e)
            raise
        return filename

    def _get_gcs_path(self, table_name: str, chunk_id: int) -> str:
        today = datetime.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")
        return (
            f"gs://{self.bucket_name}/{self.folder_name}/{year}/{month}/{day}/"
            f"{str(chunk_id)}/{table_name}.parquet"
        )

    def _upload_to_gcs(self, filename: str, gcs_path: str) -> None:
        blob = self.bucket.blob(filename)
        try:
            blob.upload_from_filename(filename)
        except Exception as e:
            _LOG.error("Failed to upload file to GCS: %s", e)
            raise
        _LOG.info("Uploaded %s to GCS path: %s", filename, gcs_path)
