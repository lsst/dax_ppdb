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

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import astropy
import pyarrow
import sqlalchemy
from lsst.dax.apdb import ApdbTableData, ReplicaChunk
from lsst.dax.apdb.timer import Timer
from lsst.dax.apdb.versionTuple import VersionTuple
from lsst.dax.ppdb.ppdb import ChunkStatus
from pyarrow import parquet

from ..config import PpdbConfig
from ..gcp._auth import get_auth_default
from ..gcp._pubsub import Publisher
from ..sql._ppdb_sql import PpdbSql

__all__ = ["ChunkExporter"]

_LOG = logging.getLogger(__name__)

_DEFAULT_COMPRESSION_FORMAT = "snappy"

_DEFAULT_BATCH_SIZE = 1000

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
    schema_version : `VersionTuple`
        Version of the APDB schema to use for the export.
    batch_size : `int`, optional
        Number of rows to process in each batch. Default is 1000.
    compression_format : `str`, optional
        Compression format for Parquet files. Default is "snappy".
    """

    def __init__(
        self,
        config: PpdbConfig,
        schema_version: VersionTuple,
        directory: Path,
        topic_name: str | None = None,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        compression_format: str = _DEFAULT_COMPRESSION_FORMAT,
    ):
        super().__init__(config)
        self.schema_version = schema_version
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        _LOG.info("Directory for chunk export: %s", self.directory)
        self.batch_size = batch_size
        self.compression_format = compression_format
        self.column_type_map = self._make_column_type_map(self._sa_metadata)

        self.credentials, self.project_id = get_auth_default()

        self.topic_name = topic_name if topic_name else "track-chunk-topic"
        self.publisher = Publisher(self.project_id, self.topic_name)

    @classmethod
    def _make_column_type_map(cls, metadata: sqlalchemy.MetaData) -> dict[str, dict[str, pyarrow.DataType]]:
        """Create a mapping of column names to Arrow types."""
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
        return column_type_map

    def _generate_manifest_data(
        self, replica_chunk: ReplicaChunk, table_dict: dict[str, ApdbTableData]
    ) -> dict[str, str]:
        """Generate the manifest data for the replica chunk."""
        return {
            "chunk_id": str(replica_chunk.id),
            "unique_id": str(replica_chunk.unique_id),
            "schema_version": str(self.schema_version),
            "exported_at": str(datetime.now(tz=timezone.utc)),
            "last_update_time": str(replica_chunk.last_update_time),  # TAI value
            "table_data": {
                table_name: {
                    "row_count": len(data.rows()),
                }
                for table_name, data in table_dict.items()
            },
            "compression_format": self.compression_format,
            "status": ChunkStatus.EXPORTED.value,
        }

    @staticmethod
    def _write_manifest(manifest_data: dict[str, str], chunk_dir: Path, replica_chunk: ReplicaChunk) -> None:
        """Write the manifest data to a JSON file."""
        final_path = chunk_dir / f"chunk_{str(replica_chunk.id)}.manifest.json"
        tmp_path = final_path.with_suffix(".tmp")
        with open(tmp_path, "w") as meta_file:
            json.dump(manifest_data, meta_file, indent=4)
        os.rename(tmp_path, final_path)
        _LOG.info("Wrote manifest file for %s: %s", replica_chunk.id, final_path)

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
        _LOG.info("Processing %s", replica_chunk.id)
        try:
            chunk_dir = self._make_path(replica_chunk.id)
            _LOG.debug("Created directory for %s: %s", replica_chunk.id, chunk_dir)

            table_dict = {
                "DiaObject": objects,
                "DiaSource": sources,
                "DiaForcedSource": forced_sources,
            }

            # Loop over the table data and write each table to a Parquet file.
            for table_name, table_data in table_dict.items():
                _LOG.info("Exporting %s", table_name)

                # Write the table data to a Parquet file.
                try:
                    with Timer("write_parquet_time", _LOG, tags={"table": table_name}) as timer:
                        self._write_parquet(table_name, table_data, chunk_dir / f"{table_name}.parquet")
                        timer.add_values(row_count=len(table_data.rows()))
                except Exception:
                    _LOG.exception("Failed to write %s", table_name)
                    raise

            # Create manifest for the replica chunk.
            try:
                manifest_data = self._generate_manifest_data(replica_chunk, table_dict)
                _LOG.info("Created manifest for %s: %s", replica_chunk.id, manifest_data)
            except Exception:
                _LOG.exception("Failed to create manifest for %s", table_name)
                raise

            # Write manifest data to a JSON file.
            try:
                ChunkExporter._write_manifest(manifest_data, chunk_dir, replica_chunk)
            except Exception:
                _LOG.exception("Failed to write manifest file for %s", table_name)
                raise
        except Exception:
            _LOG.exception("Failed to store replica chunk: %s", replica_chunk.id)
            raise

        try:
            self._post_to_track_chunk_topic(replica_chunk, ChunkStatus.EXPORTED, chunk_dir)
        except Exception:
            _LOG.exception("Failed to post to track chunk topic for %s", replica_chunk.id)
            raise

        _LOG.info("Done processing %s", replica_chunk.id)

    def _make_path(self, chunk_id: int) -> Path:
        path = Path(
            self.directory,
            datetime.today().strftime("%Y/%m/%d"),
            str(chunk_id),
        )
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _write_parquet(self, table_name: str, table_data: ApdbTableData, file_path: Path) -> None:
        # Get rows from the table data
        rows = list(table_data.rows())  # This is a list of rows (records)

        # Writing parquet is silently skipepd if there are no rows. The
        # DiaForcedSource table is empty for some chunks.
        if not rows:
            return

        # Get the expected column types for the table
        expected_types = self.column_type_map.get(table_name)
        if expected_types is None:
            raise ValueError(f"No column type map found for table: {table_name}")

        # Get column names from the table data
        input_column_names = list(table_data.column_names())  # These are the column names (attributes)

        # Only keep columns present in the expected schema
        selected_column_names = [name for name in input_column_names if name in expected_types]
        if not selected_column_names:
            raise ValueError(f"No matching columns found for table: {table_name}")

        # Prepare columns (columns, not rows)
        column_indices = {name: input_column_names.index(name) for name in selected_column_names}
        selected_columns = [[row[column_indices[name]] for row in rows] for name in selected_column_names]

        # Prepare schema
        schema = pyarrow.schema(
            [(column_name, expected_types[column_name]) for column_name in selected_column_names]
        )

        # Write in batches
        with parquet.ParquetWriter(file_path, schema, compression=self.compression_format) as writer:
            for i in range(0, len(rows), self.batch_size):
                # Ensure the batch size is valid even for the last batch
                batch_size = min(self.batch_size, len(rows) - i)

                # Prepare the batch columns by slicing the data
                try:
                    batch_columns = [
                        pyarrow.array(column[i : i + batch_size], type=expected_types[column_name])
                        for column, column_name in zip(selected_columns, selected_column_names)
                    ]

                    # Create a pyarrow Table from the selected batch
                    batch_table = pyarrow.Table.from_arrays(batch_columns, schema=schema)
                    writer.write_table(batch_table)
                except Exception as e:
                    raise ValueError(
                        f"Failed to create Arrow arrays for table {table_name}, batch {i}-{i+batch_size}: {e}"
                    )

        _LOG.info("Finished writing %s to %s", table_name, file_path)

    def _post_to_track_chunk_topic(
        self, replica_chunk: ReplicaChunk, status: ChunkStatus, directory: Path
    ) -> None:
        """Publish a message to the 'track-chunk-topic' Pub/Sub topic.

        This will add a new record to the chunk tracking database with status
        of 'exported'. The chunk uploader process will then pick up this record
        and copy the chunk into cloud storage.
        """
        # Convert last_update_time and replica_time to UTC datetime
        last_update_time = datetime.fromtimestamp(replica_chunk.last_update_time.unix_tai, tz=timezone.utc)
        now = datetime.fromtimestamp(astropy.time.Time.now().unix_tai, tz=timezone.utc)

        # Construct the message payload
        message = {
            "operation": "insert",
            "apdb_replica_chunk": replica_chunk.id,
            "values": {
                "last_update_time": str(last_update_time),
                "unique_id": str(replica_chunk.unique_id),
                "replica_time": str(now),
                "status": status.value,
                "directory": str(directory),
            },
        }

        self.publisher.publish(message)
