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
import shutil
from datetime import datetime, timezone
from pathlib import Path

import astropy
import pyarrow
from lsst.dax.apdb import ApdbTableData, ReplicaChunk
from lsst.dax.apdb.timer import Timer
from lsst.dax.apdb.versionTuple import VersionTuple
from lsst.ppdb.gcp.auth import get_auth_default
from lsst.ppdb.gcp.pubsub import Publisher
from pyarrow import parquet

from ..config import PpdbConfig
from ..sql._ppdb_replica_chunk_sql import ChunkStatus, PpdbReplicaChunkSql
from ._arrow import create_arrow_schema

__all__ = ["ChunkExporter"]

_LOG = logging.getLogger(__name__)


class ChunkExporter(PpdbReplicaChunkSql):
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
    delete_existing : `bool`, optional
        If `True`, existing directories for chunks will be deleted before
        export. Default is `False`.

    Notes
    -----
    By default, the exporter will not overwrite existing directories for
    chunks. This is designed to prevent accidental data loss or ingestion of
    duplicate data. In the production system, if a chunk directory already
    exists, it may indicate that there is an error in the ETL process which
    needs to be resolved or cleared before proceeding. The `delete_existing`
    option can be used to override this behavior, but it should be used with
    caution as it will remove any existing data in the specified directory. It
    may be useful for testing and development purposes.
    """

    def __init__(
        self,
        config: PpdbConfig,
        schema_version: VersionTuple,
        directory: Path,
        topic_name: str | None = None,
        batch_size: int | None = None,
        compression_format: str | None = None,
        delete_existing: bool = False,
    ):
        super().__init__(config)
        self.schema_version = schema_version
        self.directory = directory
        if self.directory == "/":
            raise ValueError("Export directory cannot be the root directory ('/').")
        _LOG.info("Directory for chunk export: %s", self.directory)
        self.batch_size = batch_size or 10000
        self.compression_format = compression_format or "snappy"

        self.credentials, self.project_id = get_auth_default()

        self.topic_name = topic_name if topic_name else "track-chunk-topic"
        self.publisher = Publisher(self.project_id, self.topic_name)

        self.delete_existing = delete_existing

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
            chunk_dir = self._get_chunk_path(replica_chunk.id)

            if chunk_dir.exists():
                if not self.delete_existing:
                    raise FileExistsError(f"Directory already exists for {replica_chunk.id}: {chunk_dir}")
                _LOG.warning("Overwriting existing directory for %s: %s", replica_chunk.id, chunk_dir)
                shutil.rmtree(chunk_dir)

            chunk_dir.mkdir(parents=True, exist_ok=True)
            _LOG.info("Created directory for %s: %s", replica_chunk.id, chunk_dir)

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

    def _get_chunk_path(self, chunk_id: int) -> Path:
        path = Path(
            self.directory,
            datetime.today().strftime("%Y/%m/%d"),
            str(chunk_id),
        )
        return path

    def _write_parquet(self, table_name: str, table_data: ApdbTableData, file_path: Path) -> None:
        """Batch write a table of APDB data to Parquet.

        Parameters
        ----------
        table_name : str
            Logical table name (for logging and error messages).
        table_data : ApdbTableData
            The APDB table data to write.
        file_path : Path
            Destination Parquet file path.
        """
        rows = list(table_data.rows())
        if not rows:
            return

        # Create Arrow schema from the table data column definitions.
        schema = create_arrow_schema(table_data.column_defs())
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
        batch_size = max(1, int(self.batch_size))

        with parquet.ParquetWriter(file_path, schema, compression=self.compression_format) as writer:
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

        _LOG.info("Wrote %s rows from %s to %s (batch_size=%s)", total, table_name, file_path, batch_size)

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
