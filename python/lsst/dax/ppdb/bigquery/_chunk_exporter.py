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
import shutil
from datetime import datetime, timezone
from pathlib import Path

from lsst.dax.apdb import ApdbMetadata, ApdbTableData, ApdbTables, ReplicaChunk, monitor
from lsst.dax.apdb.timer import Timer
from lsst.dax.ppdbx.gcp.auth import get_auth_default

from .._arrow import write_parquet
from ..config import PpdbConfig
from ..ppdb import Ppdb, PpdbReplicaChunk
from ._config import PpdbBigQueryConfig
from ._manifest import Manifest, TableStats
from ._replica_chunk import ChunkStatus, PpdbReplicaChunkExtended, PpdbReplicaChunkSql

__all__ = ["ChunkExporter"]

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


class ChunkExporter(Ppdb):
    """Exports data from Cassandra to local Parquet files.

    Parameters
    ----------
    config : `PpdbConfig`
        Configuration object for PPDB, which must have the type
        `PpdbBigQueryConfig`.

    Notes
    -----
    By default, the exporter will not overwrite existing directories for
    chunks. This is designed to prevent accidental data loss or ingestion of
    duplicate data. In the production system, if a chunk directory already
    exists, it may indicate that there is an error in the ETL process which
    needs to be resolved or cleared before proceeding. The ``delete_existing``
    configuration option can be used to override this behavior, but it should
    be used with caution as it will remove any existing data in the specified
    directory. It may be useful for testing and development purposes.
    """

    def __init__(self, config: PpdbConfig):
        # Check for correct config type.
        if not isinstance(config, PpdbBigQueryConfig):
            raise TypeError(f"Expecting PpdbBigQueryConfig instance but got {type(config)}")

        # Initialize the SQL interface.
        self._sql = PpdbReplicaChunkSql(config)
        self._metadata = self._sql.metadata  # APDB metadata object (not SQA)
        self._schema_version = self._sql.schema_version  # Database schema version

        # Read parameters from config.
        if config.directory is None:
            raise ValueError("Directory for chunk export is not set in configuration.")
        self.directory: Path = config.directory
        self.batch_size = config.batch_size
        self.compression_format = config.compression_format
        self.delete_existing = config.delete_existing

        # Authenticate with Google Cloud to set credentials and project ID.
        self.credentials, self.project_id = get_auth_default()

    @property
    def metadata(self) -> ApdbMetadata:
        """Implement `Ppdb` interface to return APDB metadata object."""
        # docstring is inherited from a base class
        return self._metadata

    def get_replica_chunks(self, start_chunk_id: int | None = None) -> list[PpdbReplicaChunk] | None:
        # docstring is inherited from a base class
        return self._sql.get_replica_chunks(start_chunk_id)

    def _generate_manifest(
        self, replica_chunk: ReplicaChunk, table_dict: dict[str, ApdbTableData]
    ) -> Manifest:
        """Generate the manifest data for the replica chunk."""
        return Manifest(
            replica_chunk_id=str(replica_chunk.id),
            unique_id=replica_chunk.unique_id,
            schema_version=str(self._schema_version),
            exported_at=datetime.now(timezone.utc),
            last_update_time=str(replica_chunk.last_update_time),  # TAI value
            table_data={
                table_name: TableStats(row_count=len(data.rows())) for table_name, data in table_dict.items()
            },
            compression_format=self.compression_format,
        )

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

            chunk_dir.mkdir(parents=True)
            _LOG.info("Created directory for %s: %s", replica_chunk.id, chunk_dir)

            table_dict = {
                ApdbTables.DiaObject.value: objects,
                ApdbTables.DiaSource.value: sources,
                ApdbTables.DiaForcedSource.value: forced_sources,
            }

            # Loop over the table data and write each table to a Parquet file.
            for table_name, table_data in table_dict.items():
                if not table_data.rows():
                    _LOG.debug("No data for %s in %s, skipping export", table_name, replica_chunk.id)
                    continue
                parquet_file_path = chunk_dir / f"{table_name}.parquet"
                try:
                    with Timer(
                        "write_parquet_time", _MON, tags={"table": table_name, "path": str(parquet_file_path)}
                    ) as timer:
                        row_count = write_parquet(
                            table_name,
                            table_data,
                            parquet_file_path,
                            batch_size=self.batch_size,
                            compression_format=self.compression_format,
                            exclude_columns={"apdb_replica_subchunk"},
                        )
                        timer.add_values(row_count=row_count)
                    _LOG.info("Wrote %s with %d rows to %s", table_name, row_count, parquet_file_path)
                except Exception:
                    _LOG.exception("Failed to write %s", table_name)
                    raise

            # Create manifest for the replica chunk.
            try:
                manifest = self._generate_manifest(replica_chunk, table_dict)
                _LOG.info("Generated manifest for %s: %s", replica_chunk.id, manifest.model_dump_json())
            except Exception:
                _LOG.exception("Failed to generate manifest for %d", replica_chunk.id)
                raise

            # Write manifest data to a JSON file.
            try:
                manifest.write_json_file(chunk_dir)
            except Exception:
                _LOG.exception("Failed to write manifest file for %d to %s", replica_chunk.id, chunk_dir)
                raise
        except Exception:
            _LOG.exception("Failed to store replica chunk: %s", replica_chunk.id)
            raise

        # Store the replica chunk info in the database, including status and
        # directory.
        replica_chunk_ext = PpdbReplicaChunkExtended.from_replica_chunk(
            replica_chunk, ChunkStatus.EXPORTED, chunk_dir
        )
        try:
            self._sql.store_chunk(replica_chunk_ext, False)
        except Exception as e:
            _LOG.exception("Failed to store replica chunk info in database for %s", replica_chunk.id)
            raise e

        _LOG.info("Done processing %s", replica_chunk.id)

    def _get_chunk_path(self, chunk_id: int) -> Path:
        path = Path(
            self.directory,
            datetime.today().strftime("%Y/%m/%d"),
            str(chunk_id),
        )
        return path
