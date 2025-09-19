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

import datetime
import logging
import shutil
from collections.abc import Iterable, Sequence
from datetime import timezone
from pathlib import Path

import felis
import sqlalchemy
from lsst.dax.apdb import (
    ApdbMetadata,
    ApdbTableData,
    ApdbTables,
    ReplicaChunk,
    VersionTuple,
    monitor,
    schema_model,
)
from lsst.dax.apdb.timer import Timer

from .._arrow import write_parquet
from ..config import PpdbConfig
from ..ppdb import Ppdb, PpdbReplicaChunk
from ..sql import PpdbSqlBase, PpdbSqlBaseConfig
from .manifest import Manifest, TableStats
from .replica_chunk import ChunkStatus, PpdbReplicaChunkExtended

__all__ = ["PpdbBigQuery", "PpdbBigQueryConfig"]

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


VERSION = VersionTuple(0, 1, 0)
"""Version for the code defined in this module. This needs to be updated
(following compatibility rules) when schema produced by this code changes.
"""


class PpdbBigQueryConfig(PpdbConfig):
    """Configuration for BigQuery-based PPDB."""

    directory: Path | None = None
    """Directory where the exported chunks will be stored."""

    delete_existing: bool = False
    """If `True`, existing directories for chunks will be deleted before
    export. If `False`, an error will be raised if the directory already
    exists.
    """

    stage_chunk_topic: str = "stage-chunk-topic"
    """Pub/Sub topic name for triggering chunk staging process."""

    batch_size: int = 1000
    """Number of rows to process in each batch when writing parquet files."""

    compression_format: str = "snappy"
    """Compression format for Parquet files."""

    bucket: str | None = None
    """Name of Google Cloud Storage bucket for uploading chunks."""

    prefix: str | None = None
    """Base prefix for the object in cloud storage."""

    dataset: str | None = None
    """Target BigQuery dataset, e.g., 'my_project:my_dataset'
    (`str` or `None`). If not provided the project will be derived from the
    Google Cloud environment at runtime.
    """

    sql: PpdbSqlBaseConfig | None = None
    """SQL database configuration (`PpdbSqlBaseConfig` or `None`)."""


class PpdbBigQuery(Ppdb, PpdbSqlBase):
    """Provides operations for the BigQuery-based PPDB.

    Parameters
    ----------
    config : `PpdbConfig`
        Configuration object for PPDB, which must have the type
        `PpdbBigQueryConfig`.
    """

    def __init__(self, config: PpdbConfig):
        # Check for correct config type.
        if not isinstance(config, PpdbBigQueryConfig):
            raise TypeError(f"Expecting PpdbBigQueryConfig instance but got {type(config)}")

        # Initialize the SQL interface.
        if config.sql is None:
            raise ValueError("The 'sql' section is missing from the BigQuery config.")
        PpdbSqlBase.__init__(self, config.sql)

        # Check code compatibility with database. Base class already checked
        # schema version.
        self.check_code_version()

        # Read parameters from config.
        if config.directory is None:
            raise ValueError("Directory for chunk export is not set in configuration.")
        self.directory: Path = config.directory
        self.batch_size = config.batch_size
        self.compression_format = config.compression_format
        self.delete_existing = config.delete_existing

    @property
    def metadata(self) -> ApdbMetadata:
        """Implement `Ppdb` interface to return APDB metadata object.

        Returns
        -------
        metadata : `ApdbMetadata`
            APDB metadata object.
        """
        return self._metadata

    def _generate_manifest(
        self, replica_chunk: ReplicaChunk, table_dict: dict[str, ApdbTableData]
    ) -> Manifest:
        """Generate the manifest data for the replica chunk."""
        return Manifest(
            replica_chunk_id=str(replica_chunk.id),
            unique_id=replica_chunk.unique_id,
            schema_version=str(self.schema_version),
            exported_at=datetime.datetime.now(timezone.utc),
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
            chunk_dir = self._get_chunk_path(replica_chunk)

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
            self.store_chunk(replica_chunk_ext, False)
        except Exception as e:
            _LOG.exception("Failed to store replica chunk info in database for %s", replica_chunk.id)
            raise e

        _LOG.info("Done processing %s", replica_chunk.id)

    def _get_chunk_path(self, chunk: ReplicaChunk) -> Path:
        last_update_time = chunk.last_update_time.to_datetime()
        assert isinstance(last_update_time, datetime.datetime)
        path = Path(
            self.directory,
            chunk.last_update_time.strftime("%Y/%m/%d"),
            str(chunk.id),
        )
        return path

    def get_replica_chunks(self, start_chunk_id: int | None = None) -> Sequence[PpdbReplicaChunk] | None:
        # Docstring is inherited.
        return self.get_replica_chunks_ext(start_chunk_id=start_chunk_id)

    def get_replica_chunks_ext(
        self, status: ChunkStatus | None = None, start_chunk_id: int | None = None
    ) -> Sequence[PpdbReplicaChunkExtended]:
        """Find replica chunks having the specified status with the option to
        start from a specific chunk ID.

        If neither argument is provided, all chunks are returned.

        Parameters
        ----------
        status : `ChunkStatus`
            Status of the replica chunks to return.
        start_chunk_id : `int`, optional
            If provided, only return chunks with ID greater than or equal to
            this value.

        Returns
        -------
        chunks : `~collections.abc.Sequence` [ `PpdbReplicaChunkExtended` ]
            List of chunks with the specified status. Chunks are ordered by
            their ``last_update_time`` and include the ``directory`` and
            ``status`` fields.
        """
        table = self.get_table("PpdbReplicaChunk")
        query = sqlalchemy.sql.select(
            table.columns["apdb_replica_chunk"],
            table.columns["last_update_time"],
            table.columns["unique_id"],
            table.columns["replica_time"],
            table.columns["status"],  # Extended column
            table.columns["directory"],  # Extended column
        ).order_by(table.columns["last_update_time"])
        if start_chunk_id is not None:
            query = query.where(table.columns["apdb_replica_chunk"] >= start_chunk_id)
        if status is not None:
            query = query.where(table.columns["status"] == status.value)
        ids: list[PpdbReplicaChunkExtended] = []
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
            for row in result:
                last_update_time = self.to_astropy_tai(row[1])
                replica_time = self.to_astropy_tai(row[3])
                ids.append(
                    PpdbReplicaChunkExtended(
                        id=row[0],
                        last_update_time=last_update_time,
                        unique_id=row[2],
                        replica_time=replica_time,
                        status=row[4],
                        directory=row[5],
                    )
                )
        return ids

    def store_chunk(self, replica_chunk: PpdbReplicaChunkExtended, update: bool) -> None:
        """Insert or replace single record in PpdbReplicaChunk table, including
        the status and directory of the replica chunk.

        Parameters
        ----------
        replica_chunk : `PpdbReplicaChunkExtended`
            The replica chunk to store.
        update : `bool`
            If `True` then perform an UPSERT operation to update existing
            records. If `False` then only INSERT is performed and an error is
            raised if the record already exists.
        """
        _LOG.info("Storing replica chunk: %s", replica_chunk)
        with self._engine.begin() as connection:
            table = self.get_table("PpdbReplicaChunk")
            row = {
                "apdb_replica_chunk": replica_chunk.id,
                "last_update_time": replica_chunk.last_update_time_dt_utc,
                "unique_id": replica_chunk.unique_id,
                "replica_time": replica_chunk.replica_time_dt_utc,
                "status": replica_chunk.status,
                "directory": str(replica_chunk.directory),
            }
            if update:
                self.upsert(connection, table, row, "apdb_replica_chunk")
            else:
                insert = table.insert()
                connection.execute(insert, row)

    @classmethod
    def create_replica_chunk_table(cls, table_name: str | None = None) -> schema_model.Table:
        """Create the ``PpdbReplicaChunk`` table with additional fields for
        status and directory.

        Parameters
        ----------
        table_name : `str`, optional
            Name of the table to create. If not provided, defaults to
            "PpdbReplicaChunk".

        Notes
        -----
        This overrides the base method to add additional columns for
        ``status`` and ``directory`` to the replica chunk table schema.
        """
        replica_chunk_table = super().create_replica_chunk_table(table_name)
        replica_chunk_table.columns.extend(
            [
                schema_model.Column(
                    name="status",
                    id=f"#{table_name}.status",
                    datatype=felis.datamodel.DataType.string,
                ),
                schema_model.Column(
                    name="directory",
                    id=f"#{table_name}.directory",
                    datatype=felis.datamodel.DataType.string,
                    nullable=True,  # We might want to allow NULL if an error occurs when exporting.
                ),
            ]
        )
        return replica_chunk_table

    @classmethod
    def filter_table_names(cls, original_table_names: Iterable[str]) -> Iterable[str]:
        # Docstring is inherited.
        # Only the metadata table is needed for the BigQuery-based PPDB.
        return ["metadata"]

    @classmethod
    def init_database(
        cls,
        db_url: str,
        schema_file: str | None = None,
        schema_name: str | None = None,
        felis_schema: str | None = None,
        use_connection_pool: bool = True,
        isolation_level: str | None = None,
        connection_timeout: float | None = None,
        drop: bool = False,
    ) -> PpdbConfig:
        """Initialize PPDB database and return configuration object.

        Parameters
        ----------
        db_url : `str`
            SQLAlchemy database connection URI.
        schema_name : `str` or `None`
            Database schema name, if `None` then default schema is used.
        schema_file : `str` or `None`
            Name of YAML file with ``felis`` schema, if `None` then default
            schema file is used.
        felis_schema : `str` or `None`
            Name of the schema in YAML file, if `None` then file has to contain
            single schema.
        use_connection_pool : `bool`
            If True then allow use of connection pool.
        isolation_level : `str` or `None`
            Transaction isolation level, if unset then backend-default value is
            used.
        connection_timeout: `float` or `None`
            Maximum connection timeout in seconds.
        drop : `bool`
            If `True` then drop existing tables.
        """
        sa_metadata, schema_version = cls.read_schema(schema_file, schema_name, felis_schema, db_url)
        sql_config = cls.make_config(
            db_url=db_url,
            schema_name=schema_name,
            felis_path=schema_file,
            felis_schema=felis_schema,
            use_connection_pool=use_connection_pool,
            isolation_level=isolation_level,
            connection_timeout=connection_timeout,
        )
        cls.make_database(sql_config, sa_metadata, schema_version, drop)
        # DM-52460: This method or whatever eventually creates this
        # configuration object needs to be updated to allow setting
        # the BigQuery-specific parameters here.
        bq_config = PpdbBigQueryConfig(sql=sql_config)
        return bq_config

    @classmethod
    def get_meta_code_version_key(cls) -> str:
        # Docstring is inherited.
        return "version:PpdbBigQuery"

    @classmethod
    def get_code_version(cls) -> VersionTuple:
        # Docstring is inherited.
        return VERSION
