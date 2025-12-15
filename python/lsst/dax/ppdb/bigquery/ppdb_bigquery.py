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
from collections.abc import Collection, Iterable, Sequence
from pathlib import Path

import felis
import sqlalchemy

from lsst.dax.apdb import (
    ApdbMetadata,
    ApdbTableData,
    ApdbTables,
    ApdbUpdateRecord,
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
from .ppdb_replica_chunk_extended import ChunkStatus, PpdbReplicaChunkExtended

__all__ = ["ConfigValidationError", "PpdbBigQuery", "PpdbBigQueryConfig"]

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


VERSION = VersionTuple(0, 1, 0)
"""Version for the code defined in this module. This needs to be updated
(following compatibility rules) when schema produced by this code changes.
"""


class PpdbBigQueryConfig(PpdbConfig):
    """Configuration for BigQuery-based PPDB."""

    project_id: str
    """Google Cloud project ID."""

    dataset_id: str
    """Target BigQuery dataset ID, without the project."""

    bucket_name: str
    """Name of Google Cloud Storage bucket for uploading chunks."""

    object_prefix: str
    """Base prefix for the object in cloud storage."""

    replication_dir: str
    """Directory where the exported chunks will be stored."""

    stage_chunk_topic: str = "stage-chunk-topic"
    """Pub/Sub topic name for triggering chunk staging process."""

    parq_batch_size: int = 10000
    """Number of rows to process in each batch when writing parquet files."""

    parq_compression: str = "snappy"
    """Compression format for Parquet files."""

    delete_existing_dirs: bool = False
    """If `True`, existing directories for chunks will be deleted before
    export. If `False`, an error will be raised if the directory already
    exists.
    """

    sql: PpdbSqlBaseConfig
    """SQL database configuration (`PpdbSqlBaseConfig`)."""

    @property
    def replication_path(self) -> Path:
        """Return path for writing replica chunk data (`pathlib.Path`)."""
        return Path(self.replication_dir)

    @property
    def fq_dataset_id(self) -> str:
        """Fully qualified BigQuery dataset ID, including project.

        Returns
        -------
        fq_dataset_id : `str`
            Fully qualified BigQuery dataset ID, including project.
        """
        return f"{self.project_id}:{self.dataset_id}"


class ConfigValidationError(Exception):
    """Indicates an error validating the configuration."""


class PpdbBigQuery(Ppdb, PpdbSqlBase):
    """Provides operations for the BigQuery-based PPDB.

    Parameters
    ----------
    config : `PpdbBigQueryConfig`
        Configuration object with BigQuery and SQL database parameters.
    """

    def __init__(self, config: PpdbBigQueryConfig):
        # Initialize the SQL interface for the PPDB.
        PpdbSqlBase.__init__(self, config.sql)

        # Read parameters from config.
        if config.replication_dir is None:
            raise ValueError("Directory for chunk export is not set in configuration.")
        self.replication_path = config.replication_path
        self.parq_batch_size = config.parq_batch_size
        self.parq_compression = config.parq_compression
        self.delete_existing_dirs = config.delete_existing_dirs

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
            exported_at=datetime.datetime.now(datetime.UTC),
            last_update_time=str(replica_chunk.last_update_time),  # TAI value
            table_data={
                table_name: TableStats(row_count=len(data.rows())) for table_name, data in table_dict.items()
            },
            compression_format=self.parq_compression,
        )

    def store(
        self,
        replica_chunk: ReplicaChunk,
        objects: ApdbTableData,
        sources: ApdbTableData,
        forced_sources: ApdbTableData,
        update_records: Collection[ApdbUpdateRecord],
        *,
        update: bool = False,
    ) -> None:
        # Docstring is inherited.
        _LOG.info("Processing %s", replica_chunk.id)

        # TODO: APDB does not generate ApdbUpdateRecords yet, but we will
        # eventually have to add support for it.
        if update_records:
            raise NotImplementedError("PpdbBigQuery does not support record updates yet.")

        try:
            chunk_dir = self._get_chunk_path(replica_chunk)

            if chunk_dir.exists():
                if not self.delete_existing_dirs:
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
                            batch_size=self.parq_batch_size,
                            compression_format=self.parq_compression,
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

        if manifest.is_empty_chunk():
            # Mark as skipped if there is no data to export.
            status = ChunkStatus.SKIPPED
            _LOG.warning("No data to export for %s, marking chunk as skipped", replica_chunk.id)
        else:
            status = ChunkStatus.EXPORTED

        # Store the replica chunk info in the database, including status and
        # directory.
        replica_chunk_ext = PpdbReplicaChunkExtended.from_replica_chunk(replica_chunk, status, chunk_dir)
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
            self.replication_path,
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
                        directory=Path(row[5]),
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
    def init_bigquery(
        cls,
        db_url: str,
        project_id: str,
        dataset_id: str,
        bucket_name: str,
        object_prefix: str,
        replication_dir: str,
        db_drop: bool,
        *,
        db_schema: str | None = None,
        felis_path: str | None = None,
        felis_schema: str | None = None,  # TODO: Remove this eventually (DM-52584)
        stage_chunk_topic: str | None = None,
        parq_batch_size: int | None = None,
        parq_compression: str | None = None,
        delete_existing_dirs: bool = False,
        validate_config: bool = False,
    ) -> PpdbBigQueryConfig:
        """Initialize PPDB database and return configuration object.

        Parameters
        ----------
        db_url : `str`
            Database URL in SQLAlchemy format for PPDB instance.
        project_id : `str`
            GCP project ID.
        dataset_id : `str`
            BigQuery dataset name without the project ID.
        bucket_name : `str`
            GCS bucket name to use for Parquet output.
        object_prefix : `str`
            Object prefix to use in GCS bucket for Parquet output.
        replication_dir : `str`
            Directory used for replication staging area.
        db_drop : `bool`
            If True then drop existing db tables.
        db_schema : `str`, optional
            Database schema name for PPDB instance.
        felis_path : `str`, optional
            Path to Felis database. If `None`, defaults to the default path in
            SDM Schemas.
        felis_schema : `str`, optional
            Felis schema name within the YAML file.
        stage_chunk_topic : `str`, optional
            Pub/Sub topic to use for staging chunks.
        parq_batch_size : `int`, optional
            Number of rows to use when batching Parquet output.
        parq_compression : `str`, optional
            Compression codec to use for Parquet output.
        delete_existing_dirs : `bool`, optional
            If True then delete existing replication staging directories.
        validate_config : `bool`, optional
            If `True`, validate the configuration against GCP resources.

        Raises
        ------
        ConfigValidationError
            Raised if validation of the configuration fails.
        """
        # Create the schema and SQL db for tracking replica chunks; values of
        # None are handled by these methods.
        sa_metadata, schema_version = cls.read_schema(felis_path, db_schema, felis_schema, db_url)
        sql_config = PpdbSqlBaseConfig(
            db_url=db_url, schema_name=db_schema, felis_path=felis_path, felis_schema=felis_schema
        )
        cls.make_database(sql_config, sa_metadata, schema_version, db_drop)

        # Build config parameters.
        bq_config = PpdbBigQueryConfig(
            sql=sql_config,
            replication_dir=replication_dir,
            bucket_name=bucket_name,
            dataset_id=dataset_id,
            project_id=project_id,
            object_prefix=object_prefix,
            delete_existing_dirs=delete_existing_dirs,
        )
        if parq_batch_size is not None:
            bq_config.parq_batch_size = parq_batch_size
        if parq_compression is not None:
            bq_config.parq_compression = parq_compression
        if stage_chunk_topic is not None:
            bq_config.stage_chunk_topic = stage_chunk_topic

        # Validate the config if requested.
        if validate_config:
            _LOG.info("validating BigQuery configuration")
            PpdbBigQuery.validate_config(bq_config)
            _LOG.info("BigQuery configuration validated successfully")

        return bq_config

    @classmethod
    def get_meta_code_version_key(cls) -> str:
        # Docstring is inherited.
        return "version:PpdbBigQuery"

    @classmethod
    def get_code_version(cls) -> VersionTuple:
        # Docstring is inherited.
        return VERSION

    @classmethod
    def validate_config(cls, config: PpdbBigQueryConfig) -> None:
        """Validate the BigQuery PPDB configuration against GCP resources.

        A number of resources need to be created before the PPDB can be used
        and this method checks that they exist and are accessible with the
        current Google Cloud credentials.

        Parameters
        ----------
        config : `PpdbBigQueryConfig`
            Configuration to validate.

        Raises
        ------
        ConfigValidationError
            Raised if the configuration is invalid.

        Notes
        -----
        This method does not do any authentication to Google Cloud itself but
        depends on the environment being set up beforehand with a user
        account that has the appropriate permissions. This would typically be
        the identity of whoever is managing the PPDB and its project(s) and
        running the ``ppdb-cli`` command line interface. By design, a single
        service account would not have all of the permissions for checking
        these resources and so should not be used here.

        The best way to configure the environment for the validation will be to
        ensure that ``GOOGLE_APPLICATION_CREDENTIALS`` is unset and then run
        ``gcloud auth application-default login`` to set up the default user
        credentials, which will then be used automatically. There will likely
        be 403 errors if the permissions are insufficient in either case.
        """
        # Check for GCP dependencies and import the necessary modules. Raise an
        # error with instructions if the module is not found.
        try:
            from lsst.dax.ppdbx.gcp.bq import check_dataset_exists
            from lsst.dax.ppdbx.gcp.gcs import check_bucket_exists
            from lsst.dax.ppdbx.gcp.pubsub import Publisher
        except ImportError as e:
            raise ConfigValidationError(
                "The lsst.dax.ppdbx.gcp module is required for GCP support.\n"
                "Please 'pip install' the lsst-dax-ppdbx-gcp package from:\n"
                "https://github.com/lsst-dm/dax_ppdbx_gcp"
            ) from e

        # Check existence of the Pub/Sub stage chunk topic.
        try:
            Publisher(config.project_id, config.stage_chunk_topic).validate_topic_exists()
        except Exception as e:
            raise ConfigValidationError("Failed to validate Pub/Sub topic") from e

        # Check existence of the storage bucket for chunks.
        try:
            check_bucket_exists(config.bucket_name)
        except Exception as e:
            raise ConfigValidationError("Failed to validate GCS bucket") from e

        # Check existence of the BigQuery dataset.
        try:
            check_dataset_exists(config.project_id, config.dataset_id)
        except Exception as e:
            raise ConfigValidationError("Failed to validate BigQuery dataset") from e
