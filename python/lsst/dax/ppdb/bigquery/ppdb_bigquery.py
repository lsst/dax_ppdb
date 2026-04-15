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

from __future__ import annotations

__all__ = ["ConfigValidationError", "PpdbBigQuery", "PpdbBigQueryConfig"]

import datetime
import logging
import os
import shutil
from collections.abc import Collection, Iterable, Sequence
from pathlib import Path
from typing import Any

import felis
import sqlalchemy
from google.cloud import secretmanager

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
from ..ppdb import Ppdb, PpdbReplicaChunk
from ..ppdb_config import PpdbConfig
from ..sql import PasswordProvider, PpdbSqlBase, PpdbSqlBaseConfig
from .manifest import Manifest, TableStats
from .ppdb_replica_chunk_extended import ChunkStatus, PpdbReplicaChunkExtended
from .sql_resource import SqlResource
from .updates.update_records import UpdateRecords

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
        `str`
            Fully qualified BigQuery dataset ID, including project.
        """
        return f"{self.project_id}:{self.dataset_id}"


class _SecretManagerPasswordProvider(PasswordProvider):
    """Retrieves a database password from Google Cloud Secret Manager.

    Parameters
    ----------
    project_id
        GCP project that owns the secret.
    secret_name
        Name of the secret. Defaults to ``"ppdb-db-password"``.
    """

    def __init__(self, project_id: str, secret_name: str = "ppdb-db-password") -> None:
        self._project_id = project_id
        self._secret_name = secret_name

    def get_password(self) -> str:
        """Return the password fetched from Secret Manager."""
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{self._project_id}/secrets/{self._secret_name}/versions/latest"
        _LOG.debug("Retrieving database password from Secret Manager: %s", name)
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")


class ConfigValidationError(Exception):
    """Indicates an error validating the configuration."""


class PpdbBigQuery(Ppdb, PpdbSqlBase):
    """Provides operations for the BigQuery-based PPDB.

    Parameters
    ----------
    config
        Configuration object with BigQuery and SQL database parameters.
    """

    def __init__(self, config: PpdbBigQueryConfig):
        # Read parameters from config.
        if config.replication_dir is None:
            raise ValueError("Directory for chunk export is not set in configuration.")

        # Build an optional password provider for GCP Secret Manager.
        password_provider: PasswordProvider | None = None
        if os.getenv("PPDB_USE_SECRET_MANAGER", "false").lower() == "true":
            _LOG.debug("Using Secret Manager to retrieve database password")
            password_provider = _SecretManagerPasswordProvider(config.project_id)

        # Delegate SQL initialisation (schema load, engine, metadata, version
        # checks) to the base class, passing the optional password provider.
        PpdbSqlBase.__init__(self, config.sql, password_provider=password_provider)

        self._config = config

    @property
    def metadata(self) -> ApdbMetadata:
        """APDB metadata object from `~lsst.dax.ppdb.Ppdb` interface
        (`~lsst.dax.apdb.ApdbMetadata`).
        """
        return self._metadata

    @property
    def config(self) -> PpdbBigQueryConfig:
        """PPDB config associated with this instance."""
        return self._config

    @classmethod
    def from_env(cls) -> PpdbBigQuery:
        """Create an instance of this class from a config pointed to by an
        environment variable.

        Returns
        -------
        `PpdbBigQuery`
            An instance of the PPDB BigQuery interface.
        """
        ppdb_config_uri = os.environ.get("PPDB_CONFIG_URI", None)
        if ppdb_config_uri:
            logging.info("PPDB_CONFIG_URI: %s", ppdb_config_uri)
        else:
            raise OSError("PPDB_CONFIG_URI is not set in the environment")
        ppdb = Ppdb.from_uri(ppdb_config_uri)
        if not isinstance(ppdb, PpdbBigQuery):
            raise ValueError(f"Ppdb from environment has wrong type: {type(ppdb)}")
        return ppdb

    def _generate_manifest(
        self,
        replica_chunk: ReplicaChunk,
        table_dict: dict[str, ApdbTableData],
        update_records: Collection[ApdbUpdateRecord],
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
            compression_format=self.config.parq_compression,
            update_count=len(update_records),
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

        try:
            chunk_dir = self._create_chunk_dir(replica_chunk)

            if update_records:
                self._handle_updates(replica_chunk, update_records, chunk_dir)

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
                            batch_size=self.config.parq_batch_size,
                            compression_format=self.config.parq_compression,
                            exclude_columns={"apdb_replica_subchunk"},
                        )
                        timer.add_values(row_count=row_count)
                    _LOG.info("Wrote %s with %d rows to %s", table_name, row_count, parquet_file_path)
                except Exception:
                    _LOG.exception("Failed to write %s", table_name)
                    raise

            # Create manifest for the replica chunk.
            try:
                manifest = self._generate_manifest(replica_chunk, table_dict, update_records)
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

        # Store the replica chunk info in the database, including status,
        # directory, and update count.
        replica_chunk_ext = PpdbReplicaChunkExtended.from_replica_chunk(
            replica_chunk, status, chunk_dir, len(update_records)
        )
        try:
            self.store_chunk(replica_chunk_ext, False)
        except Exception as e:
            _LOG.exception("Failed to store replica chunk info in database for %s", replica_chunk.id)
            raise e

        _LOG.info("Done processing %s", replica_chunk.id)

    def _create_chunk_dir(self, chunk: ReplicaChunk) -> Path:
        """Create the directory for the replica chunk based on its last update
        time and ID.

        Parameters
        ----------
        chunk
            The replica chunk for which to create the directory.

        Returns
        -------
        `pathlib.Path`
            Path to the created directory for the replica chunk.
        """
        last_update_time = chunk.last_update_time.to_datetime()
        assert isinstance(last_update_time, datetime.datetime)
        chunk_dir = Path(
            self.config.replication_path,
            chunk.last_update_time.strftime("%Y/%m/%d"),
            str(chunk.id),
        )
        if chunk_dir.exists():
            if not self.config.delete_existing_dirs:
                raise FileExistsError(f"Directory already exists for {chunk.id}: {chunk_dir}")
            _LOG.warning("Overwriting existing directory for %s: %s", chunk.id, chunk_dir)
            shutil.rmtree(chunk_dir)

        chunk_dir.mkdir(parents=True)
        _LOG.info("Created directory for %s: %s", chunk.id, chunk_dir)

        return chunk_dir

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
        status
            Status of the replica chunks to return.
        start_chunk_id
            If provided, only return chunks with ID greater than or equal to
            this value.

        Returns
        -------
        `~collections.abc.Sequence` [ `PpdbReplicaChunkExtended` ]
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
            table.columns["gcs_uri"],  # Extended column
            table.columns["update_count"],  # Extended column
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
                        gcs_uri=row[6],
                        update_count=row[7],
                    )
                )
        return ids

    def get_replica_chunks_ext_by_ids(self, chunk_ids: Sequence[int]) -> Sequence[PpdbReplicaChunkExtended]:
        """Find replica chunks for a list of chunk IDs.

        Parameters
        ----------
        chunk_ids
            Replica chunk IDs to retrieve.

        Returns
        -------
        `~collections.abc.Sequence` [ `PpdbReplicaChunkExtended` ]
            List of matching chunks ordered by ``apdb_replica_chunk``.
        """
        if not chunk_ids:
            return []

        table = self.get_table("PpdbReplicaChunk")
        query = (
            sqlalchemy.sql.select(
                table.columns["apdb_replica_chunk"],
                table.columns["last_update_time"],
                table.columns["unique_id"],
                table.columns["replica_time"],
                table.columns["status"],
                table.columns["directory"],
                table.columns["gcs_uri"],
                table.columns["update_count"],
            )
            .where(table.columns["apdb_replica_chunk"].in_(chunk_ids))
            .order_by(table.columns["apdb_replica_chunk"])
        )

        chunks: list[PpdbReplicaChunkExtended] = []
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
            for row in result:
                last_update_time = self.to_astropy_tai(row[1])
                replica_time = self.to_astropy_tai(row[3])
                chunks.append(
                    PpdbReplicaChunkExtended(
                        id=row[0],
                        last_update_time=last_update_time,
                        unique_id=row[2],
                        replica_time=replica_time,
                        status=row[4],
                        directory=Path(row[5]),
                        gcs_uri=row[6],
                        update_count=row[7],
                    )
                )
        return chunks

    def store_chunk(self, replica_chunk: PpdbReplicaChunkExtended, update: bool) -> None:
        """Insert or replace single record in PpdbReplicaChunk table, including
        the status and directory of the replica chunk.

        Parameters
        ----------
        replica_chunk
            The replica chunk to store.
        update
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
                "gcs_uri": replica_chunk.gcs_uri,
                "update_count": replica_chunk.update_count,
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
        table_name
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
                # Status of the chunk export process (e.g. pending, exported,
                # skipped).
                schema_model.Column(
                    name="status",
                    id=f"#{table_name}.status",
                    datatype=felis.datamodel.DataType.string,
                ),
                # Local directory where the chunk data is stored for export.
                schema_model.Column(
                    name="directory",
                    id=f"#{table_name}.directory",
                    datatype=felis.datamodel.DataType.string,
                    nullable=True,  # We might want to allow NULL if an error occurs when exporting.
                ),
                # URI of the chunk data in GCS after it has been uploaded.
                # This is not a full object path but a prefix ("directory")
                # under which the manifest and parquet files for the chunk
                # are located.
                schema_model.Column(
                    name="gcs_uri",
                    id=f"#{table_name}.gcs_uri",
                    datatype=felis.datamodel.DataType.string,
                    nullable=True,
                ),
                # Count of update records included in the chunk.
                schema_model.Column(
                    name="update_count",
                    id=f"#{table_name}.update_count",
                    datatype=felis.datamodel.DataType.int,
                    nullable=False,
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
        db_url
            Database URL in SQLAlchemy format for PPDB instance.
        project_id
            GCP project ID.
        dataset_id
            BigQuery dataset name without the project ID.
        bucket_name
            GCS bucket name to use for Parquet output.
        object_prefix
            Object prefix to use in GCS bucket for Parquet output.
        replication_dir
            Directory used for replication staging area.
        db_drop
            If True then drop existing db tables.
        db_schema
            Database schema name for PPDB instance.
        felis_path
            Path to Felis database. If `None`, defaults to the default path in
            SDM Schemas.
        felis_schema
            Felis schema name within the YAML file.
        stage_chunk_topic
            Pub/Sub topic to use for staging chunks.
        parq_batch_size
            Number of rows to use when batching Parquet output.
        parq_compression
            Compression codec to use for Parquet output.
        delete_existing_dirs
            If True then delete existing replication staging directories.
        validate_config
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

        password_provider: PasswordProvider | None = None
        if os.getenv("PPDB_USE_SECRET_MANAGER", "false").lower() == "true":
            _LOG.info("Using Secret Manager to retrieve database password")
            password_provider = _SecretManagerPasswordProvider(bq_config.project_id)
        engine = cls.make_engine(bq_config.sql, password_provider=password_provider)
        cls.make_database(engine, bq_config.sql, sa_metadata, schema_version, db_drop)

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
        config
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

    def _handle_updates(
        self, replica_chunk: ReplicaChunk, apdb_update_records: Collection[ApdbUpdateRecord], chunk_dir: Path
    ) -> None:
        """Handle updates to existing records in the PPDB by writing a JSON
        file with the update information for the replica chunk.

        Parameters
        ----------
        replica_chunk
            The replica chunk associated with the updates.
        update_records
            Collection of update records to process.

        Notes
        -----
        Serializes the ApdbUpdateRecord objects into a dictionary structure
        for processing.
        """
        update_records = UpdateRecords(
            records=list(apdb_update_records),
        )
        parquet_path = chunk_dir / UpdateRecords.PARQUET_FILE_NAME
        update_records.write_parquet_file(parquet_path)

        _LOG.info(
            "Saved %d update records for %s to %s",
            len(update_records.records),
            replica_chunk.id,
            parquet_path,
        )

    def get_promotable_chunks(self) -> list[int]:
        """Return the first uninterrupted sequence of staged chunks such that
        all prior chunks are promoted.

        Returns
        -------
        `list` [`int`]
            A list of tuples containing the ``apdb_replica_chunk`` values of
            the promotable chunks.

        Notes
        -----
        This query finds the contiguous sequence of ``staged`` chunks beginning
        with the earliest chunk that is not yet ``promoted``, and ending just
        before the first chunk that is not ``staged``. If no such ending
        exists, all ``staged`` chunks from that point onward are returned. If
        no chunks are ``staged`` after the first non-``promoted`` chunk, an
        empty list is returned.
        """
        table = self.get_table("PpdbReplicaChunk")
        if not table.schema:
            raise ValueError("Table schema is not set, cannot construct query")
        quoted_table_name = (
            self._engine.dialect.identifier_preparer.quote(table.schema)
            + "."
            + self._engine.dialect.identifier_preparer.quote(table.name)
        )

        sql = SqlResource("select_promotable_chunks", {"table_name": quoted_table_name}).sql

        with self._engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(sql))
            chunk_ids = [row[0] for row in result]
        return chunk_ids

    def mark_chunks_promoted(self, promotable_chunks: list[int]) -> int:
        """Set status='promoted' for the given chunk IDs. Returns number
        updated.

        Parameters
        ----------
        promotable_chunks
            List of integers containing the ``apdb_replica_chunk`` values of
            the promotable chunks.

        Returns
        -------
        `int`
            The number of rows updated in the database, which should be equal
            to the number of promotable chunks provided, if they were all found
            and updated successfully.
        """
        table = self.get_table("PpdbReplicaChunk")
        stmt = (
            sqlalchemy.update(table)
            .where(table.c.apdb_replica_chunk.in_(promotable_chunks), table.c.status != "promoted")
            .values(status="promoted")
        )

        with self._engine.begin() as conn:
            result: sqlalchemy.engine.CursorResult = conn.execute(stmt)
            return result.rowcount or 0

    def update(self, chunk_id: int, values: dict[str, Any]) -> int:
        """Update an existing replica chunk in the database.

        Parameters
        ----------
        chunk_id
            The ID of the replica chunk to update.
        values
            A dictionary of column names and their new values to update.

        Returns
        -------
        `int`
            The number of rows updated. This should be 1 if the update is
            successful, or 0 if no rows were updated (e.g., if the chunk ID
            does not exist or the status is already set to the new value).
        """
        logging.info("Preparing to update replica chunk %d with values: %s", chunk_id, values)
        table = self.get_table("PpdbReplicaChunk")
        stmt = sqlalchemy.update(table).where(table.c.apdb_replica_chunk == chunk_id).values(values)
        with self._engine.begin() as conn:
            result = conn.execute(stmt)
            affected_rows = result.rowcount

        new_status = values.get("status")
        if affected_rows == 0:
            logging.warning(
                "No rows updated for replica chunk %s with status '%s'",
                chunk_id,
                new_status,
            )
        else:
            logging.info(
                "Successfully updated %d row(s) for replica chunk %s to status '%s'",
                affected_rows,
                chunk_id,
                new_status,
            )
        return affected_rows
