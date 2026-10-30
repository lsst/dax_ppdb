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

__all__ = ["PpdbBigQuery", "UpdatableField"]

import datetime
import json
import logging
import os
import shutil
from collections.abc import Collection, Iterable, Sequence
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Self

import astropy.time
import felis
import sqlalchemy
from google.cloud.sql.connector import Connector
from sqlalchemy.engine.interfaces import DBAPIConnection

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
from ..sql import PpdbSqlBase, PpdbSqlBaseConfig
from .manifest import Manifest, ParquetFileEntry
from .ppdb_bigquery_config import PpdbBigQueryConfig
from .ppdb_replica_chunk_extended import ChunkStatus, PpdbReplicaChunkExtended
from .updates.update_records import UpdateRecords

_LOG = logging.getLogger(__name__)

_MON = monitor.MonAgent(__name__)


VERSION = VersionTuple(0, 1, 0)
"""Version for the code defined in this module. This needs to be updated
(following compatibility rules) when schema produced by this code changes.
"""


class UpdatableField(StrEnum):
    """Chunk fields that are allowed to be updated on existing chunks."""

    STATUS = "status"
    GCS_URI = "gcs_uri"
    ERROR_MESSAGE = "error_message"
    BIGQUERY_JOB_IDS = "bigquery_job_ids"


class PpdbBigQuery(Ppdb, PpdbSqlBase):
    """Provides management operations for the BigQuery-based PPDB.

    Parameters
    ----------
    config
        Configuration object with BigQuery and SQL database parameters.

    Notes
    -----
    Whether the SQLAlchemy engine uses the Cloud SQL Python Connector is
    controlled by the ``CLOUDSQL_ENABLED`` environment variable; see
    `is_cloudsql_enabled` and `make_engine`.

    Use this interface as a context manager or call `close` when finished
    to release database connections and the Cloud SQL connector.
    """

    # ----------------------------------------------------------------------
    # Construction and properties
    # ----------------------------------------------------------------------

    def __init__(self, config: PpdbBigQueryConfig):
        # Read parameters from config.
        if config.replication_dir is None:
            raise ValueError("Directory for chunk export is not set in configuration.")

        # Delegate SQL initialisation (schema load, engine, metadata, version
        # checks) to the base class. Whether the Cloud SQL Connector is used is
        # determined from the environment (see `is_cloudsql_enabled`) inside
        # `make_engine`.
        PpdbSqlBase.__init__(self, config.sql)

        self._config = config

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Release database connections and the Cloud SQL connector."""
        self._engine.dispose()

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

    @property
    def chunk_table(self) -> sqlalchemy.schema.Table:
        """The SQL table object for the replica chunk table."""
        return self.get_table("PpdbReplicaChunk")

    # ----------------------------------------------------------------------
    # Factory and initialization class methods
    # ----------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> PpdbBigQuery:
        """Create an instance of this class from a config pointed to by an
        environment variable.

        Returns
        -------
        `PpdbBigQuery`
            An instance of the PPDB BigQuery interface.

        Raises
        ------
        OSError
            Raised if ``PPDB_CONFIG_URI`` is not set in the environment.
        ValueError
            Raised if the configuration is not for a BigQuery PPDB.

        Notes
        -----
        Whether the Cloud SQL Python Connector is used is controlled by the
        ``CLOUDSQL_ENABLED`` environment variable; see `is_cloudsql_enabled`.
        """
        ppdb_config_uri = os.environ.get("PPDB_CONFIG_URI", None)
        if ppdb_config_uri:
            logging.debug("PPDB_CONFIG_URI: %s", ppdb_config_uri)
        else:
            raise OSError("PPDB_CONFIG_URI is not set in the environment")
        config = PpdbBigQueryConfig.from_uri(ppdb_config_uri)
        if not isinstance(config, PpdbBigQueryConfig):
            raise ValueError(f"Config from environment has wrong type: {type(config)}")
        return cls(config)

    @classmethod
    def init_bigquery(
        cls,
        db_url: str,
        project_id: str,
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
    ) -> PpdbBigQueryConfig:
        """Initialize PPDB database and return configuration object.

        Parameters
        ----------
        db_url
            Database URL in SQLAlchemy format for PPDB instance.
        project_id
            GCP project ID.
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

        engine = cls.make_engine(bq_config.sql)
        cls.make_database(engine, bq_config.sql, sa_metadata, schema_version, db_drop)

        return bq_config

    # ----------------------------------------------------------------------
    # Engine creation
    # ----------------------------------------------------------------------

    @classmethod
    def is_cloudsql_enabled(cls) -> bool:
        """Return whether the Cloud SQL Python Connector is enabled.

        Returns
        -------
        `bool`
            `True` if the ``CLOUDSQL_ENABLED`` environment variable is set to
            ``"true"`` (case-insensitive), `False` otherwise.
        """
        return os.environ.get("CLOUDSQL_ENABLED", "false").lower() == "true"

    @classmethod
    def make_engine(cls, config: PpdbSqlBaseConfig) -> sqlalchemy.engine.Engine:
        """Make a SQLAlchemy engine, optionally using the Cloud SQL Connector.

        Whether the Cloud SQL Python Connector is used is determined by the
        ``CLOUDSQL_ENABLED`` environment variable (see `is_cloudsql_enabled`).
        When enabled, the connection parameters are read from the environment:

        - ``CLOUDSQL_INSTANCE_CONNECTION_NAME``: instance connection name in
          the form ``project:region:instance`` (required).
        - ``CLOUDSQL_USER``: database user or IAM principal (required).
        - ``CLOUDSQL_DB_NAME``: database name (required).
        - ``CLOUDSQL_DRIVERNAME``: Cloud SQL Connector database driver.
          Defaults to ``pg8000``.
        - ``CLOUDSQL_IP_TYPE``: IP type used to reach the instance, one of
          ``private`` (default), ``public``, or ``psc``.

        Authentication uses automatic IAM database authentication.

        The caller must dispose the returned engine when finished.

        Parameters
        ----------
        config
            Configuration object with SQL parameters.

        Returns
        -------
        `sqlalchemy.engine.Engine`
            The SQLAlchemy engine.
        """
        if cls.is_cloudsql_enabled():
            return cls._make_connector_engine()
        return super().make_engine(config)

    @classmethod
    def _make_connector_engine(cls) -> sqlalchemy.engine.Engine:
        """Create a SQLAlchemy engine using the Cloud SQL Python Connector.

        The connection parameters are read from the environment; see
        `make_engine` for the environment variables that are used.
        Authentication uses automatic IAM database authentication.

        Returns
        -------
        `sqlalchemy.engine.Engine`
            The SQLAlchemy engine backed by the Cloud SQL Connector.

        Raises
        ------
        OSError
            Raised if a required environment variable is not set.

        Notes
        -----
        The connector is created with a lazy refresh strategy, which is
        recommended for serverless environments such as Cloud Run and Cloud
        Functions where CPU may be throttled between requests. A reference to
        the connector is retained by the returned engine's connection factory,
        so it remains alive until the engine is disposed. Disposal closes
        the connector after closing pooled database connections.
        """
        instance_connection_name = os.environ.get("CLOUDSQL_INSTANCE_CONNECTION_NAME")
        if not instance_connection_name:
            raise OSError("CLOUDSQL_INSTANCE_CONNECTION_NAME is not set in the environment")
        db_user = os.environ.get("CLOUDSQL_USER")
        if not db_user:
            raise OSError("CLOUDSQL_USER is not set in the environment")
        db_name = os.environ.get("CLOUDSQL_DB_NAME")
        if not db_name:
            raise OSError("CLOUDSQL_DB_NAME is not set in the environment")
        driver = os.environ.get("CLOUDSQL_DRIVERNAME", "pg8000")

        # The IP type may be overridden via the environment; it defaults to
        # private, which is the recommended production configuration.
        ip_type = os.environ.get("CLOUDSQL_IP_TYPE", "private").lower()
        if ip_type not in ("private", "public", "psc"):
            raise OSError(
                f"Invalid CLOUDSQL_IP_TYPE value: {ip_type!r} (expected 'private', 'public', or 'psc')"
            )
        connect_kwargs: dict[str, str | bool] = {
            "user": db_user,
            "db": db_name,
            "ip_type": ip_type,
            "enable_iam_auth": True,
        }
        _LOG.debug("Using IAM authentication for Cloud SQL Connector")

        # Lazy refresh is recommended for serverless environments where CPU may
        # be throttled between requests.
        connector = Connector(refresh_strategy="lazy")

        def getconn() -> DBAPIConnection:
            return connector.connect(instance_connection_name, driver, **connect_kwargs)

        _LOG.debug(
            "Creating Cloud SQL Connector engine for %s (driver=%s)",
            instance_connection_name,
            driver,
        )
        engine = sqlalchemy.create_engine(f"postgresql+{driver}://", creator=getconn)

        def close_connector(_: sqlalchemy.engine.Engine) -> None:
            """Close the cloud sql connector when the engine is cleaned up."""
            connector.close()

        sqlalchemy.event.listen(engine, "engine_disposed", close_connector, once=True)
        return engine

    # ----------------------------------------------------------------------
    # SQL schema and versioning class methods
    # ----------------------------------------------------------------------

    @classmethod
    def get_meta_code_version_key(cls) -> str:
        # Docstring is inherited.
        return "version:PpdbBigQuery"

    @classmethod
    def get_code_version(cls) -> VersionTuple:
        # Docstring is inherited.
        return VERSION

    @classmethod
    def filter_table_names(cls, original_table_names: Iterable[str]) -> Iterable[str]:
        # Docstring is inherited.
        # Only the metadata table is needed for the BigQuery-based PPDB.
        return ["metadata"]

    @classmethod
    def create_replica_chunk_table(cls, table_name: str | None = None) -> schema_model.Table:
        """Create the ``PpdbReplicaChunk`` table with additional fields for
        replication tracking.

        Parameters
        ----------
        table_name
            Name of the table to create. If not provided, defaults to
            "PpdbReplicaChunk".

        Notes
        -----
        This overrides the base method to add additional columns for
        ``status``, ``gcs_uri``, ``update_count``, ``status_time``,
        ``error_message`` and ``bigquery_job_ids`` to the replica chunk table
        schema.
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
                # Time of the last status change.
                schema_model.Column(
                    name="status_time",
                    id=f"#{table_name}.status_time",
                    datatype=felis.datamodel.DataType.timestamp,
                    nullable=True,
                ),
                # Reason the chunk failed, set only when the status is failed.
                schema_model.Column(
                    name="error_message",
                    id=f"#{table_name}.error_message",
                    datatype=felis.datamodel.DataType.text,
                    nullable=True,
                ),
                # JSON object with the BigQuery job IDs of the last staging
                # attempt, keyed by table name plus "commit".
                schema_model.Column(
                    name="bigquery_job_ids",
                    id=f"#{table_name}.bigquery_job_ids",
                    datatype=felis.datamodel.DataType.text,
                    nullable=True,
                ),
            ]
        )
        return replica_chunk_table

    # ----------------------------------------------------------------------
    # Public API -- chunk lifecycle (store)
    # ----------------------------------------------------------------------

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
            chunk_dir = self._make_chunk_dir(replica_chunk)

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
                manifest = self._generate_manifest(replica_chunk, table_dict, update_records, chunk_dir)
                _LOG.debug("Generated manifest for %s: %s", replica_chunk.id, manifest.model_dump_json())
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

        if manifest.is_empty_chunk:
            # Mark as skipped if there is no data to export.
            status = ChunkStatus.SKIPPED
            _LOG.warning("No data to export for %s, marking chunk as skipped", replica_chunk.id)
        else:
            status = ChunkStatus.EXPORTED

        # Store the replica chunk info in the database, including status,
        # and update count.
        replica_chunk_ext = PpdbReplicaChunkExtended.from_replica_chunk(
            replica_chunk, status, len(update_records)
        )
        try:
            self.insert_chunks([replica_chunk_ext])
        except Exception as e:
            _LOG.exception("Failed to store replica chunk info in database for %s", replica_chunk.id)
            raise e

        _LOG.info("Done processing %s", replica_chunk.id)

    # ----------------------------------------------------------------------
    # Public API -- chunk queries
    # ----------------------------------------------------------------------

    def get_replica_chunks(self, start_chunk_id: int | None = None) -> Sequence[PpdbReplicaChunk] | None:
        # Docstring is inherited.
        where_clauses: list[sqlalchemy.ColumnElement] = []
        if start_chunk_id is not None:
            where_clauses.append(self.chunk_table.columns["apdb_replica_chunk"] >= start_chunk_id)
        return self.query_chunks(*where_clauses, order_by=self.chunk_table.columns["last_update_time"])

    def query_chunks(
        self,
        *where_clauses: sqlalchemy.ColumnElement,
        order_by: sqlalchemy.ColumnElement | None = None,
    ) -> list[PpdbReplicaChunkExtended]:
        """Query the ``PpdbReplicaChunk`` table with arbitrary filters.

        Parameters
        ----------
        *where_clauses
            SQLAlchemy column expressions to use as WHERE filters. Each
            expression is applied with AND semantics.
        order_by
            Column expression to order results by. Defaults to
            ``apdb_replica_chunk`` if not provided.

        Returns
        -------
        `list` [ `PpdbReplicaChunkExtended` ]
            List of matching chunks.
        """
        if order_by is None:
            order_by = self.chunk_table.columns["apdb_replica_chunk"]
        query = sqlalchemy.sql.select(self.chunk_table).order_by(order_by)
        for clause in where_clauses:
            query = query.where(clause)
        chunks: list[PpdbReplicaChunkExtended] = []
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
            for row in result.mappings():
                chunks.append(
                    PpdbReplicaChunkExtended(
                        id=row["apdb_replica_chunk"],
                        last_update_time=astropy.time.Time(
                            row["last_update_time"], format="datetime", scale="tai"
                        ),
                        unique_id=row["unique_id"],
                        replica_time=astropy.time.Time(row["replica_time"], format="datetime", scale="tai"),
                        status=ChunkStatus(row["status"]),
                        gcs_uri=row["gcs_uri"],
                        update_count=row["update_count"],
                        status_time=row["status_time"],
                        error_message=row["error_message"],
                        bigquery_job_ids=(
                            json.loads(row["bigquery_job_ids"])
                            if row["bigquery_job_ids"] is not None
                            else None
                        ),
                    )
                )
        return chunks

    def get_promotable_chunks(self) -> list[PpdbReplicaChunkExtended]:
        """Return the first uninterrupted sequence of staged chunks such that
        all prior chunks are promoted.

        Returns
        -------
        `list` [`PpdbReplicaChunkExtended`]
            Promotable chunks in order.

        Notes
        -----
        Finds the contiguous sequence of ``staged`` chunks beginning with the
        earliest chunk that is not yet ``promoted`` or ``skipped``, and ending
        just before the first chunk that is not ``staged``. If no such ending
        exists, all ``staged`` chunks from that point onward are returned. If
        no chunks are ``staged`` after the first non-``promoted`` chunk, an
        empty list is returned.
        """
        chunks = self.query_chunks(
            self.chunk_table.columns["status"].notin_(
                [ChunkStatus.PROMOTED.value, ChunkStatus.SKIPPED.value]
            ),
            order_by=self.chunk_table.columns["apdb_replica_chunk"],
        )
        promotable: list[PpdbReplicaChunkExtended] = []
        for chunk in chunks:
            if chunk.status == ChunkStatus.STAGED:
                promotable.append(chunk)
            else:
                break
        return promotable

    def find_chunk_by_id(self, chunk_id: int) -> PpdbReplicaChunkExtended | None:
        """Find a replica chunk by its ID.

        Parameters
        ----------
        chunk_id
            The ID of the replica chunk to find.

        Returns
        -------
        `PpdbReplicaChunkExtended` or `None`
            The matching replica chunk, or `None` if not found.
        """
        chunks = self.query_chunks(self.chunk_table.columns["apdb_replica_chunk"] == chunk_id)
        return chunks[0] if chunks else None

    # ----------------------------------------------------------------------
    # Public API -- chunk mutations
    # ----------------------------------------------------------------------

    def insert_chunks(self, chunks: Sequence[PpdbReplicaChunkExtended]) -> None:
        """Insert one or more new replica chunks into the
        ``PpdbReplicaChunk`` table.

        Parameters
        ----------
        chunks
            Replica chunks to insert.

        Raises
        ------
        ValueError
            Raised if ``chunks`` is empty.
        `sqlalchemy.exc.IntegrityError`
            Raised if any chunk already exists in the table.
        """
        if not chunks:
            raise ValueError("chunks must not be empty")
        table = self.chunk_table
        with self._engine.begin() as connection:
            connection.execute(table.insert(), [chunk.to_row() for chunk in chunks])

    def update_chunks(self, chunks: Sequence[PpdbReplicaChunkExtended], fields: set[UpdatableField]) -> int:
        """Update one or more existing replica chunks in the
        ``PpdbReplicaChunk`` table.

        Parameters
        ----------
        chunks
            Replica chunks with updated values. Each chunk must already
            exist in the table.
        fields
            Set of field names to update.

        Raises
        ------
        ValueError
            Raised if ``fields`` is empty or contains invalid field names,
            or if ``chunks`` is empty.
        LookupError
            Raised if any chunk does not exist in the table.
        """
        if not chunks:
            raise ValueError("chunks must not be empty")
        if not fields:
            raise ValueError("fields must not be empty")
        table = self.chunk_table
        updated_count = 0
        with self._engine.begin() as connection:
            for chunk in chunks:
                row = chunk.to_row()
                values: dict[str, object] = {k: v for k, v in row.items() if k in fields}
                if UpdatableField.STATUS in fields:
                    values["status_time"] = sqlalchemy.func.now()
                result = connection.execute(
                    table.update().where(table.c.apdb_replica_chunk == chunk.id).values(**values)
                )
                if result.rowcount == 0:
                    raise LookupError(f"Chunk {chunk.id} does not exist")
                updated_count += result.rowcount
        return updated_count

    def update_chunk_status(
        self,
        chunk_ids: Collection[int],
        status: ChunkStatus,
        *,
        expected: Collection[ChunkStatus],
        **values: object,
    ) -> bool:
        """Set the status of one or more replica chunks in a single
        transaction, only if each chunk's current status is expected.

        Parameters
        ----------
        chunk_ids
            IDs of the replica chunks.
        status
            New status.
        expected
            Statuses the chunks must currently have for the update to apply.
        **values
            Other columns to set, by `UpdatableField` name.

        Returns
        -------
        `bool`
            `True` if every chunk was updated. `False` if any chunk does not
            exist or does not have an expected status, in which case no chunk
            is updated.

        Notes
        -----
        ``status_time`` is set to the database's current time. Meant to be
        called by `ChunkTracker`, which decides the expected statuses and the
        other values.
        """
        ids = set(chunk_ids)
        table = self.chunk_table
        statement = (
            table.update()
            .where(table.c.apdb_replica_chunk.in_(ids))
            .where(table.c.status.in_([s.value for s in expected]))
            .values(status=status.value, status_time=sqlalchemy.func.now(), **values)
        )
        with self._engine.connect() as connection:
            with connection.begin() as transaction:
                result = connection.execute(statement)
                if result.rowcount != len(ids):
                    transaction.rollback()
                    return False
        return True

    # ----------------------------------------------------------------------
    # Private helpers
    # ----------------------------------------------------------------------

    def _generate_manifest(
        self,
        replica_chunk: ReplicaChunk,
        table_dict: dict[str, ApdbTableData],
        update_records: Collection[ApdbUpdateRecord],
        chunk_dir: Path,
    ) -> Manifest:
        """Generate the manifest data for the replica chunk.

        Parameters
        ----------
        replica_chunk
            The replica chunk for which to generate the manifest.
        table_dict
            Dictionary mapping table names to their corresponding data for the
            chunk.
        update_records
            Collection of update records included in the chunk.
        chunk_dir
            Local directory where chunk parquet files were written.

        Returns
        -------
        Manifest
            The manifest created from the input data.
        """
        # Add file data for each table if it has rows.
        file_entries = {
            f"{table_name}.parquet": ParquetFileEntry.from_path(
                path=chunk_dir / f"{table_name}.parquet",
                row_count=row_count,
            )
            for table_name, data in table_dict.items()
            if (row_count := len(data.rows())) > 0
        }

        # Add file data for the update records if they exist.
        if update_records:
            file_entries[UpdateRecords.PARQUET_FILE_NAME] = ParquetFileEntry.from_path(
                path=chunk_dir / UpdateRecords.PARQUET_FILE_NAME,
                row_count=len(update_records),
            )

        return Manifest(
            replica_chunk_id=str(replica_chunk.id),
            unique_id=replica_chunk.unique_id,
            schema_version=str(self.schema_version),
            exported_at=datetime.datetime.now(datetime.UTC),
            last_update_time=str(replica_chunk.last_update_time),  # TAI value
            files=file_entries,
            compression_format=self.config.parq_compression,
        )

    def _make_chunk_dir(self, chunk: ReplicaChunk) -> Path:
        """Make the directory for the replica chunk within the replication
        staging area.

        Parameters
        ----------
        chunk
            The replica chunk for which to create the directory.

        Returns
        -------
        `pathlib.Path`
            Path to the created directory for the replica chunk.
        """
        chunk_dir = self.config.chunk_dir(chunk.id)
        if chunk_dir.exists():
            if not self.config.delete_existing_dirs:
                raise FileExistsError(f"Directory already exists for {chunk.id}: {chunk_dir}")
            _LOG.warning("Overwriting existing directory for %s: %s", chunk.id, chunk_dir)
            shutil.rmtree(chunk_dir)

        chunk_dir.mkdir(parents=True)
        _LOG.debug("Created directory for %s: %s", chunk.id, chunk_dir)

        return chunk_dir

    def _handle_updates(
        self, replica_chunk: ReplicaChunk, apdb_update_records: Collection[ApdbUpdateRecord], chunk_dir: Path
    ) -> None:
        """Handle updates to existing records in the PPDB by writing a parquet
        file with the update information for the replica chunk.

        Parameters
        ----------
        replica_chunk
            The replica chunk associated with the updates.
        apdb_update_records
            Collection of update records to process.
        chunk_dir
            The directory containing the chunk's data.
        """
        update_records = UpdateRecords(
            records=[(replica_chunk.id, record) for record in apdb_update_records],
        )
        parquet_path = chunk_dir / UpdateRecords.PARQUET_FILE_NAME
        update_records.write_parquet_file(parquet_path)

        _LOG.info(
            "Saved %d update records for %s to %s",
            len(update_records.records),
            replica_chunk.id,
            parquet_path,
        )
