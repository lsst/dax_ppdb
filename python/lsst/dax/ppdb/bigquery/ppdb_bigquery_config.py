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

from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel

from ..ppdb_config import PpdbConfig
from ..sql import PpdbSqlBaseConfig

__all__ = [
    "DatasetType",
    "Datasets",
    "PpdbBigQueryConfig",
]

_PPDB_PREFIX = "ppdb"


class DatasetType(StrEnum):
    """Typed keys for BigQuery dataset categories."""

    STAGING = "staging"
    INTERNAL = "internal"
    PROMOTION = "promotion"
    PUBLIC = "public"


class Datasets(BaseModel):
    """Mapping of dataset types to their target names in BigQuery."""

    staging: str = f"{_PPDB_PREFIX}_staging"
    """Name of the staging dataset used for intermediate storage during
    replication and promotion processes."""

    internal: str = f"{_PPDB_PREFIX}_internal"
    """Name of the internal dataset containing the fully replicated and
    promoted data."""

    promotion: str = f"{_PPDB_PREFIX}_promotion"
    """Name of the promotion dataset used as a workspace for building new
    internal tables."""

    public: str = f"{_PPDB_PREFIX}_public"
    """Name of the public dataset which is presented through the TAP
    interface."""

    def name_for(self, dataset_type: DatasetType) -> str:
        """Return the configured dataset name for a dataset type."""
        match dataset_type:
            case DatasetType.INTERNAL:
                return self.internal
            case DatasetType.PROMOTION:
                return self.promotion
            case DatasetType.PUBLIC:
                return self.public
            case DatasetType.STAGING:
                return self.staging

        # Defensive check in case a bad value was passed at runtime.
        raise ValueError(f"Unsupported dataset type: {dataset_type!r}")


_DEFAULT_FELIS_SCHEMA_URI = "resource://lsst.sdm.schemas/ppdb.yaml"
"""Default URI for the Felis PPDB schema in SDM Schemas."""


class PpdbBigQueryConfig(PpdbConfig):
    """Configuration for BigQuery-based PPDB."""

    felis_schema_uri: str = _DEFAULT_FELIS_SCHEMA_URI
    """URI for the FELIS schema used in the PPDB."""

    project_id: str
    """Google Cloud project ID."""

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
    """SQL database configuration (`~lsst.dax.ppdb.sql.PpdbSqlBaseConfig`)."""

    datasets: Datasets = Datasets()
    """Names of the BigQuery datasets used for different purposes
    (`Datasets`).
    """

    @property
    def replication_path(self) -> Path:
        """Return path for writing replica chunk data (`pathlib.Path`)."""
        return Path(self.replication_dir)

    def chunk_dir(self, chunk_id: int) -> Path:
        """Return the local directory for a replica chunk's data.

        Parameters
        ----------
        chunk_id
            ID of the replica chunk.

        Returns
        -------
        `pathlib.Path`
            Path to the chunk's local directory, formed by convention from
            the replication path and the chunk ID.
        """
        return self.replication_path / str(chunk_id)

    def fqn_for(self, dataset_type: DatasetType, table_name: str | None = None) -> str:
        """Return the fully qualified BigQuery dataset or table name (if
        provided) for a dataset type.

        Parameters
        ----------
        dataset_type
            Type of dataset to get the name for.
        table_name
            Optional table name to include in the fully qualified name.

        Returns
        -------
        str
            Fully qualified BigQuery dataset or table name.
        """
        dataset_name = self.datasets.name_for(dataset_type)
        fqn = f"{self.project_id}.{dataset_name}"
        if table_name:
            fqn = f"{fqn}.{table_name}"
        return fqn
