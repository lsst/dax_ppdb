
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

from pathlib import Path

from ..ppdb_config import PpdbConfig
from ..sql import PpdbSqlBaseConfig

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
    """SQL database configuration (`~lsst.dax.ppdb.sql.PpdbSqlBaseConfig`)."""

    @property
    def replication_path(self) -> Path:
        """Return path for writing replica chunk data (`pathlib.Path`)."""
        return Path(self.replication_dir)

    @property
    def fq_dataset_id(self) -> str:
        """Fully qualified BigQuery dataset ID, including project (`str`)."""
        return f"{self.project_id}:{self.dataset_id}"