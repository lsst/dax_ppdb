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

from ..ppdb import PpdbConfig
from ..sql.ppdb_sql_config import PpdbSqlConfig


class PpdbBigQueryConfig(PpdbConfig):
    """Configuration for BigQuery-based PPDB.

    This also includes configuration for the SQL database used to track
    replica chunks under the `sql` attribute.
    """

    directory: Path | None = None
    """Directory where the exported chunks will be stored
    (`Path` or `None`)."""

    delete_existing: bool = False
    """If `True`, existing directories for chunks will be deleted before
    export. If `False`, an error will be raised if the directory already
    exists."""

    stage_chunk_topic: str = "stage-chunk-topic"
    """Pub/Sub topic name for triggering chunk staging process (`str`)."""

    batch_size: int = 1000
    """Number of rows to process in each batch when writing parquet files
    (`int`, defaults to 1000)."""

    compression_format: str = "snappy"
    """Compression format for Parquet files (`str`, defaults to "snappy")."""

    bucket: str | None = None
    """Name of Google Cloud Storage bucket for uploading chunks (`str`)."""

    prefix: str | None = None
    """Base prefix for the object in cloud storage (`str`)."""

    dataset: str | None = None
    """Target BigQuery dataset, e.g., 'my_project:my_dataset'
    (`str` or `None`). If not provided the project will be derived from the
    Google Cloud environment at runtime."""

    sql: PpdbSqlConfig | None = None
    """SQL database configuration (`SqlConfig` or `None`)."""
