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

from ..sql._ppdb_sql import PpdbSqlConfig


# DM-52173: Due to the class structure of ChunkExporter, the config needs to
# inherit from PpdbSqlConfig. This should be refactored in the future so that
# it is standalone.
class PpdbBigQueryConfig(PpdbSqlConfig):
    """Configuration for BigQuery-based PPDB."""

    apdb_schema_uri: str = "resource://lsst.sdm.schemas/apdb.yaml"
    """URI of the APDB schema definition."""

    replica_chunk_table: str = "PpdbReplicaChunk"
    """Name of the table used to track replica chunks."""

    directory: Path | None = None
    """Directory where the exported chunks will be stored."""

    delete_existing: bool = False
    """If `True`, existing directories for chunks will be deleted before
    export. If `False`, an error will be raised if the directory already
    exists."""

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
    """Target BigQuery dataset, e.g., 'my_project:my_dataset'. If not provided
    the project will be derived from the environment."""
