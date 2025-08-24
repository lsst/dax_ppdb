from pathlib import Path

from ..sql._ppdb_sql import PpdbSqlConfig


# DM-52173: Due to the class structure of ChunkExporter, the config needs to
# inherit from PpdbSqlConfig. This should be refactored in the future so that
# it is is standalone.
class PpdbBigQueryConfig(PpdbSqlConfig):
    """Configuration for BigQuery-based PPDB."""

    directory: Path
    """Directory where the exported chunks will be stored."""

    # FIXME: This should go away in favor of direct db writes.
    topic_name: str = "track-chunk-topic"
    """Pub/Sub topic name for tracking chunk exports."""

    batch_size: int = 1000
    """Number of rows to process in each batch when writing parquet files."""

    compression_format: str = "snappy"
    """Compression format for Parquet files."""

    delete_existing: bool = False
    """If `True`, existing directories for chunks will be deleted before
    export. If `False`, an error will be raised if the directory already
    exists."""

    bucket: str
    """Name of Google Cloud Storage bucket for uploading chunks."""

    prefix: str
    """Base prefix for the object in cloud storage."""

    dataset: str
    """Target BigQuery dataset, e.g., 'my_project:my_dataset'. If not provided
    the project will be derived from the environment."""
