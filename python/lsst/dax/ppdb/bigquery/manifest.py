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

__all__ = ["Manifest", "ParquetFileStats"]

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(UTC)


class ParquetFileStats(BaseModel):
    """Per-parquet-file statistics."""

    row_count: int = Field(ge=0, description="Non-negative count of rows written for this table.")
    """Number of rows written for this table (must be non-negative)."""

    checksum: str | None = None
    """SHA-256 checksum for this parquet file, if one was written."""

    size_bytes: int | None = None
    """Size of this parquet file in bytes, if one was written."""

    @staticmethod
    def compute_checksum(file_path: Path) -> str | None:
        """Compute the SHA-256 checksum of a file.

        Parameters
        ----------
        file_path
            Path to the file.

        Returns
        -------
        `str` or `None`
            SHA-256 hex digest, or `None` if the file does not exist.
        """
        if not file_path.exists():
            return None
        digest = hashlib.sha256()
        with open(file_path, "rb") as fd:
            for chunk in iter(lambda: fd.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def compute_size(file_path: Path) -> int | None:
        """Return the size of a file in bytes.

        Parameters
        ----------
        file_path
            Path to the file.

        Returns
        -------
        `int` or `None`
            File size in bytes, or `None` if the file does not exist.
        """
        if not file_path.exists():
            return None
        return file_path.stat().st_size


class Manifest(BaseModel):
    """Manifest record for replica chunk data that has been extracted into
    parquet files.
    """

    FILE_NAME: ClassVar[str] = "manifest.json"
    """Name of the manifest file."""

    model_config = ConfigDict(extra="forbid")
    """Pydantic model configuration."""

    replica_chunk_id: str
    """Sequential identifier of the replica chunk being exporter (`str`)."""

    unique_id: UUID
    """Globally unique opaque identifier for the export operation or replica
    (`uuid.UUID`).
    """

    schema_version: str
    """Version string of the schema used to produce this export (`str`)."""

    exported_at: datetime = Field(default_factory=_utc_now)
    """Timestamp when the export was produced (UTC). Serialized as ISO 8601
    (`datetime`)."""

    last_update_time: str
    """Source system's last-update timestamp for the replica; TAI value, kept
    as string to avoid any precision issues (`str`)."""

    table_data: dict[str, ParquetFileStats]
    """Mapping of table name to per-table statistics
    (`dict` [`str`, `ParquetFileStats`])."""

    updates_data: ParquetFileStats | None = None
    """Statistics for ``update_records.parquet`` when update records exist."""

    compression_format: str
    """Name of the compression format used for artifacts (e.g., "gzip",
    "zstd", "snappy", etc.)."""

    update_count: int = Field(default=0, ge=0)
    """Number of update records included in the exported data (`int`)."""

    def write_json_file(self, dir_path: Path) -> None:
        """Save the manifest to a JSON file in the specified directory.

        Parameters
        ----------
        dir_path
            Path to the directory where the manifest file should be written.
        """
        file_path = os.path.join(dir_path, self.FILE_NAME)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.model_dump(), f, indent=2, default=str)

    @classmethod
    def from_json_file(cls, file_path: Path) -> Manifest:
        """Load a manifest from a JSON file.

        Parameters
        ----------
        file_path
            Path to the JSON file containing the manifest.

        Returns
        -------
        `Manifest`
            The loaded manifest object.
        """
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
        return cls.model_validate(data)

    @classmethod
    def from_json_str(cls, content: str) -> Manifest:
        """Load a manifest from a string with JSON data.

        Parameters
        ----------
        content
            The string with the JSON data.
        """
        data = json.loads(content)
        return cls.model_validate(data)

    def is_empty_chunk(self) -> bool:
        """Check if the manifest represents an empty replica chunk in which
        all tables have zero rows and no update records are included.

        Returns
        -------
        `bool`
            `True` if all tables have zero rows and no update records are
            included, indicating an empty chunk, `False` otherwise.
        """
        return all(table.row_count == 0 for table in self.table_data.values()) and self.update_count == 0

    def has_table_data(self) -> bool:
        """Check if the manifest contains any table data with non-zero row
        counts.

        Returns
        -------
        bool
            `True` if at least one table has a non-zero row count, `False`
            otherwise.
        """
        return any(table.row_count > 0 for table in self.table_data.values())
