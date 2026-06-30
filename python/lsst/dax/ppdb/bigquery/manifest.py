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

__all__ = ["Manifest", "ParquetFileEntry"]

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .updates import UpdateRecords


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(UTC)


class ParquetFileEntry(BaseModel):
    """Information about a single parquet file for the replica chunk.

    Non-existent files, e.g. where data for the table was not present in the
    chunk, should not have entries. Their absence from the manifest indicates
    that they were not written. Empty files (no rows) should also not have
    entries as they would result in no-ops when loading the data into BigQuery.
    """

    row_count: int = Field(ge=1, description="Count of rows written for this table.")
    """Number of rows written for this table (must be positive)."""

    checksum: str
    """SHA-256 checksum for this parquet file."""

    size_bytes: int = Field(ge=1, description="Size of this parquet file in bytes.")
    """Size of this parquet file in bytes."""

    @classmethod
    def from_path(cls, path: Path, row_count: int) -> ParquetFileEntry:
        """Create a `ParquetFileEntry` from a parquet file path and row count.

        Parameters
        ----------
        path
            Path to the parquet file.
        row_count
            Number of rows written for this table.

        Returns
        -------
        `ParquetFileEntry`
            The created `ParquetFileEntry` object.
        """
        return cls(
            row_count=row_count,
            checksum=cls.compute_checksum(path),
            size_bytes=cls.compute_size(path),
        )

    @staticmethod
    def compute_checksum(file_path: Path) -> str:
        """Compute the SHA-256 checksum of a file.

        Parameters
        ----------
        file_path
            Path to the file.

        Returns
        -------
        `str`
            SHA-256 hex digest of the file contents.

        Raises
        ------
        FileNotFoundError
            Raised if the file does not exist.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        digest = hashlib.sha256()
        with open(file_path, "rb") as fd:
            chunk_size = 1024 * 1024
            while chunk := fd.read(chunk_size):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def compute_size(file_path: Path) -> int:
        """Return the size of a file in bytes.

        Parameters
        ----------
        file_path
            Path to the file.

        Returns
        -------
        `int`
            File size in bytes.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        return file_path.stat().st_size


class Manifest(BaseModel):
    """Manifest record for replica chunk data that has been extracted into
    parquet files.
    """

    FILE_NAME: ClassVar[str] = "manifest.json"
    """Name of the manifest file."""

    model_config = ConfigDict(extra="forbid")
    """Pydantic model configuration."""

    schema_version: str
    """Version string of the schema used to produce this export (`str`)."""

    replica_chunk_id: str
    """Sequential identifier of the replica chunk (`str`)."""

    unique_id: UUID
    """Globally unique opaque identifier for the export operation or replica
    (`uuid.UUID`).
    """

    exported_at: datetime = Field(default_factory=_utc_now)
    """Timestamp when the export was produced (UTC). Serialized as ISO 8601
    (`datetime`).
    """

    last_update_time: str
    """Source system's last-update timestamp for the replica; TAI value, kept
    as string to avoid any precision issues (`str`)."""

    compression_format: str
    """Name of the compression format used for artifacts (e.g., "gzip",
    "zstd", "snappy", etc.)."""

    files: dict[str, ParquetFileEntry] = Field(default_factory=dict)
    """Mapping of parquet file names to their entries."""

    @property
    def is_empty_chunk(self) -> bool:
        """`True` if the manifest represents an empty replica chunk with no
        table data or update records, `False` otherwise (`bool`).
        """
        return len(self.files) == 0

    @property
    def has_table_data(self) -> bool:
        """`True` if the chunk contains any parquet files with table data,
        `False` otherwise (`bool`).
        """
        return any(name != UpdateRecords.PARQUET_FILE_NAME for name in self.files)

    @property
    def has_update_records(self) -> bool:
        """`True` if the chunk contains the update records parquet file,
        `False` otherwise (`bool`).
        """
        return UpdateRecords.PARQUET_FILE_NAME in self.files

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
