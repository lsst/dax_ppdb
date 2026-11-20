# This file is part of dax_ppdb.
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

__all__ = ["FileEntry", "Manifest"]

from datetime import datetime
from pathlib import Path
from typing import ClassVar

from pyarrow import parquet
from pydantic import BaseModel, ConfigDict, Field

from lsst.resources import ResourcePath, ResourcePathExpression

from ..manifest import InvalidManifestError, ParquetFileEntry, _utc_now


class FileEntry(ParquetFileEntry):
    """Information about a single uploaded SSO parquet file."""

    source_path: str
    """Local path the file was uploaded from."""

    @classmethod
    def from_path(cls, path: Path, row_count: int | None = None) -> FileEntry:
        """Create a `FileEntry` from a local parquet file.

        Parameters
        ----------
        path
            Path to the parquet file.
        row_count
            Number of rows in the file. If `None`, it is read from the parquet
            file's metadata.

        Returns
        -------
        `FileEntry`
            The created `FileEntry` object.

        Raises
        ------
        ValueError
            Raised if the file has no rows.

        Notes
        -----
        The SSO uploader never passes ``row_count``, so it's always read from
        the file. The parameter is kept only so that this method matches
        `~lsst.dax.ppdb.bigquery.ParquetFileEntry.from_path`, which it
        overrides.
        """
        if row_count is None:
            row_count = parquet.read_metadata(path).num_rows
        if row_count == 0:
            raise ValueError("the file has no rows")
        return cls(
            row_count=row_count,
            checksum=cls.compute_checksum(path),
            size_bytes=cls.compute_size(path),
            source_path=str(path),
        )


class Manifest(BaseModel):
    """Manifest for a set of SSO parquet files uploaded to Cloud Storage.

    Notes
    -----
    The uploader writes the manifest last, after every parquet file has been
    uploaded, so if the manifest exists, the upload is complete. Each table's
    files are under ``<prefix>/<table>/`` and are listed by file name in
    `tables`.
    """

    FILE_NAME: ClassVar[str] = "manifest.json"
    """Name of the manifest file."""

    model_config = ConfigDict(extra="forbid")
    """Pydantic model configuration."""

    uploaded_at: datetime = Field(default_factory=_utc_now)
    """Timestamp when the upload was made (UTC). Serialized as ISO 8601
    (`datetime`).
    """

    tables: dict[str, dict[str, FileEntry]] = Field(default_factory=dict)
    """Mapping of table names to the entries for their parquet files, by
    file name.
    """

    def row_count(self, table_name: str) -> int:
        """Return the total number of rows uploaded for a table.

        Parameters
        ----------
        table_name
            Name of the table.

        Returns
        -------
        `int`
            Sum of the row counts of the table's parquet files.
        """
        return sum(entry.row_count for entry in self.tables[table_name].values())

    @classmethod
    def from_uri(cls, folder: ResourcePathExpression) -> Manifest:
        """Load the manifest of an upload from its folder.

        Parameters
        ----------
        folder
            Location of the upload's folder, e.g. a ``gs://`` URI.

        Returns
        -------
        `Manifest`
            The loaded manifest object.

        Raises
        ------
        InvalidManifestError
            Raised if the manifest file is not valid UTF-8 text, is not valid
            JSON, or does not match the manifest model.
        FileNotFoundError
            Raised if the manifest file does not exist.
        google.api_core.exceptions.GoogleAPIError
            Raised if Cloud Storage returns an error reading a ``gs://``
            manifest, after the storage client's own retries are exhausted.
        """
        manifest_uri = ResourcePath(folder).join(cls.FILE_NAME)
        content = manifest_uri.read()
        try:
            return cls.model_validate_json(content.decode("utf-8"))
        # All errors that can be raised here are subclasses of ValueError.
        except ValueError as e:
            raise InvalidManifestError(f"Invalid manifest {manifest_uri}: {e}") from e
