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

__all__ = ["PpdbReplicaChunkExtended", "ChunkStatus"]

import dataclasses
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path

import astropy.time
from lsst.dax.apdb import ReplicaChunk

from ..ppdb import PpdbReplicaChunk


class ChunkStatus(StrEnum):
    """Status of a replica chunk in the PPDB."""

    EXPORTED = "exported"
    """Chunk has been exported from the APDB to a local parquet file."""
    UPLOADED = "uploaded"
    """Chunk has been uploaded to cloud storage."""
    FAILED = "failed"
    """Chunk processing failed and an error occurred."""


@dataclass(frozen=True)
class PpdbReplicaChunkExtended(PpdbReplicaChunk):
    """Replica chunk with additional information for BigQuery-based PPDB."""

    status: ChunkStatus
    """Status of the replica chunk."""

    directory: Path
    """Directory where the exported replica chunk data is stored locally."""

    @property
    def manifest_name(self) -> str:
        """Filename of the manifest file for this chunk."""
        return f"chunk_{self.id}.manifest.json"

    @property
    def manifest_path(self) -> Path:
        """Path to the manifest file for this chunk, or `None` if directory is
        not set.
        """
        if self.directory is None:
            raise ValueError(f"directory for replica chunk {self.id} is not set")
        return self.directory / self.manifest_name

    @property
    def replica_time_dt_utc(self) -> datetime:
        """Return the replica_time as a `datetime` in UTC."""
        return datetime.fromtimestamp(self.replica_time.unix_tai, tz=timezone.utc)

    @property
    def last_update_time_dt_utc(self) -> datetime:
        """Return the last_update_time as a `datetime` in UTC."""
        return datetime.fromtimestamp(self.last_update_time.unix_tai, tz=timezone.utc)

    @classmethod
    def from_replica_chunk(
        cls, replica_chunk: ReplicaChunk, status: ChunkStatus, directory: Path
    ) -> PpdbReplicaChunkExtended:
        """Create a `PpdbReplicaChunkExtended` from a `ReplicaChunk`.

        Parameters
        ----------
        replica_chunk : `ReplicaChunk`
            The `ReplicaChunk` to convert.
        status : `ChunkStatus`
            Status of the replica chunk.
        directory : `pathlib.Path`
            Directory where the replica chunk data is stored.

        Returns
        -------
        extended_chunk : `PpdbReplicaChunkExtended`
            The converted `PpdbReplicaChunkExtended`.
        """
        return PpdbReplicaChunkExtended(
            id=replica_chunk.id,
            unique_id=replica_chunk.unique_id,
            last_update_time=replica_chunk.last_update_time,
            replica_time=astropy.time.Time.now(),
            status=status,
            directory=directory,
        )

    def with_new_status(self, new_status: ChunkStatus) -> PpdbReplicaChunkExtended:
        """Create a new `PpdbReplicaChunkExtended` with the same properties as
        this one, but with a different status.

        Parameters
        ----------
        new_status : `ChunkStatus`
            The new status to set.

        Returns
        -------
        new_chunk : `PpdbReplicaChunkExtended`
            The new chunk with the updated status.
        """
        return dataclasses.replace(self, status=new_status)
