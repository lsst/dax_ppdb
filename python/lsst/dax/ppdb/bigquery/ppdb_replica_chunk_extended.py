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

__all__ = ["ChunkStatus", "PpdbReplicaChunkExtended"]

import dataclasses
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

import astropy.time

if TYPE_CHECKING:
    import uuid

from lsst.dax.apdb import ReplicaChunk

from ..ppdb import PpdbReplicaChunk


class ChunkStatus(StrEnum):
    """Status of a replica chunk in the PPDB."""

    EXPORTED = "exported"
    """Chunk has been exported from the APDB to a local parquet file."""
    UPLOADED = "uploaded"
    """Chunk has been uploaded to cloud storage."""
    STAGING = "staging"
    """Chunk is being copied into the staging tables."""
    RETRYING = "retrying"
    """Staging hit a transient error and the chunk is waiting to be retried."""
    STAGED = "staged"
    """Chunk data has been copied into the staging tables."""
    PROMOTED = "promoted"
    """Chunk data has been promoted from the staging to production tables."""
    FAILED = "failed"
    """Chunk processing failed and an error occurred."""
    SKIPPED = "skipped"
    """Chunk processing is skipped because there is no data to export."""

    @property
    def allowed_from(self) -> frozenset[ChunkStatus]:
        """Statuses a chunk may change to this status from
        (`frozenset` [ `ChunkStatus` ]).
        """
        return _ALLOWED_FROM[self]

    def can_transition_to(self, new_status: ChunkStatus) -> bool:
        """Return whether a chunk may change from this status to another.

        Parameters
        ----------
        new_status
            The status to change to.

        Returns
        -------
        `bool`
            `True` if the transition is allowed, `False` otherwise.
        """
        return self in new_status.allowed_from


# Allowed status changes: each status and the statuses it may be set from.
# Chunks are created as exported or skipped. A failed chunk is retried by hand
# by setting it back to exported or uploaded.
_ALLOWED_FROM: dict[ChunkStatus, frozenset[ChunkStatus]] = {
    ChunkStatus.EXPORTED: frozenset({ChunkStatus.UPLOADED, ChunkStatus.FAILED}),
    ChunkStatus.UPLOADED: frozenset({ChunkStatus.EXPORTED, ChunkStatus.FAILED}),
    ChunkStatus.STAGING: frozenset({ChunkStatus.UPLOADED, ChunkStatus.RETRYING}),
    ChunkStatus.RETRYING: frozenset({ChunkStatus.STAGING}),
    ChunkStatus.STAGED: frozenset({ChunkStatus.STAGING}),
    ChunkStatus.PROMOTED: frozenset({ChunkStatus.STAGED}),
    ChunkStatus.FAILED: frozenset(
        {ChunkStatus.EXPORTED, ChunkStatus.UPLOADED, ChunkStatus.STAGING, ChunkStatus.RETRYING}
    ),
    ChunkStatus.SKIPPED: frozenset(),
}


@dataclass(frozen=True)
class PpdbReplicaChunkExtended(PpdbReplicaChunk):
    """Replica chunk with additional information for BigQuery-based PPDB."""

    status: ChunkStatus
    """Status of the replica chunk."""

    gcs_uri: str | None = None
    """GCS URI where the replica chunk data is stored, or `None` if not
    uploaded yet."""

    update_count: int = 0
    """Count of APDB update records included in the chunk
    (must be non-negative)."""

    status_time: datetime | None = None
    """Time of the last status change, or `None` if not recorded."""

    error_message: str | None = None
    """Reason the chunk failed, set only when the status is ``failed``."""

    bigquery_job_ids: dict[str, str] | None = None
    """IDs of the BigQuery jobs from the last staging attempt, keyed by table
    name, plus ``commit`` for the transaction that copied the chunk into the
    staging tables."""

    @property
    def replica_time_dt_utc(self) -> datetime:
        """Return the replica_time as a `datetime` in UTC."""
        return datetime.fromtimestamp(self.replica_time.unix_tai, tz=UTC)

    @property
    def last_update_time_dt_utc(self) -> datetime:
        """Return the last_update_time as a `datetime` in UTC."""
        return datetime.fromtimestamp(self.last_update_time.unix_tai, tz=UTC)

    @classmethod
    def from_replica_chunk(
        cls,
        replica_chunk: ReplicaChunk,
        status: ChunkStatus,
        update_count: int = 0,
    ) -> PpdbReplicaChunkExtended:
        """Create a `PpdbReplicaChunkExtended` from a
        `~lsst.dax.apdb.ReplicaChunk`.

        Parameters
        ----------
        replica_chunk
            The `~lsst.dax.apdb.ReplicaChunk` to convert.
        status
            Status of the replica chunk.

        Returns
        -------
        `PpdbReplicaChunkExtended`
            The converted `PpdbReplicaChunkExtended`.
        """
        return PpdbReplicaChunkExtended(
            id=replica_chunk.id,
            unique_id=replica_chunk.unique_id,
            last_update_time=replica_chunk.last_update_time,
            replica_time=astropy.time.Time.now(),
            status=status,
            update_count=update_count,
        )

    def with_new_status(self, new_status: ChunkStatus) -> PpdbReplicaChunkExtended:
        """Create a new `PpdbReplicaChunkExtended` with the same properties as
        this one, but with a different status.

        Parameters
        ----------
        new_status
            The new status to set.

        Returns
        -------
        `PpdbReplicaChunkExtended`
            The new chunk with the updated status.
        """
        return dataclasses.replace(self, status=new_status)

    def with_new_gcs_uri(self, new_gcs_uri: str | None) -> PpdbReplicaChunkExtended:
        """Create a new `PpdbReplicaChunkExtended` with the same properties as
        this one, but with a different GCS URI.

        Parameters
        ----------
        new_gcs_uri
            The new GCS URI to set, or `None` to clear it.

        Returns
        -------
        `PpdbReplicaChunkExtended`
            The new chunk with the updated GCS URI.
        """
        return dataclasses.replace(self, gcs_uri=new_gcs_uri)

    def to_row(self) -> dict[str, int | str | datetime | uuid.UUID | None]:
        """Convert to a dictionary of column values for the
        ``PpdbReplicaChunk`` table.

        Returns
        -------
        `dict`
            Dictionary of column names to values.
        """
        return {
            "apdb_replica_chunk": self.id,
            "last_update_time": self.last_update_time_dt_utc,
            "unique_id": self.unique_id,
            "replica_time": self.replica_time_dt_utc,
            "status": self.status.value,
            "gcs_uri": self.gcs_uri,  # This field can be null.
            "update_count": self.update_count,
            "status_time": self.status_time if self.status_time is not None else datetime.now(UTC),
            "error_message": self.error_message,
            "bigquery_job_ids": (
                json.dumps(self.bigquery_job_ids) if self.bigquery_job_ids is not None else None
            ),
        }
