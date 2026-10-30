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

__all__ = ["ChunkNotFoundError", "ChunkStatusError", "ChunkStatusTransitionError", "ChunkTracker"]

import json
from collections.abc import Collection

from .ppdb_bigquery import PpdbBigQuery, UpdatableField
from .ppdb_replica_chunk_extended import ChunkStatus, PpdbReplicaChunkExtended

# Longest error message stored in the chunk table.
_MAX_ERROR_MESSAGE_LENGTH = 2000


class ChunkStatusError(Exception):
    """Base class for errors raised when a replica chunk's status can't be
    changed.
    """


class ChunkNotFoundError(ChunkStatusError):
    """Raised when a replica chunk does not exist."""


class ChunkStatusTransitionError(ChunkStatusError):
    """Raised when a replica chunk can't change to a status from its current
    status.
    """


class ChunkTracker:
    """Reads and updates the status of replica chunks in the PPDB tracking
    database.

    Parameters
    ----------
    ppdb
        Interface to the PPDB.

    Notes
    -----
    Status changes are checked against `ChunkStatus.allowed_from` in the
    same database update that applies them, so the check can't race with
    another update. The other columns follow the status:

    - ``error_message`` is set with ``failed`` and cleared otherwise.
    - ``gcs_uri`` is cleared with ``exported`` and otherwise left unchanged
      unless given.
    - ``bigquery_job_ids`` is left unchanged unless given.
    """

    def __init__(self, ppdb: PpdbBigQuery):
        self._ppdb = ppdb

    def get(self, chunk_id: int) -> PpdbReplicaChunkExtended | None:
        """Return a replica chunk.

        Parameters
        ----------
        chunk_id
            ID of the replica chunk.

        Returns
        -------
        `PpdbReplicaChunkExtended` or `None`
            The chunk, or `None` if it does not exist.
        """
        return self._ppdb.find_chunk_by_id(chunk_id)

    def update_status(
        self,
        chunk_id: int,
        status: ChunkStatus,
        *,
        error_message: str | None = None,
        gcs_uri: str | None = None,
        bigquery_job_ids: dict[str, str] | None = None,
    ) -> None:
        """Change the status of a replica chunk, if the change is allowed from
        its current status.

        Parameters
        ----------
        chunk_id
            ID of the replica chunk.
        status
            New status.
        error_message
            Reason for the failure, stored with ``failed``.
        gcs_uri
            URI of the chunk's data, or `None` to leave it unchanged.
        bigquery_job_ids
            BigQuery job IDs to record, or `None` to leave them unchanged.

        Raises
        ------
        ChunkNotFoundError
            Raised if the chunk does not exist.
        ChunkStatusTransitionError
            Raised if the change is not allowed from the chunk's current
            status.
        """
        self.update_statuses(
            [chunk_id],
            status,
            error_message=error_message,
            gcs_uri=gcs_uri,
            bigquery_job_ids=bigquery_job_ids,
        )

    def update_statuses(
        self,
        chunk_ids: Collection[int],
        status: ChunkStatus,
        *,
        error_message: str | None = None,
        gcs_uri: str | None = None,
        bigquery_job_ids: dict[str, str] | None = None,
    ) -> None:
        """Change the status of several replica chunks together. Either all of
        them change or none do.

        Parameters
        ----------
        chunk_ids
            IDs of the replica chunks.
        status
            New status.
        error_message
            As for `update_status`.
        gcs_uri
            As for `update_status`.
        bigquery_job_ids
            As for `update_status`.

        Raises
        ------
        ChunkNotFoundError
            Raised if any chunk does not exist. No chunk is changed.
        ChunkStatusTransitionError
            Raised if any chunk can't change from its current status. No chunk
            is changed.
        """
        values: dict[str, object] = {
            UpdatableField.ERROR_MESSAGE: (
                error_message[:_MAX_ERROR_MESSAGE_LENGTH]
                if status is ChunkStatus.FAILED and error_message
                else None
            )
        }
        if status is ChunkStatus.EXPORTED:
            values[UpdatableField.GCS_URI] = None
        elif gcs_uri is not None:
            values[UpdatableField.GCS_URI] = gcs_uri
        if bigquery_job_ids is not None:
            values[UpdatableField.BIGQUERY_JOB_IDS] = json.dumps(bigquery_job_ids)

        if not self._ppdb.update_chunk_status(
            chunk_ids,
            status,
            expected=status.allowed_from,
            **{str(field): value for field, value in values.items()},
        ):
            # The update was refused. Look the chunks up to say why.
            chunks = {chunk_id: self._ppdb.find_chunk_by_id(chunk_id) for chunk_id in chunk_ids}
            missing = [chunk_id for chunk_id, chunk in chunks.items() if chunk is None]
            if missing:
                raise ChunkNotFoundError(f"Replica chunks {missing} do not exist")
            current = {
                chunk_id: str(chunk.status)
                for chunk_id, chunk in chunks.items()
                if chunk is not None and not chunk.status.can_transition_to(status)
            }
            raise ChunkStatusTransitionError(f"Can't set replica chunks to {status} from {current}")
