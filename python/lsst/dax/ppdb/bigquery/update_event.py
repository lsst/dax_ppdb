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

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class UpdateEvent(BaseModel):
    """
    A single field-level update event emitted by the replication system.

    This model represents an immutable change instruction and is used as the
    canonical payload for Pub/Sub messages and for persistence in the update
    ledger. One instance corresponds to one field update on one record.
    """

    replica_chunk_id: int = Field(
        ...,
        description="Identifier of the replica chunk to which this event belongs.",
    )

    unique_id: UUID = Field(
        ...,
        description="Unique identifier of the replica chunk update.",
    )

    source_time_ns: int = Field(
        ...,
        description="Nanosecond-precision timestamp when the change occurred at the source.",
    )

    source_sequence: int = Field(
        ...,
        ge=0,
        description="Record order within the source batch.",
    )

    table_name: str = Field(
        ...,
        description="Logical table this update targets.",
    )

    record_id: str = Field(
        ...,
        description="Canonical primary key of the record being modified.",
    )

    field_name: str = Field(
        ...,
        description="Name of the field being updated.",
    )

    value_json: Any | None = Field(
        ...,
        description="JSON-encoded new value for the field, including explicit null.",
    )

    ingest_time: datetime | None = Field(
        None,
        description="Set by the ingest service when writing the ledger (UTC).",
    )
