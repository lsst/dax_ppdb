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

__all__ = ["ExpandedUpdateRecord"]

from typing import Any

from pydantic import BaseModel, Field

from lsst.dax.apdb import ApdbUpdateRecord


class ExpandedUpdateRecord(BaseModel):
    """A single normalized (expanded) update row.

    This model represents one field-level update after expanding an
    original logical update event into one row per updated field.
    It is the canonical shape loaded into the BigQuery updates table.
    """

    table_name: str = Field(
        ...,
        min_length=1,
        description=("Logical target table for the update (e.g., 'DiaObject', 'DiaSource')."),
    )

    record_id: tuple[int, ...] = Field(
        ...,
        description=(
            "Identifier of the record being updated. For update types with a single record ID, this "
            "will be a tuple of one element. For updates on records with a composite key "
            "(e.g., DiaForcedSource), this will include all components of the key, in order."
        ),
    )

    field_name: str = Field(
        ...,
        min_length=1,
        description=("Name of the target column being updated."),
    )

    field_value: Any = Field(
        ...,
        description=("New value for the field."),
    )

    apdb_replica_chunk: int = Field(
        ...,
        ge=0,
        description=("Source replica chunk identifier associated with this update."),
    )

    update_time_ns: int = Field(
        ge=0,
        description=("Source event timestamp in nanoseconds since the epoch."),
    )

    update_order: int = Field(
        description=("Ordering value within the replica chunk or update batch."),
    )

    @classmethod
    def from_update_record(
        cls, update_record: ApdbUpdateRecord, apdb_replica_chunk: int
    ) -> list[ExpandedUpdateRecord]:
        """Expand a single APDB update record into field-level update rows.

        Parameters
        ----------
        update_record
            The APDB update record to expand.
        apdb_replica_chunk
            The replica chunk ID associated with this update record.

        Returns
        -------
        `list` [ `ExpandedUpdateRecord` ]
            One expanded record per field being updated.
        """
        table_name = update_record.apdb_table.value
        record_id_values = tuple(value for _, value in update_record.record_id())
        return [
            cls(
                table_name=table_name,
                record_id=record_id_values,
                field_name=field_name,
                field_value=field_value,
                apdb_replica_chunk=apdb_replica_chunk,
                update_order=update_record.update_order,
                update_time_ns=update_record.update_time_ns,
            )
            for field_name, field_value in update_record.record_payload()
        ]
