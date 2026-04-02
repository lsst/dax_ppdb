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

__all__ = ["UpdateRecords"]

import json
from pathlib import Path
from typing import Any, ClassVar, cast

from pydantic import BaseModel, field_serializer, field_validator

from lsst.dax.apdb.apdbUpdateRecord import ApdbUpdateRecord


class UpdateRecords(BaseModel):
    """Data model for APDB update records."""

    FILE_NAME: ClassVar[str] = "update_records.json"
    """Name of the JSON file with the updates."""

    replica_chunk_id: int
    """Identifier of the replica chunk to which these update records belong."""

    records: list[ApdbUpdateRecord]
    """List of APDB update records included in this object."""

    @field_serializer("records")
    def serialize_records(
        self,
        records: list[ApdbUpdateRecord],
    ) -> list[dict[str, Any]]:
        """Serialize the ``ApdbUpdateRecord`` objects to JSON.

        Parameters
        ----------
        records : `list` [ `ApdbUpdateRecord` ]
            The list of APDB update records to serialize.

        Returns
        -------
        serialized_records : `list` [ `dict` [ `str`, `Any` ]]
            The serialized JSON data.
        """
        serialized_records: list[dict[str, Any]] = []
        for update_record in records:
            record_dict: dict[str, Any] = json.loads(update_record.to_json())
            record_dict["update_time_ns"] = update_record.update_time_ns
            record_dict["update_order"] = update_record.update_order
            serialized_records.append(record_dict)
        return serialized_records

    @field_validator("records", mode="before")
    @classmethod
    def deserialize_records(
        cls,
        records: list[dict[str, Any]] | list[ApdbUpdateRecord],
    ) -> list[ApdbUpdateRecord]:
        """Deserialize the JSON data to ``ApdbUpdateRecord`` objects.

        Parameters
        ----------
        records : `list` [ `dict` [ `str`, `Any` ] | `ApdbUpdateRecord` ]
            The list of serialized JSON data or already deserialized
            ApdbUpdateRecord objects.

        Returns
        -------
        update_records : `list` [ `ApdbUpdateRecord` ]
            The list of APDB update records.
        """
        if records and isinstance(records[0], ApdbUpdateRecord):
            return cast(list[ApdbUpdateRecord], records)
        deserialized_records: list[ApdbUpdateRecord] = []
        for record in records:
            if isinstance(record, dict):
                record_copy = record.copy()
                update_time_ns = record_copy.pop("update_time_ns")
                update_order = record_copy.pop("update_order")
                json_str = json.dumps(record_copy)
                update_record = ApdbUpdateRecord.from_json(
                    update_time_ns,
                    update_order,
                    json_str,
                )
                deserialized_records.append(update_record)
            elif isinstance(record, ApdbUpdateRecord):
                deserialized_records.append(record)
            else:
                raise TypeError("Each record must be a dict or ApdbUpdateRecord")
        return deserialized_records

    def write_json_file(self, path: Path) -> None:
        with open(path, "w") as f:
            json.dump(self.model_dump(), f, indent=2, default=str)

    @classmethod
    def from_json_file(cls, path: Path) -> UpdateRecords:
        with open(path) as f:
            data = json.load(f)
        return cls.model_validate(data)

    @classmethod
    def from_json_string(cls, json_str: str) -> UpdateRecords:
        data = json.loads(json_str)
        return cls.model_validate(data)
