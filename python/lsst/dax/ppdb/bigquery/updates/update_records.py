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

from io import BytesIO
from pathlib import Path
from typing import ClassVar

import pyarrow
from pyarrow import parquet

from lsst.dax.apdb.apdbUpdateRecord import ApdbUpdateRecord


class UpdateRecords:
    """Container for APDB update records with Parquet serialization.

    Parameters
    ----------
    records
        List of ``(apdb_replica_chunk, record)`` pairs, associating each APDB
        update record with the replica chunk to which it belongs. The chunk id
        is stored per row so that files for multiple chunks can be
        concatenated.
    """

    PARQUET_FILE_NAME: ClassVar[str] = "updates.parquet"
    """Name of the Parquet file with the updates."""

    _PARQUET_SCHEMA: ClassVar[pyarrow.Schema] = pyarrow.schema(
        [
            pyarrow.field("update_time_ns", pyarrow.int64()),
            pyarrow.field("update_order", pyarrow.int64()),
            pyarrow.field("json_payload", pyarrow.string()),
            pyarrow.field("apdb_replica_chunk", pyarrow.int64()),
        ]
    )

    def __init__(self, records: list[tuple[int, ApdbUpdateRecord]]) -> None:
        self.records = records

    def write_parquet_file(self, path: Path) -> None:
        """Write the update records to a Parquet file.

        Each record is stored with ``update_time_ns``, ``update_order``,
        ``json_payload``, and ``apdb_replica_chunk`` columns, where
        ``json_payload`` contains the serialized record data and
        ``apdb_replica_chunk`` identifies the source replica chunk.

        Parameters
        ----------
        path
            Destination Parquet file path.
        """
        apdb_replica_chunks: list[int] = []
        update_times: list[int] = []
        update_orders: list[int] = []
        json_payloads: list[str] = []

        for apdb_replica_chunk, record in self.records:
            apdb_replica_chunks.append(apdb_replica_chunk)
            update_times.append(record.update_time_ns)
            update_orders.append(record.update_order)
            json_payloads.append(record.to_json())

        table = pyarrow.table(
            {
                "apdb_replica_chunk": apdb_replica_chunks,
                "update_time_ns": update_times,
                "update_order": update_orders,
                "json_payload": json_payloads,
            },
            schema=self._PARQUET_SCHEMA,
        )
        parquet.write_table(table, path)

    @classmethod
    def from_parquet_file(cls, path: Path) -> UpdateRecords:
        """Read update records from a Parquet file.

        Parameters
        ----------
        path
            Path to the Parquet file.

        Returns
        -------
        `UpdateRecords`
            The deserialized update records.
        """
        with open(path, "rb") as f:
            return cls.from_parquet_bytes(f.read())

    @classmethod
    def from_parquet_bytes(cls, data: bytes) -> UpdateRecords:
        """Read update records from Parquet-formatted bytes.

        Parameters
        ----------
        data
            Parquet file content as bytes.

        Returns
        -------
        `UpdateRecords`
            The deserialized update records.
        """
        table = parquet.read_table(BytesIO(data), schema=cls._PARQUET_SCHEMA)
        records: list[tuple[int, ApdbUpdateRecord]] = []
        for update_time_ns, update_order, json_payload, apdb_replica_chunk in zip(
            table.column("update_time_ns").to_pylist(),
            table.column("update_order").to_pylist(),
            table.column("json_payload").to_pylist(),
            table.column("apdb_replica_chunk").to_pylist(),
            strict=True,
        ):
            record = ApdbUpdateRecord.from_json(update_time_ns, update_order, json_payload)
            records.append((apdb_replica_chunk, record))
        return cls(records=records)
