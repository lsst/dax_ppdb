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

__all__ = ["UpdateRecordExpander"]

import logging

from lsst.dax.apdb.apdbUpdateRecord import ApdbUpdateRecord

from .expanded_update_record import ExpandedUpdateRecord
from .update_records import UpdateRecords

_LOG = logging.getLogger(__name__)


class UpdateRecordExpander:
    """Expand APDB update records into individual field-level updates for
    BigQuery.
    """

    @classmethod
    def expand_single_record(
        cls, update_record: ApdbUpdateRecord, replica_chunk_id: int
    ) -> list[ExpandedUpdateRecord]:
        """Expand a single APDB update record into ExpandedUpdateRecord
        objects.

        Parameters
        ----------
        update_record : `ApdbUpdateRecord`
            A single APDB update record to expand.
        replica_chunk_id : `int`
            The replica chunk ID associated with this update record.

        Returns
        -------
        expanded_records : `list` [ `ExpandedUpdateRecord` ]
            List of ExpandedUpdateRecord objects, one per field being updated.
        """
        # Get the target table from the update record
        table_name = update_record.apdb_table.name

        # Create an ExpandedUpdateRecord for each field being updated
        expanded_records = []
        record_id_values = tuple(value for _, value in update_record.record_id())
        for field_name, field_value in update_record.record_payload():
            # This particular type of update should not be propagated if
            # nDiaSources is set to None.
            if (
                update_record.update_type == "close_diaobject_validity"
                and field_name == "nDiaSources"
                and field_value is None
            ):
                continue
            expanded_record = ExpandedUpdateRecord(
                table_name=table_name,
                record_id=record_id_values,
                field_name=field_name,
                field_value=field_value,
                replica_chunk_id=replica_chunk_id,
                update_order=update_record.update_order,
                update_time_ns=update_record.update_time_ns,
            )
            expanded_records.append(expanded_record)

        return expanded_records

    @classmethod
    def expand_updates(
        cls, update_records: UpdateRecords, replica_chunk_id: int
    ) -> list[ExpandedUpdateRecord]:
        """Expand the APDB update records into a list of individual updates.

        Parameters
        ----------
        update_records : `UpdateRecords`
            The APDB update records to expand.
        replica_chunk_id : `int`
            The replica chunk ID associated with these update records.

        Returns
        -------
        expanded_updates : `list` [ `ExpandedUpdateRecord` ]
            A list of individual updates derived from the input update records.
        """
        expanded_updates = []

        for update_record in update_records.records:
            expanded_records = cls.expand_single_record(update_record, replica_chunk_id)
            expanded_updates.extend(expanded_records)

        _LOG.info(
            "Created %d expanded records from %d update records in chunk %d",
            len(expanded_updates),
            len(update_records.records),
            replica_chunk_id,
        )

        return expanded_updates
