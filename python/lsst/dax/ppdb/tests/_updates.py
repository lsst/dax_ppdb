# This file is part of dax_ppdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from lsst.dax.apdb import (
    ApdbCloseDiaObjectValidityRecord,
    ApdbReassignDiaSourceToDiaObjectRecord,
    ApdbReassignDiaSourceToSSObjectRecord,
    ApdbUpdateNDiaSourcesRecord,
    ApdbUpdateRecord,
    ApdbWithdrawDiaForcedSourceRecord,
    ApdbWithdrawDiaSourceRecord,
)

from ..bigquery.updates import UpdateRecords


def _create_test_update_records() -> UpdateRecords:
    """Create test UpdateRecords with sample ApdbUpdateRecord instances."""
    records: list[ApdbUpdateRecord] = []

    # Hardcoded test values
    test_update_time_ns = 1640995200000000000  # 2022-01-01 00:00:00 UTC in nanoseconds
    test_mjd_tai = 59580.0  # Corresponding MJD TAI for 2022-01-01
    test_replica_chunk_id = 12345

    # Reassign DIASource to different DIAObject
    records.append(
        ApdbReassignDiaSourceToDiaObjectRecord(
            update_time_ns=test_update_time_ns,
            update_order=0,
            diaSourceId=100001,
            diaObjectId=300001,
            ra=45.0,
            dec=-30.0,
            midpointMjdTai=60000.0,
        )
    )

    # Reassign DIASource to SSObject
    records.append(
        ApdbReassignDiaSourceToSSObjectRecord(
            update_time_ns=test_update_time_ns,
            update_order=1,
            diaSourceId=100002,
            ssObjectId=2001,
            ssObjectReassocTimeMjdTai=test_mjd_tai,
            ra=45.0,
            dec=-30.0,
            midpointMjdTai=60000.0,
        )
    )

    # Withdraw DIASource
    records.append(
        ApdbWithdrawDiaSourceRecord(
            update_time_ns=test_update_time_ns,
            update_order=2,
            diaSourceId=100003,
            timeWithdrawnMjdTai=test_mjd_tai,
            ra=45.0,
            dec=-30.0,
            midpointMjdTai=60000.0,
        )
    )

    # Withdraw DIAForcedSource
    records.append(
        ApdbWithdrawDiaForcedSourceRecord(
            update_time_ns=test_update_time_ns,
            update_order=3,
            diaObjectId=200001,
            visit=12345,
            detector=42,
            timeWithdrawnMjdTai=test_mjd_tai,
            ra=45.0,
            dec=-30.0,
            midpointMjdTai=60000.0,
        )
    )

    # Close DIAObject validity interval
    records.append(
        ApdbCloseDiaObjectValidityRecord(
            update_time_ns=test_update_time_ns,
            update_order=4,
            diaObjectId=200001,
            validityEndMjdTai=test_mjd_tai,
            nDiaSources=5,
            ra=45.0,
            dec=-30.0,
        )
    )

    # Update DIAObject nDiaSources count
    records.append(
        ApdbUpdateNDiaSourcesRecord(
            update_time_ns=test_update_time_ns,
            update_order=5,
            diaObjectId=200002,
            nDiaSources=10,
            ra=45.0,
            dec=-30.0,
        )
    )

    # Add duplicate records for testing deduplication
    # Duplicate of the first record but with later timestamp (should be kept)
    records.append(
        ApdbReassignDiaSourceToDiaObjectRecord(
            update_time_ns=test_update_time_ns + 1000000000,  # 1 second later
            update_order=0,
            diaSourceId=100001,
            diaObjectId=400001,  # Different target object
            ra=45.0,
            dec=-30.0,
            midpointMjdTai=60000.0,
        )
    )

    # Duplicate of the nDiaSources update but with earlier timestamp (should be
    # discarded)
    records.append(
        ApdbUpdateNDiaSourcesRecord(
            update_time_ns=test_update_time_ns - 1000000000,  # 1 second earlier
            update_order=5,
            diaObjectId=200002,
            nDiaSources=8,  # Different value but older timestamp
            ra=45.0,
            dec=-30.0,
        )
    )

    return UpdateRecords(
        replica_chunk_id=test_replica_chunk_id,
        records=records,
    )
