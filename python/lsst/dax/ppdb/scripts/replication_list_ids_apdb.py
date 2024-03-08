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

__all__ = ["replication_list_ids_apdb"]

from lsst.dax.apdb import Apdb


def replication_list_ids_apdb(apdb_config: str) -> None:
    """Print full list of insert IDs existing on APDB side.

    Parameters
    ----------
    apdb_config : `str`
        URL for APDB configuration file.
    """
    apdb = Apdb.from_uri(apdb_config)
    inserts = apdb.getInsertIds()
    if inserts is not None:
        print(" InsertId              Insert time                        Unique Id")
        sep = "-" * 83
        print(sep)
        inserts = sorted(inserts, key=lambda iid: iid.insert_time.nsecs())
        for insert in inserts:
            insert_time = insert.insert_time
            print(f"{insert.id:10d}  {insert_time.toString(insert_time.TAI)}/tai  {insert.unique_id}")
        print(sep)
        print(f"Total: {len(inserts)}")
    else:
        print("APDB instance does not support InsertIds")
