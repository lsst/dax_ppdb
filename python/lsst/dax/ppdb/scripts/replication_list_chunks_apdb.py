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

__all__ = ["replication_list_chunks_apdb"]

from lsst.dax.apdb import ApdbReplica


def replication_list_chunks_apdb(apdb_config: str) -> None:
    """Print full list of replica chunks existing on APDB side.

    Parameters
    ----------
    apdb_config : `str`
        URL for APDB configuration file.
    """
    apdb = ApdbReplica.from_uri(apdb_config)
    chunks = apdb.getReplicaChunks()
    if chunks is not None:
        print(" Chunk Id              Update time                    Unique Id")
        sep = "-" * 77
        print(sep)
        chunks = sorted(chunks, key=lambda chunk: chunk.id)
        for chunk in chunks:
            insert_time = chunk.last_update_time
            print(f"{chunk.id:10d}  {insert_time.tai.isot}/tai  {chunk.unique_id}")
        print(sep)
        print(f"Total: {len(chunks)}")
    else:
        print("APDB instance does not support InsertIds")
