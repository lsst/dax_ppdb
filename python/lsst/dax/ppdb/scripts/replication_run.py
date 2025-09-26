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

__all__ = ["replication_run"]

import logging

from lsst.dax.apdb import ApdbReplica

from ..ppdb import Ppdb
from ..replicator import Replicator

_LOG = logging.getLogger(__name__)


def replication_run(
    apdb_config: str,
    ppdb_config: str,
    single: bool,
    update: bool,
    min_wait_time: int,
    max_wait_time: int,
    check_interval: int,
    exit_on_empty: bool,
) -> None:
    """Execute replication process from APDB to PPDB.

    Parameters
    ----------
    apdb_config : `str`
        URL for APDB configuration file.
    ppdb_config : `str`
        URL for PPDB configuration file.
    single : `bool`
        Copy single bucket and stop.
    update : `bool`
        If `True` then allow updates to previously replicated data.
    min_wait_time : `int`
        Minimum time in seconds to wait for replicating a chunk after a next
        chunk appears.
    max_wait_time : `int`
        Maximum time in seconds to wait for replicating a chunk if no chunk
        appears.
    check_interval : `int`
        Time in seconds to wait before next check if there was no replicated
        chunks.
    exit_on_empty : `bool`
        If `True` then exit if there are no chunks to replicate, otherwise
        keep waiting for new chunks.
    """
    apdb = ApdbReplica.from_uri(apdb_config)
    ppdb = Ppdb.from_uri(ppdb_config)

    replicator = Replicator(apdb, ppdb, update, min_wait_time, max_wait_time, check_interval)
    replicator.run(single=single, exit_on_empty=exit_on_empty)
