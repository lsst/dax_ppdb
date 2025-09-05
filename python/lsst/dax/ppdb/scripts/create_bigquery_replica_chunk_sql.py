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

__all__ = ["create_bigquery_replica_chunk_sql"]

from ..bigquery._config import PpdbBigQueryConfig
from ..bigquery._replica_chunk import PpdbReplicaChunkSql
from ..ppdb import PpdbConfig


def create_bigquery_replica_chunk_sql(
    ppdb_config: str,
    drop: bool,
) -> None:
    """Create new SQL database for tracking replica chunks.

    Parameters
    ----------
    ppdb_config : `str`
        Path to the PPDB configuration which must be of type
        `PpdbBigQueryConfig`.
    drop : `bool`
        If `True` then drop existing tables.
    """
    bq_config = PpdbConfig.from_uri(ppdb_config)
    if not isinstance(bq_config, PpdbBigQueryConfig):
        raise ValueError(f"PPDB configuration must be of type 'bigquery': {ppdb_config}")
    sql_config = bq_config.sql
    if sql_config is None:
        raise ValueError("SQL configuration is not provided in the PPDB configuration")
    PpdbReplicaChunkSql.init_database(sql_config, drop=drop)
