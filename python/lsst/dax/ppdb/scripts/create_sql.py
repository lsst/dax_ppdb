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

__all__ = ["create_sql"]

import yaml

from ..sql import PpdbSql


def create_sql(
    db_url: str,
    schema: str | None,
    config_path: str,
    felis_path: str,
    felis_schema: str,
    connection_pool: bool,
    isolation_level: str | None,
    connection_timeout: float | None,
    drop: bool,
) -> None:
    """Create new PPDB instance in SQL database.

    Parameters
    ----------
    db_url : `str`
        SQLAlchemy connection string.
    schema : `str` or `None`
        Database schema name, `None` to use default schema.
    config_path : `str`
        Name of the file to write PPDB configuration.
    felis_path : `str`
        Path to the Felis YAML file with table schema definition.
    felis_schema : `str`
        Name of the schema defined in felis YAML file.
    connection_pool : `bool`
        If True then enable connection pool.
    isolation_level : `str` or `None`
        Transaction isolation level, if unset then backend-default value is
        used.
    connection_timeout: `float` or `None`
        Maximum connection timeout in seconds.
    drop : `bool`
        If `True` then drop existing tables.
    """
    config = PpdbSql.init_database(
        db_url=db_url,
        schema_name=schema,
        schema_file=felis_path,
        felis_schema=felis_schema,
        use_connection_pool=connection_pool,
        isolation_level=isolation_level,
        connection_timeout=connection_timeout,
        drop=drop,
    )
    config_dict = config.model_dump(exclude_unset=True, exclude_defaults=True)
    config_dict["implementation_type"] = "sql"
    with open(config_path, "w") as config_file:
        yaml.dump(config_dict, config_file)
