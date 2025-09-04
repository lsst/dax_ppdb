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

from ..ppdb import PpdbConfig


class PpdbSqlConfig(PpdbConfig):
    """Configuration for the `PpdbSql` class."""

    db_url: str
    """SQLAlchemy database connection URI."""

    schema_name: str | None = None
    """Database schema name, if `None` then default schema is used."""

    felis_path: str | None = None
    """Name of YAML file with ``felis`` schema, if `None` then default schema
    file is used.
    """

    felis_schema: str | None = None
    """Name of the schema in YAML file, if `None` then file has to contain
    single schema.
    """

    use_connection_pool: bool = True
    """If True then allow use of connection pool."""

    isolation_level: str | None = None
    """Transaction isolation level, if unset then backend-default value is
    used.
    """

    connection_timeout: float | None = None
    """Maximum connection timeout in seconds."""
