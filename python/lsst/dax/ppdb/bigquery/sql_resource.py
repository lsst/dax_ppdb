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


from lsst.resources import ResourcePath


class SqlResource:
    """Class for loading SQL query text from a resource file and optionally
    formatting it with provided arguments.

    Parameters
    ----------
    sql_resource_name : `str`
        Base name of the SQL file (without .sql extension) containing the
        query in the `resources/config/sql` directory within the
        `lsst.dax.ppdb` package.
    format_args : `dict` [ `str`, `str` ], optional
        Optional dictionary of arguments for formatting the SQL text.
    """

    SQL_RESOURCE_URI = "resource://lsst.dax.ppdb/resources/config/sql"

    def __init__(self, sql_resource_name: str, format_args: dict[str, str] | None = None) -> None:
        sql_resource_path = f"{self.SQL_RESOURCE_URI}/{sql_resource_name}.sql"
        sql = ResourcePath(sql_resource_path).read().decode("utf-8")
        if format_args is not None:
            try:
                sql = sql.format(**format_args)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to format SQL resource at {sql_resource_path} with arguments {format_args}"
                ) from e
        self._sql = sql

    @property
    def sql(self) -> str:
        """SQL query string (`str`)."""
        return self._sql
