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

__all__ = ["config_type_for_name", "ppdb_type", "ppdb_type_for_name"]

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import PpdbConfig
    from .sql import PpdbSql


def ppdb_type(config: PpdbConfig) -> type[PpdbSql]:
    """Return Ppdb class matching Ppdb configuration type.

    Parameters
    ----------
    config : `PpdbConfig`
        Configuration object, sub-class of PpdbConfig.

    Returns
    -------
    type : `type` [`Ppdb`]
        Subclass of `Ppdb` class.

    Raises
    ------
    TypeError
        Raised if type of ``config`` does not match any known types.
    """
    from .sql import PpdbSqlConfig

    if type(config) is PpdbSqlConfig:
        from .sql import PpdbSql

        return PpdbSql

    raise TypeError(f"Unknown type of config object: {type(config)}")


def ppdb_type_for_name(type_name: str) -> type[PpdbSql]:
    """Return Ppdb class matching type name.

    Parameters
    ----------
    type_name : `str`
        Short type name of Ppdb implement, for now only "sql" is supported.

    Returns
    -------
    type : `type` [`Ppdb`]
        Subclass of `Ppdb` class.

    Raises
    ------
    TypeError
        Raised if ``type_name`` does not match any known types.
    """
    if type_name == "sql":
        from .sql import PpdbSql

        return PpdbSql

    raise TypeError(f"Unknown type name: {type_name}")


def config_type_for_name(type_name: str) -> type[PpdbConfig]:
    """Return PpdbConfig class matching type name.

    Parameters
    ----------
    type_name : `str`
        Short type name of Ppdb implement, for now only "sql" is supported.

    Returns
    -------
    type : `type` [`Ppdb`]
        Subclass of `PpdbConfig` class.

    Raises
    ------
    TypeError
        Raised if ``type_name`` does not match any known types.
    """
    if type_name == "sql":
        from .sql import PpdbSqlConfig

        return PpdbSqlConfig

    raise TypeError(f"Unknown type name: {type_name}")
