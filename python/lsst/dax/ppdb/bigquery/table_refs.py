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

__all__ = ["TableRefs"]

from .ppdb_bigquery_config import DatasetType, PpdbBigQueryConfig


class TableRefs:
    """Builder for fully qualified BigQuery table references.

    Parameters
    ----------
    config
        Configuration providing the project ID and dataset names.
    """

    def __init__(self, config: PpdbBigQueryConfig):
        self._config = config

    def staging(self, table_name: str) -> str:
        """Return the fully qualified staging table reference.

        Parameters
        ----------
        table_name
            Name of the table.

        Returns
        -------
        `str`
            Fully qualified staging table reference.
        """
        return self._config.fqn_for(DatasetType.STAGING, table_name)

    def promotion(self, table_name: str) -> str:
        """Return the fully qualified promotion table reference.

        Parameters
        ----------
        table_name
            Name of the table.

        Returns
        -------
        `str`
            Fully qualified promotion table reference.
        """
        return self._config.fqn_for(DatasetType.PROMOTION, table_name)

    def internal(self, table_name: str) -> str:
        """Return the fully qualified internal table reference.

        Parameters
        ----------
        table_name
            Name of the table.

        Returns
        -------
        `str`
            Fully qualified internal table reference.
        """
        return self._config.fqn_for(DatasetType.INTERNAL, table_name)

    def public(self, table_name: str) -> str:
        """Return the fully qualified public table reference.

        Parameters
        ----------
        table_name
            Name of the table.

        Returns
        -------
        `str`
            Fully qualified public table reference.
        """
        return self._config.fqn_for(DatasetType.PUBLIC, table_name)
