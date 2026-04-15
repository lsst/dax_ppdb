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

from pydantic import BaseModel


class TableRefs(BaseModel, frozen=True):
    """Fully-qualified BigQuery table references for a set of tables.

    Provides named properties for production, staging, and promoted temporary
    table FQNs derived from the base ``table_names``.  The format strings
    used for staging and promoted-tmp names are configurable.

    Parameters
    ----------
    project_id
        GCP project ID.
    dataset_id
        BigQuery dataset ID.
    table_names
        Base table names.
    staging_format
        Format string for staging table names.
    promoted_tmp_format
        Format string for promoted temporary table names.
    """

    project_id: str
    dataset_id: str
    table_names: tuple[str, ...]
    staging_format: str = "_{}_staging"
    promoted_tmp_format: str = "_{}_promoted_tmp"

    def _fqns(self, fmt: str = "{}") -> list[str]:
        """Build fully-qualified names using the given format string."""
        return [f"{self.project_id}.{self.dataset_id}.{fmt.format(name)}" for name in self.table_names]

    @property
    def prod(self) -> list[str]:
        """Fully-qualified production table names (`list` [`str`])."""
        return self._fqns()

    @property
    def staging(self) -> list[str]:
        """Fully-qualified staging table names (`list` [`str`])."""
        return self._fqns(self.staging_format)

    @property
    def promoted_tmp(self) -> list[str]:
        """Fully-qualified promoted temporary table names (`list` [`str`])."""
        return self._fqns(self.promoted_tmp_format)
