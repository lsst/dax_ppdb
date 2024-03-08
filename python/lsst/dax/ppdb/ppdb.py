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

__all__ = ["Ppdb"]

from abc import ABC, abstractmethod
from dataclasses import dataclass

import lsst.daf.base as dafBase
from lsst.dax.apdb import ApdbInsertId, ApdbMetadata
from lsst.resources import ResourcePathExpression

from ._factory import ppdb_type
from .config import PpdbConfig


@dataclass(frozen=True)
class PpdbInsertId(ApdbInsertId):
    """InsertId with additional PPDB-specific info"""

    replica_time: dafBase.DateTime
    """Time of this insert, usually corresponds to visit time
    (`dafBase.DateTime`).
    """


class Ppdb(ABC):
    """Class defining an interface for PPDB management operations."""

    @classmethod
    def from_config(cls, config: PpdbConfig) -> Ppdb:
        """Create Ppdb instance from configuration object.

        Parameters
        ----------
        config : `PpdbConfig`
            Configuration object, type of this object determines type of the
            Ppdb implementation.

        Returns
        -------
        ppdb : `Ppdb`
            Instance of `Ppdb` class.
        """
        # Dispatch to actual implementation class based on config type.

        ppdb_class = ppdb_type(config)
        return ppdb_class(config)

    @classmethod
    def from_uri(cls, uri: ResourcePathExpression) -> Ppdb:
        """Read PPDB configuration from URI and make a Ppdb instance.

        Parameters
        ----------
        uri : `~lsst.resources.ResourcePathExpression`
            Location of the file containing serialized configuration in YAML
            format.

        Returns
        -------
        ppdb : `Ppdb`
            Instance of `Ppdb` class.
        """
        config = PpdbConfig.from_uri(uri)
        return cls.from_config(config)

    @property
    @abstractmethod
    def metadata(self) -> ApdbMetadata:
        """Object controlling access to metadata
        (`~lsst.dax.apdbApdbMetadata`).
        """
        raise NotImplementedError()

    @abstractmethod
    def get_insert_ids(self) -> list[PpdbInsertId] | None:
        """Return collection of insert identifiers known to the database.

        Returns
        -------
        ids : `list` [`PpdbInsertId`] or `None`
            List of identifiers, they may be time-ordered if database supports
            ordering. `None` is returned if database is not configured to store
            insert identifiers.
        """
        raise NotImplementedError()
