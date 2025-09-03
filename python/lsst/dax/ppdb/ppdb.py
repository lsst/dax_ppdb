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

import astropy.time

from lsst.dax.apdb import ApdbMetadata, ApdbTableData, ReplicaChunk
from lsst.resources import ResourcePathExpression

from ._factory import ppdb_type
from .config import PpdbConfig


@dataclass(frozen=True)
class PpdbReplicaChunk(ReplicaChunk):
    """ReplicaChunk with additional PPDB-specific info."""

    replica_time: astropy.time.Time
    """Time when this bucket was replicated (`astropy.time.Time`)."""


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
        (`~lsst.dax.apdb.ApdbMetadata`).
        """
        raise NotImplementedError()

    # TODO: Change return type to Sequence so that we can support variadic
    # types
    @abstractmethod
    def get_replica_chunks(self, start_chunk_id: int | None = None) -> list[PpdbReplicaChunk] | None:
        """Return collection of replica chunks known to the database.

        Parameters
        ----------
        start_chunk_id : `int`, optional
            If specified this will be the starting chunk ID to return. If not
            specified then all chunks areturned

        Returns
        -------
        chunks : `list` [`PpdbReplicaChunk`] or `None`
            List of chunks, they may be time-ordered if database supports
            ordering. `None` is returned if database is not configured to store
            chunk information.
        """
        raise NotImplementedError()

    @abstractmethod
    def store(
        self,
        replica_chunk: ReplicaChunk,
        objects: ApdbTableData,
        sources: ApdbTableData,
        forced_sources: ApdbTableData,
        *,
        update: bool = False,
    ) -> None:
        """Copy APDB data to PPDB.

        Parameters
        ----------
        replica_chunk : `~lsst.dax.apdb.ReplicaChunk`
            Insertion ID for APDB data.
        objects : `~lsst.dax.apdb.ApdbTableData`
            Matching APDB data for DiaObjects.
        sources : `~lsst.dax.apdb.ApdbTableData`
            Matching APDB data for DiaSources.
        forced_sources : `~lsst.dax.apdb.ApdbTableData`
            Matching APDB data for DiaForcedSources.
        update : `bool`, optional
            If `True` then allow updates for existing  data from the same
            ``replica_chunk``.

        Notes
        -----
        Replication from APDB to PPDB should happen in the same order as
        insertion order for APDB, i.e. in the order of increasing
        ``replica_chunk.id`` values.
        """
        raise NotImplementedError()
