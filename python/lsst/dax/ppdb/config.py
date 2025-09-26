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

__all__ = ["PpdbConfig"]

from collections.abc import Mapping
from typing import Any

import yaml
from pydantic import BaseModel

from lsst.resources import ResourcePath, ResourcePathExpression

from ._factory import config_type_for_name


class PpdbConfig(BaseModel):
    """Base class for PPDB configuration types."""

    @classmethod
    def from_uri(cls, uri: ResourcePathExpression) -> PpdbConfig:
        """Load configuration object from external file.

        Parameters
        ----------
        uri : `~lsst.resources.ResourcePathExpression`
            Location of the file containing serialized configuration in YAML
            format.

        Returns
        -------
        config : `PpdbConfig`
            PPD configuration object.
        """
        path = ResourcePath(uri)
        config_str = path.read()
        config_object = yaml.safe_load(config_str)
        if not isinstance(config_object, Mapping):
            raise TypeError("YAML configuration file does not represent valid object")
        config_dict: dict[str, Any] = dict(config_object)
        type_name = config_dict.pop("implementation_type", None)
        if not type_name:
            raise LookupError("YAML configuration file does not have `implementation_type` key")
        klass = config_type_for_name(type_name)
        return klass(**config_dict)
