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

from collections.abc import Sequence

import pyarrow
from felis.datamodel import DataType

_FELIS_TYPE_MAP = {
    DataType.long: pyarrow.int64(),
    DataType.boolean: pyarrow.bool_(),
    DataType.char: pyarrow.string(),
    DataType.double: pyarrow.float64(),
    DataType.int: pyarrow.int32(),
    DataType.float: pyarrow.float64(),
    DataType.short: pyarrow.int16(),
    DataType.timestamp: pyarrow.timestamp("ms", tz="UTC"),
    DataType.string: pyarrow.string(),
}


def _felis_to_arrow_type(felis_type: DataType) -> pyarrow.DataType:
    """Convert a Felis data type to an Arrow data type.

    Parameters
    ----------
    felis_type : `felis.datamodel.DataType`
        Felis data type to convert.

    Returns
    -------
    arrow_type : `pyarrow.DataType`
        Corresponding Arrow data type.
    """
    if felis_type not in _FELIS_TYPE_MAP:
        raise ValueError(f"Unsupported Felis type: {felis_type}")
    return _FELIS_TYPE_MAP.get(felis_type)


def create_arrow_schema(column_defs: Sequence[tuple[str, DataType]]) -> pyarrow.Schema:
    return pyarrow.schema([(name, _felis_to_arrow_type(dtype)) for name, dtype in column_defs])
