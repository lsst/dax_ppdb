# This file is part of dax_ppdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["PpdbTest"]

import unittest
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from ..config import PpdbConfig
from ..ppdb import Ppdb

if TYPE_CHECKING:

    class TestCaseMixin(unittest.TestCase):
        """Base class for mixin test classes that use TestCase methods."""

else:

    class TestCaseMixin:
        """Do-nothing definition of mixin base class for regular execution."""


class PpdbTest(TestCaseMixin, ABC):
    """Base class for Ppdb tests that can be specialized for concrete
    implementation.

    This can only be used as a mixin class for a unittest.TestCase and it
    calls various assert methods.
    """

    @abstractmethod
    def make_instance(self, **kwargs: Any) -> PpdbConfig:
        """Make database instance and return configuration for it."""
        raise NotImplementedError()

    def test_empty_db(self) -> None:
        """Test for instantiation a database and making queries on empty
        database.
        """
        config = self.make_instance()
        ppdb = Ppdb.from_config(config)
        chunks = ppdb.get_replica_chunks()
        if chunks is not None:
            self.assertEqual(len(chunks), 0)
