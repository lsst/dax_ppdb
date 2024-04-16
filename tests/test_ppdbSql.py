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

import gc
import os
import shutil
import tempfile
import unittest
from typing import Any

from lsst.dax.ppdb import PpdbConfig
from lsst.dax.ppdb.sql import PpdbSql
from lsst.dax.ppdb.tests import PpdbTest

try:
    import testing.postgresql
except ImportError:
    testing = None

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")


class ApdbSQLiteTestCase(PpdbTest, unittest.TestCase):
    """A test case for PpdbSql class using SQLite backend."""

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.db_url = f"sqlite:///{self.tempdir}/apdb.sqlite3"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def make_instance(self, **kwargs: Any) -> PpdbConfig:
        """Make config class instance used in all tests."""
        return PpdbSql.init_database(db_url=self.db_url, schema_file=TEST_SCHEMA, **kwargs)


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class ApdbPostgresTestCase(PpdbTest, unittest.TestCase):
    """A test case for ApdbSql class using Postgres backend."""

    @classmethod
    def setUpClass(cls) -> None:
        # Create the postgres test server.
        cls.postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.postgresql.clear_cache()
        super().tearDownClass()

    def setUp(self) -> None:
        self.server = self.postgresql()

    def tearDown(self) -> None:
        self.server = self.postgresql()

    def make_instance(self, **kwargs: Any) -> PpdbConfig:
        """Make config class instance used in all tests."""
        return PpdbSql.init_database(db_url=self.server.url(), schema_file=TEST_SCHEMA, **kwargs)


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class ApdbPostgresNamespaceTestCase(ApdbPostgresTestCase):
    """A test case for ApdbSql class using Postgres backend with schema name"""

    # use mixed case to trigger quoting
    schema_name = "test_schema001"

    def make_instance(self, **kwargs: Any) -> PpdbConfig:
        """Make config class instance used in all tests."""
        return super().make_instance(schema_name=self.schema_name, **kwargs)


if __name__ == "__main__":
    unittest.main()
