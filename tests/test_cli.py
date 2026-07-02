# This file is part of dax_ppdb.
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

import os
import tempfile
import unittest

import yaml
from google.cloud import bigquery

from lsst.dax.ppdb.bigquery.ppdb_bigquery_config import (
    DatasetType,
)
from lsst.dax.ppdb.cli import ppdb_cli
from lsst.dax.ppdb.tests._bigquery import (
    drop_datasets,
    have_valid_google_credentials,
    make_bigquery_config,
    search_indexes_enabled,
)


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class CreateDatasetsTestCase(unittest.TestCase):
    """Integration tests for the ``ppdb-cli create-datasets`` command."""

    def setUp(self) -> None:
        self.client = bigquery.Client()

        self.config = make_bigquery_config(test_name="test_cli_create_datasets")

        # Serialize the configuration to a YAML file that the CLI can load.
        self.tempdir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.tempdir, "ppdb_config.yaml")
        config_dict = self.config.model_dump(exclude_unset=True, exclude_defaults=True)
        config_dict["implementation_type"] = "bigquery"
        with open(self.config_path, "w") as config_file:
            yaml.dump(config_dict, config_file)

        # Add cleanup of datasets after test.
        self.addCleanup(drop_datasets, self.config)

    def test_create_datasets(self) -> None:
        """Test that ``ppdb-cli create-datasets`` creates the BigQuery
        datasets described by the configuration file.
        """
        argv = ["create-datasets", self.config_path]

        # Avoid exhausting BigQuery search index creation quotas unless
        # explicitly enabled for the test run.
        if not search_indexes_enabled():
            argv.append("--disable-search-indexes")

        ppdb_cli.main(argv)

        # Verify that the datasets were created in BigQuery with the correct
        # number of tables.
        for dataset_type in DatasetType:
            dataset_fqn = self.config.fqn_for(dataset_type)
            self.client.get_dataset(dataset_fqn)
            tables = list(self.client.list_tables(dataset_fqn))
            self.assertEqual(len(tables), 3)


if __name__ == "__main__":
    unittest.main()
