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
import sys
import tempfile
import unittest
import uuid

import yaml
from google.cloud import bigquery

from lsst.dax.ppdb.bigquery.ppdb_bigquery_config import (
    Datasets,
    DatasetType,
    PpdbBigQueryConfig,
)
from lsst.dax.ppdb.cli import ppdb_cli
from lsst.dax.ppdb.sql import PpdbSqlBaseConfig
from lsst.dax.ppdb.tests._bigquery import (
    have_valid_google_credentials,
    search_indexes_enabled,
)


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class CreateDatasetsTestCase(unittest.TestCase):
    """Integration tests for the ``ppdb-cli create-datasets`` command."""

    def setUp(self) -> None:
        self.client = bigquery.Client()
        self.project_id = self.client.project

        # Use unique dataset names so concurrent or repeated test runs do not
        # collide with one another.
        suffix = uuid.uuid4().hex[:8]
        self.datasets = Datasets(
            internal=f"test_cli_internal_{suffix}",
            public=f"test_cli_public_{suffix}",
            staging=f"test_cli_staging_{suffix}",
        )

        config = PpdbBigQueryConfig(
            project_id=self.project_id,
            dataset_id="dummy",  # TODO: This will be removed in DM-54681.
            bucket_name="test-bucket",
            object_prefix="data/test",
            replication_dir="/tmp",
            sql=PpdbSqlBaseConfig(db_url="sqlite:///:memory:"),
            datasets=self.datasets,
        )

        # Serialize the configuration to a YAML file that the CLI can load.
        self.tempdir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.tempdir, "ppdb_config.yaml")
        config_dict = config.model_dump(exclude_unset=True, exclude_defaults=True)
        config_dict["implementation_type"] = "bigquery"
        with open(self.config_path, "w") as config_file:
            yaml.dump(config_dict, config_file)

    def tearDown(self) -> None:
        for dataset_type in DatasetType:
            dataset_name = self.datasets.name_for(dataset_type)
            dataset_fqn = f"{self.project_id}.{dataset_name}"
            try:
                self.client.delete_dataset(dataset_fqn, delete_contents=True, not_found_ok=True)
            except Exception as e:
                print(f"Failed to delete {dataset_fqn}: {type(e).__name__}: {e}", file=sys.stderr)

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
            dataset_name = self.datasets.name_for(dataset_type)
            self.client.get_dataset(f"{self.project_id}.{dataset_name}")
            tables = list(self.client.list_tables(f"{self.project_id}.{dataset_name}"))
            self.assertEqual(len(tables), 3)


if __name__ == "__main__":
    unittest.main()
