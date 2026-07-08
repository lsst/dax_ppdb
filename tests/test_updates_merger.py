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

import unittest

from google.cloud import bigquery

from lsst.dax.ppdb.bigquery import DatasetType
from lsst.dax.ppdb.bigquery.updates import (
    DiaForcedSourceUpdatesMerger,
    DiaObjectUpdatesMerger,
    DiaSourceUpdatesMerger,
    UpdateRecordExpander,
    UpdatesTable,
)
from lsst.dax.ppdb.tests import (
    create_datasets,
    drop_datasets,
    have_valid_google_credentials,
    json_rows_to_buf,
    make_bigquery_config,
)
from lsst.dax.ppdb.tests._updates import _create_test_update_records


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class TestUpdatesMerger(unittest.TestCase):
    """Test UpdatesMerger functionality."""

    dataset_types = (DatasetType.INTERNAL, DatasetType.STAGING)

    def setUp(self):
        self.client = bigquery.Client()

        self.config = make_bigquery_config("test_updates_merger")

        # Add cleanup for datasets after test.
        self.addCleanup(drop_datasets, self.config, self.dataset_types)

        create_datasets(self.config, self.dataset_types)

    def _create_target_table(self):
        schema = [
            bigquery.SchemaField("diaObjectId", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("validityEndMjdTai", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("nDiaSources", "INTEGER", mode="NULLABLE"),
        ]
        table_fqn = self.config.fqn_for(DatasetType.INTERNAL, "DiaObject")
        table = bigquery.Table(table_fqn, schema=schema)
        self.client.create_table(table)
        rows = [
            {"diaObjectId": 200001, "validityEndMjdTai": None, "nDiaSources": 3},
            {"diaObjectId": 200002, "validityEndMjdTai": None, "nDiaSources": 7},
            {"diaObjectId": 200003, "validityEndMjdTai": 59000.0, "nDiaSources": 2},
        ]
        buf = json_rows_to_buf(rows)
        job = self.client.load_table_from_file(
            buf,
            table_fqn,
            job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON),
        )
        job.result()

    def test_merge_diaobject(self):
        self._create_target_table()
        updates_table = UpdatesTable(self.client, self.config.project_id, self.config.datasets.staging)
        updates_table.create()
        update_records = _create_test_update_records()
        expanded = UpdateRecordExpander.expand_updates(update_records, 0)
        updates_table.insert(expanded)
        updates_table.create_latest_only()
        table_fqn = self.config.fqn_for(DatasetType.INTERNAL, "DiaObject")
        query = f"SELECT * FROM `{table_fqn}` ORDER BY diaObjectId"
        before = {r.diaObjectId: r for r in self.client.query(query).result()}
        merger = DiaObjectUpdatesMerger()
        merger.merge(
            client=self.client,
            updates_table_fqn=updates_table.latest_only_table_fqn,
            target_dataset_fqn=self.config.fqn_for(DatasetType.INTERNAL),
        )
        after = {r.diaObjectId: r for r in self.client.query(query).result()}
        self.assertEqual(after[200001].validityEndMjdTai, 59580.0)
        self.assertEqual(after[200001].nDiaSources, 5)
        self.assertIsNone(after[200002].validityEndMjdTai)
        self.assertEqual(after[200002].nDiaSources, 10)
        self.assertEqual(after[200003].validityEndMjdTai, before[200003].validityEndMjdTai)
        self.assertEqual(after[200003].nDiaSources, before[200003].nDiaSources)

    def test_merge_diasource(self):
        schema = [
            bigquery.SchemaField("diaSourceId", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("diaObjectId", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ssObjectId", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ssObjectReassocTimeMjdTai", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("timeWithdrawnMjdTai", "FLOAT", mode="NULLABLE"),
        ]
        table_fqn = self.config.fqn_for(DatasetType.INTERNAL, "DiaSource")
        table = bigquery.Table(table_fqn, schema=schema)
        self.client.create_table(table)
        rows = [
            {
                "diaSourceId": 100001,
                "diaObjectId": 200001,
                "ssObjectId": None,
                "ssObjectReassocTimeMjdTai": None,
                "timeWithdrawnMjdTai": None,
            },
            {
                "diaSourceId": 100002,
                "diaObjectId": 200002,
                "ssObjectId": None,
                "ssObjectReassocTimeMjdTai": None,
                "timeWithdrawnMjdTai": None,
            },
            {
                "diaSourceId": 100003,
                "diaObjectId": 200003,
                "ssObjectId": None,
                "ssObjectReassocTimeMjdTai": None,
                "timeWithdrawnMjdTai": None,
            },
            {
                "diaSourceId": 100004,
                "diaObjectId": 200004,
                "ssObjectId": None,
                "ssObjectReassocTimeMjdTai": None,
                "timeWithdrawnMjdTai": None,
            },
        ]
        job = self.client.load_table_from_file(
            json_rows_to_buf(rows),
            table_fqn,
            job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON),
        )
        job.result()

        updates_table = UpdatesTable(self.client, self.config.project_id, self.config.datasets.staging)
        updates_table.create()
        update_records = _create_test_update_records()
        expanded = UpdateRecordExpander.expand_updates(update_records, 0)
        updates_table.insert(expanded)
        updates_table.create_latest_only()

        query = f"SELECT * FROM `{table_fqn}` ORDER BY diaSourceId"
        before = {r.diaSourceId: r for r in self.client.query(query).result()}
        merger = DiaSourceUpdatesMerger()
        merger.merge(
            client=self.client,
            updates_table_fqn=updates_table.latest_only_table_fqn,
            target_dataset_fqn=self.config.fqn_for(DatasetType.INTERNAL),
        )
        after = {r.diaSourceId: r for r in self.client.query(query).result()}

        self.assertEqual(after[100001].diaObjectId, 400001)
        self.assertEqual(after[100002].ssObjectId, 2001)
        self.assertEqual(after[100002].ssObjectReassocTimeMjdTai, 59580.0)
        self.assertEqual(after[100003].timeWithdrawnMjdTai, 59580.0)
        self.assertEqual(after[100004].diaObjectId, before[100004].diaObjectId)
        self.assertEqual(after[100004].ssObjectId, before[100004].ssObjectId)
        self.assertEqual(after[100004].timeWithdrawnMjdTai, before[100004].timeWithdrawnMjdTai)

    def test_merge_diaforcedsource(self):
        schema = [
            bigquery.SchemaField("diaObjectId", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("visit", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("detector", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("timeWithdrawnMjdTai", "FLOAT", mode="NULLABLE"),
        ]
        table_fqn = self.config.fqn_for(DatasetType.INTERNAL, "DiaForcedSource")
        table = bigquery.Table(table_fqn, schema=schema)
        self.client.create_table(table)
        rows = [
            {"diaObjectId": 200001, "visit": 12345, "detector": 42, "timeWithdrawnMjdTai": None},
            {"diaObjectId": 200001, "visit": 12346, "detector": 42, "timeWithdrawnMjdTai": None},
        ]
        job = self.client.load_table_from_file(
            json_rows_to_buf(rows),
            table_fqn,
            job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON),
        )
        job.result()

        updates_table = UpdatesTable(self.client, self.config.project_id, self.config.datasets.staging)
        updates_table.create()
        update_records = _create_test_update_records()
        expanded = UpdateRecordExpander.expand_updates(update_records, 0)
        updates_table.insert(expanded)
        updates_table.create_latest_only()

        query = f"SELECT * FROM `{table_fqn}` ORDER BY diaObjectId, visit, detector"
        before = {(r.diaObjectId, r.visit, r.detector): r for r in self.client.query(query).result()}
        merger = DiaForcedSourceUpdatesMerger()
        merger.merge(
            client=self.client,
            updates_table_fqn=updates_table.latest_only_table_fqn,
            target_dataset_fqn=self.config.fqn_for(DatasetType.INTERNAL),
        )
        after = {(r.diaObjectId, r.visit, r.detector): r for r in self.client.query(query).result()}

        self.assertEqual(after[(200001, 12345, 42)].timeWithdrawnMjdTai, 59580.0)
        self.assertEqual(
            after[(200001, 12346, 42)].timeWithdrawnMjdTai,
            before[(200001, 12346, 42)].timeWithdrawnMjdTai,
        )

    def test_merge_no_updates(self):
        self._create_target_table()
        updates_table = UpdatesTable(self.client, self.config.project_id, self.config.datasets.staging)
        updates_table.create()
        updates_table.create_latest_only()
        table_fqn = self.config.fqn_for(DatasetType.INTERNAL, "DiaObject")
        before = {r.diaObjectId: r for r in self.client.query(f"SELECT * FROM `{table_fqn}`").result()}
        merger = DiaObjectUpdatesMerger()
        merger.merge(
            client=self.client,
            updates_table_fqn=updates_table.latest_only_table_fqn,
            target_dataset_fqn=self.config.fqn_for(DatasetType.INTERNAL),
        )
        after = {r.diaObjectId: r for r in self.client.query(f"SELECT * FROM `{table_fqn}`").result()}
        for obj_id in before:
            self.assertEqual(before[obj_id].validityEndMjdTai, after[obj_id].validityEndMjdTai)
            self.assertEqual(before[obj_id].nDiaSources, after[obj_id].nDiaSources)


if __name__ == "__main__":
    unittest.main()
