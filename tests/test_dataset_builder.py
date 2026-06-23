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

import sys
import unittest
import uuid
from unittest.mock import Mock, patch

from felis import Schema
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from lsst.dax.apdb import ApdbTables
from lsst.dax.ppdb.bigquery.ppdb_bigquery_config import (
    Datasets,
    DatasetType,
    PpdbBigQueryConfig,
)
from lsst.dax.ppdb.bigquery.schema.dataset_builder import (
    BaseDatasetBuilder,
    DatasetBuilder,
    DatasetBuilderError,
    DatasetBuildManager,
    InternalDatasetBuilder,
    PublicDatasetBuilder,
    SearchIndexDataType,
    SearchIndexDefinition,
    StagingDatasetBuilder,
    _update_schema_fields,
)
from lsst.dax.ppdb.bigquery.schema.felis_converter import FelisConverter
from lsst.dax.ppdb.sql import PpdbSqlBaseConfig
from lsst.dax.ppdb.tests._bigquery import (
    have_valid_google_credentials,
    search_indexes_enabled,
)

# Determine whether search index creation is enabled for tests.
_SEARCH_INDEXES_ENABLED = search_indexes_enabled()


class DatasetBuilderTestMixin:
    """Shared setup and helpers for dataset builder unit tests."""

    BIGQUERY_CLIENT_PATCH = "lsst.dax.ppdb.bigquery.schema.dataset_builder.bigquery.Client"
    EXPECTED_TABLES = {"DiaObject", "DiaSource", "DiaForcedSource"}
    EXPECTED_VIEWS = ["DiaSource", "DiaForcedSource"]

    def setUp(self) -> None:
        """Test case setup including schema, converter, and standard config
        with test dataset names.
        """
        self.schema = self._load_test_schema()
        self.converter = FelisConverter(self.schema)
        self.config = self._make_test_config(
            project_id="test-project",
            datasets=Datasets(
                internal="test_dataset_builder_internal",
                public="test_dataset_builder_public",
                staging="test_dataset_builder_staging",
            ),
        )

    @staticmethod
    def _load_test_schema() -> Schema:
        """Load the test schema from the default URI."""
        return Schema.from_uri("resource://lsst.sdm.schemas/ppdb.yaml", context={"id_generation": True})

    @staticmethod
    def _make_test_config(project_id: str, datasets: Datasets) -> PpdbBigQueryConfig:
        """Create a standard PpdbBigQueryConfig for tests with specified
        project ID and dataset names.
        """
        return PpdbBigQueryConfig(
            project_id=project_id,
            dataset_id="test_dataset_builder",
            bucket_name="test-bucket",
            object_prefix="data/test",
            replication_dir="/tmp",
            sql=PpdbSqlBaseConfig(db_url="sqlite:///:memory:"),
            datasets=datasets,
        )

    def _make_public_views(self) -> list[bigquery.Table]:
        """Create DiaSource and DiaForcedSource views with public FQN."""
        public_fqn = self.config.fqn_for(DatasetType.PUBLIC)
        return [
            bigquery.Table(f"{public_fqn}.{ApdbTables.DiaSource.value}"),
            bigquery.Table(f"{public_fqn}.{ApdbTables.DiaForcedSource.value}"),
        ]

    def _inject_mock_client(self, manager: DatasetBuildManager) -> Mock:
        """Inject a Mock client into manager and return it."""
        client = Mock()
        manager._client = client
        return client

    def _make_mock_builder(self) -> Mock:
        """Create a DatasetBuilder mock with empty outputs."""
        builder = Mock(spec=DatasetBuilder)
        builder.build_tables.return_value = []
        builder.build_views.return_value = []
        builder.build_search_indexes.return_value = []
        return builder

    def _make_builder(self, builder_class: type) -> DatasetBuilder:
        """Create a builder with standard config/converter."""
        return builder_class(config=self.config, converter=self.converter)

    def _make_manager(
        self, exists_ok: bool = False, configure_authorized_views: bool = False
    ) -> DatasetBuildManager:
        """Create DatasetBuildManager with the BigQuery client patched out as
        a Mock.
        """
        with patch(self.BIGQUERY_CLIENT_PATCH):
            return DatasetBuildManager(
                config=self.config,
                schema=self.schema,
                exists_ok=exists_ok,
                configure_authorized_views=configure_authorized_views,
            )

    def _make_fqn_table(self, dataset_type: DatasetType, table_name: str) -> bigquery.Table:
        """Create fully-qualified table reference."""
        fqn = self.config.fqn_for(dataset_type)
        return bigquery.Table(f"{fqn}.{table_name}")


class BaseDatasetBuilderTestCase(DatasetBuilderTestMixin, unittest.TestCase):
    """Base dataset builder unit tests."""

    def test_optional_methods_return_empty_lists(self) -> None:
        """Test that BaseDatasetBuilder optional methods are no-ops."""
        builder = self._make_builder(BaseDatasetBuilder)

        self.assertEqual(builder.build_tables(), [])
        self.assertEqual(builder.build_views(), [])
        self.assertEqual(builder.build_search_indexes(), [])


class PublicDatasetBuilderTestCase(DatasetBuilderTestMixin, unittest.TestCase):
    """Public dataset builder unit tests."""

    def test_creates_diaobject_with_schema_changes(self) -> None:
        """Test that PublicDatasetBuilder creates DiaObject table without
        validityEndMjdTai and with geo_point.
        """
        builder = self._make_builder(PublicDatasetBuilder)

        tables = builder.build_tables()

        self.assertEqual(len(tables), 1)
        table = tables[0]
        self.assertEqual(table.table_id, ApdbTables.DiaObject.value)

        field_names = [field.name for field in table.schema]
        self.assertIn("diaObjectId", field_names)
        self.assertNotIn("validityEndMjdTai", field_names)
        self.assertIn("geo_point", field_names)

        source_table = self.converter.find_table(ApdbTables.DiaObject.value)
        # We remove validityEndMjdTai and add geo_point, so the count is the
        # same.
        self.assertEqual(len(field_names), len(source_table.columns))

    def test_creates_explicit_views(self) -> None:
        """Test that PublicDatasetBuilder creates explicit views with fully
        qualified columns and proper FROM clauses.
        """
        builder = self._make_builder(PublicDatasetBuilder)
        public_dataset_fqn = self.config.fqn_for(DatasetType.PUBLIC)
        internal_dataset_fqn = self.config.fqn_for(DatasetType.INTERNAL)

        views = builder.build_views()

        self.assertEqual([view.table_id for view in views], self.EXPECTED_VIEWS)
        for view_name, view in zip(self.EXPECTED_VIEWS, views, strict=True):
            # Views must always have a materialized SQL query.
            self.assertTrue(view.view_query and view.view_query.strip())

            # Validate that all columns from the source table are included in
            # the view and that the expected source table is used.
            table = self.converter.find_table(view_name)
            column_names = [f"`{column.name}`" for column in table.columns]
            expected_columns = ", ".join(column_names)
            expected_query = f"SELECT {expected_columns} FROM `{internal_dataset_fqn}.{view_name}`"
            self.assertEqual(
                view.view_query,
                expected_query,
            )

            # Ensure each view is created in the public dataset namespace.
            project, dataset = public_dataset_fqn.split(".", 1)
            self.assertEqual(view.project, project)
            self.assertEqual(view.dataset_id, dataset)
            self.assertEqual(view.table_id, view_name)

    def test_creates_search_index_definitions(self) -> None:
        """Test that PublicDatasetBuilder creates expected search indexes."""
        builder = self._make_builder(PublicDatasetBuilder)

        definitions = builder.build_search_indexes()

        self.assertEqual(
            definitions,
            [
                SearchIndexDefinition(
                    table_name=ApdbTables.DiaObject.value,
                    field_data_types=(("diaObjectId", SearchIndexDataType.INT64),),
                )
            ],
        )


class InternalDatasetBuilderTestCase(DatasetBuilderTestMixin, unittest.TestCase):
    """Internal dataset builder unit tests."""

    def test_adds_geo_point_and_clustering(self) -> None:
        """Test that InternalDatasetBuilder adds geo_point GEOGRAPHY field
        with clustering.
        """
        builder = self._make_builder(InternalDatasetBuilder)

        tables = builder.build_tables()

        self.assertEqual({table.table_id for table in tables}, self.EXPECTED_TABLES)
        for table in tables:
            geo_point = next(field for field in table.schema if field.name == "geo_point")
            self.assertEqual(geo_point.field_type, "GEOGRAPHY")
            self.assertEqual(geo_point.mode, "REQUIRED")
            self.assertEqual(table.clustering_fields, ["geo_point"])

    def test_creates_search_index_definitions(self) -> None:
        """Test that InternalDatasetBuilder creates expected search indexes."""
        builder = InternalDatasetBuilder(config=self.config, converter=self.converter)

        definitions = builder.build_search_indexes()

        self.assertEqual(len(definitions), 3)
        self.assertEqual(
            {definition.table_name for definition in definitions},
            self.EXPECTED_TABLES,
        )
        for definition in definitions:
            self.assertEqual(
                definition.field_data_types,
                (("diaObjectId", SearchIndexDataType.INT64),),
            )
            self.assertEqual(definition.index_name, f"{definition.table_name}_diaObjectId_idx")


class StagingDatasetBuilderTestCase(DatasetBuilderTestMixin, unittest.TestCase):
    """Staging dataset builder unit tests."""

    def test_adds_replica_chunk(self) -> None:
        """Test that StagingDatasetBuilder adds apdb_replica_chunk INT64
        field.
        """
        builder = self._make_builder(StagingDatasetBuilder)

        tables = builder.build_tables()

        self.assertEqual({table.table_id for table in tables}, self.EXPECTED_TABLES)
        for table in tables:
            chunk_field = next(field for field in table.schema if field.name == "apdb_replica_chunk")
            self.assertEqual(chunk_field.field_type, "INT64")
            self.assertEqual(chunk_field.mode, "REQUIRED")


class UpdateSchemaFieldsTestCase(unittest.TestCase):
    """Tests for the _update_schema_fields helper."""

    def _make_table(self) -> bigquery.Table:
        """Create a table with a single existing field."""
        table = bigquery.Table("project.dataset.table")
        table.schema = [bigquery.SchemaField("existing", "INT64")]
        return table

    def test_adds_new_fields(self) -> None:
        """Test that new fields are appended to the table schema."""
        table = self._make_table()

        _update_schema_fields(table, bigquery.SchemaField("added", "STRING"))

        self.assertEqual([field.name for field in table.schema], ["existing", "added"])

    def test_rejects_duplicate_field(self) -> None:
        """Test that adding an existing field raises and names the
        duplicate.
        """
        table = self._make_table()

        with self.assertRaisesRegex(ValueError, r"already exist in the schema: existing\."):
            _update_schema_fields(table, bigquery.SchemaField("existing", "INT64"))

    def test_reports_all_duplicate_fields(self) -> None:
        """Test that every duplicate field name is reported in the message."""
        table = self._make_table()
        table.schema = [
            bigquery.SchemaField("existing", "INT64"),
            bigquery.SchemaField("other", "INT64"),
        ]

        with self.assertRaises(ValueError) as cm:
            _update_schema_fields(
                table,
                bigquery.SchemaField("existing", "INT64"),
                bigquery.SchemaField("other", "INT64"),
                bigquery.SchemaField("added", "STRING"),
            )

        message = str(cm.exception)
        self.assertRegex(message, r"\bexisting\b")
        self.assertRegex(message, r"\bother\b")
        self.assertNotRegex(message, r"\badded\b")


class SearchIndexTestCase(DatasetBuilderTestMixin, unittest.TestCase):
    """Search index definition and creation tests."""

    def test_definition_index_name(self) -> None:
        """Test that SearchIndexDefinition builds the expected index name."""
        definition = SearchIndexDefinition(
            table_name=ApdbTables.DiaObject.value,
            field_data_types=(
                ("diaObjectId", SearchIndexDataType.INT64),
                ("insertTime", SearchIndexDataType.TIMESTAMP),
            ),
        )

        self.assertEqual(definition.index_name, "DiaObject_diaObjectId_insertTime_idx")

    def test_executes_expected_query(self) -> None:
        """Test that _create_search_indexes executes expected BigQuery SQL."""
        manager = self._make_manager()

        definition = SearchIndexDefinition(
            table_name=ApdbTables.DiaObject.value,
            field_data_types=(("diaObjectId", SearchIndexDataType.INT64),),
        )
        builder = Mock(spec=DatasetBuilder)
        builder.build_search_indexes.return_value = [definition]
        query_job = Mock()
        client = self._inject_mock_client(manager)
        client.query.return_value = query_job

        manager._create_search_indexes(builder, DatasetType.PUBLIC)

        # Query execution should block until completion.
        query_job.result.assert_called_once_with()
        client.query.assert_called_once()
        sql = client.query.call_args.args[0]

        # Generated SQL should target the expected index, table, and data type.
        self.assertIn("CREATE SEARCH INDEX DiaObject_diaObjectId_idx", sql)
        self.assertIn(
            f"ON `{self.config.fqn_for(DatasetType.PUBLIC)}.{ApdbTables.DiaObject.value}`(diaObjectId)",
            sql,
        )
        self.assertIn("OPTIONS (data_types = ['INT64']);", sql)

    def test_conflict_respects_exists_ok(self) -> None:
        """Test that _create_search_indexes handles conflicts per exists_ok."""
        definition = SearchIndexDefinition(
            table_name=ApdbTables.DiaObject.value,
            field_data_types=(("diaObjectId", SearchIndexDataType.INT64),),
        )
        builder = Mock(spec=DatasetBuilder)
        builder.build_search_indexes.return_value = [definition]

        manager = self._make_manager()

        query_job = Mock()
        query_job.result.side_effect = Conflict("already exists")
        client = self._inject_mock_client(manager)
        client.query.return_value = query_job

        with self.assertRaisesRegex(DatasetBuilderError, r"Search index already exists"):
            manager._create_search_indexes(builder, DatasetType.PUBLIC)

        exists_ok_manager = self._make_manager(exists_ok=True)

        exists_ok_client = Mock()
        exists_ok_query_job = Mock()
        exists_ok_query_job.result.side_effect = Conflict("already exists")
        exists_ok_client.query.return_value = exists_ok_query_job
        exists_ok_manager._client = exists_ok_client

        exists_ok_manager._create_search_indexes(builder, DatasetType.PUBLIC)


class DatasetBuildManagerTestCase(DatasetBuilderTestMixin, unittest.TestCase):
    """Dataset build manager behavior tests."""

    def test_raises_on_client_init_failure(self) -> None:
        """Test that BigQuery client initialization errors are wrapped in
        DatasetBuilderError.
        """
        with patch(
            self.BIGQUERY_CLIENT_PATCH,
            side_effect=RuntimeError("Client initialization failed"),
        ):
            with self.assertRaisesRegex(DatasetBuilderError, r"Failed to initialize BigQuery client"):
                DatasetBuildManager(config=self.config, schema=self.schema)

    def test_create_datasets_creates_each_type(self) -> None:
        """Test that _create_datasets creates one dataset per dataset type."""
        manager = self._make_manager()
        client = self._inject_mock_client(manager)

        manager._create_datasets()

        created_fqns = [
            f"{call.args[0].project}.{call.args[0].dataset_id}"
            for call in client.create_dataset.call_args_list
        ]
        expected_fqns = [self.config.fqn_for(dataset_type) for dataset_type in DatasetType]
        self.assertCountEqual(created_fqns, expected_fqns)

    def test_create_datasets_conflict_without_exists_ok_raises(self) -> None:
        """Test that a dataset Conflict without exists_ok is wrapped as
        DatasetBuilderError.
        """
        manager = self._make_manager()
        client = self._inject_mock_client(manager)
        client.create_dataset.side_effect = Conflict("already exists")

        with self.assertRaisesRegex(DatasetBuilderError, r"already exists"):
            manager._create_datasets()

    def test_create_datasets_conflict_with_exists_ok_continues(self) -> None:
        """Test that _create_datasets tolerates conflicts when exists_ok."""
        manager = self._make_manager(exists_ok=True)
        client = self._inject_mock_client(manager)
        client.create_dataset.side_effect = Conflict("already exists")

        manager._create_datasets()

        # Creation should be attempted once per dataset type despite conflicts.
        self.assertEqual(client.create_dataset.call_count, len(DatasetType))

    def test_wraps_conflict_without_exists_ok(self) -> None:
        """Test that Conflict without exists_ok is wrapped as
        DatasetBuilderError.
        """
        table = bigquery.Table(f"{self.config.project_id}.{self.config.datasets.public}.DiaObject")

        manager = self._make_manager()
        client = self._inject_mock_client(manager)
        client.create_table.side_effect = Conflict("already exists")
        with self.assertRaisesRegex(DatasetBuilderError, r"already exists"):
            manager._create_resources([table], "table")

    def test_forwards_exists_ok_flag_to_create_table(self) -> None:
        """Test that exists_ok=True is forwarded to create_table."""
        table = bigquery.Table(f"{self.config.project_id}.{self.config.datasets.public}.DiaObject")

        exists_ok_manager = self._make_manager(exists_ok=True)
        exists_ok_client = self._inject_mock_client(exists_ok_manager)
        exists_ok_manager._create_resources([table], "table")
        exists_ok_client.create_table.assert_called_once_with(table, exists_ok=True)

    def test_wraps_non_builder_error_from_build_loop(self) -> None:
        """Test that build_datasets wraps unexpected build-loop failures."""
        manager = self._make_manager()

        public_builder = Mock(spec=DatasetBuilder)
        public_builder.build_tables.side_effect = RuntimeError("boom")
        manager._builders = {DatasetType.PUBLIC: public_builder}

        with self.assertRaisesRegex(DatasetBuilderError, r"Failed to build datasets: boom"):
            manager.build_datasets()

    def test_authorized_view_error_handling(self) -> None:
        """Test that build_datasets wraps unexpected errors from authorized
        view configuration and re-raises DatasetBuilderError unchanged.
        """
        public_view = self._make_public_views()[0]
        public_builder = self._make_mock_builder()
        public_builder.build_views.return_value = [public_view]

        manager = self._make_manager(
            configure_authorized_views=True,
        )
        manager._builders = {DatasetType.PUBLIC: public_builder}

        # Non-DatasetBuilderError exceptions are wrapped.
        with patch.object(manager, "_configure_authorized_views", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(DatasetBuilderError, r"Failed to configure authorized views: boom"):
                manager.build_datasets()

        # DatasetBuilderError is re-raised unchanged.
        manager2 = self._make_manager(
            configure_authorized_views=True,
        )
        manager2._builders = {DatasetType.PUBLIC: public_builder}

        with patch.object(
            manager2,
            "_configure_authorized_views",
            side_effect=DatasetBuilderError("authorized view failure"),
        ):
            with self.assertRaisesRegex(DatasetBuilderError, r"authorized view failure"):
                manager2.build_datasets()


class AuthorizedViewsTestCase(DatasetBuilderTestMixin, unittest.TestCase):
    """Authorized views configuration tests."""

    def test_preserves_unmanaged_view_permissions(self) -> None:
        """Test that _configure_authorized_views preserves unmanaged
        permissions while updating managed entries.
        """
        manager = self._make_manager(configure_authorized_views=True)

        # Set up a mix of access entries to test preservation of non-view and
        # unmanaged view permissions, as well as proper handling of managed
        # entries.
        non_view_entry = bigquery.AccessEntry(
            role="READER",
            entity_type="userByEmail",
            entity_id="reader@example.org",
        )
        managed_existing_entry = bigquery.AccessEntry(
            role=None,
            entity_type="view",
            entity_id={
                "projectId": self.config.project_id,
                "datasetId": self.config.datasets.public,
                "tableId": ApdbTables.DiaSource.value,
            },
        )
        stale_managed_entry = bigquery.AccessEntry(
            role=None,
            entity_type="view",
            entity_id={
                "projectId": self.config.project_id,
                "datasetId": self.config.datasets.public,
                "tableId": "OldManagedView",
            },
        )
        unrelated_view_entry = bigquery.AccessEntry(
            role=None,
            entity_type="view",
            entity_id={
                "projectId": "unrelated-project",
                "datasetId": "unrelated_dataset",
                "tableId": "UnrelatedView",
            },
        )

        dataset = Mock()
        dataset.access_entries = [
            non_view_entry,
            managed_existing_entry,
            stale_managed_entry,
            unrelated_view_entry,
        ]

        client = self._inject_mock_client(manager)
        client.get_dataset.return_value = dataset

        public_views = [
            self._make_fqn_table(DatasetType.PUBLIC, ApdbTables.DiaSource.value),
            self._make_fqn_table(DatasetType.PUBLIC, ApdbTables.DiaForcedSource.value),
        ]

        manager._configure_authorized_views(public_views)

        # Access entries should be persisted as a single dataset update.
        client.update_dataset.assert_called_once_with(dataset, ["access_entries"])

        updated_entries = dataset.access_entries

        # Validate the complete managed/unmanaged view reference set.
        updated_view_refs = {
            (
                entry.entity_id["projectId"],
                entry.entity_id["datasetId"],
                entry.entity_id["tableId"],
            )
            for entry in updated_entries
            if entry.entity_type == "view" and isinstance(entry.entity_id, dict)
        }
        self.assertEqual(
            updated_view_refs,
            {
                (
                    self.config.project_id,
                    self.config.datasets.public,
                    ApdbTables.DiaSource.value,
                ),
                (
                    self.config.project_id,
                    self.config.datasets.public,
                    ApdbTables.DiaForcedSource.value,
                ),
                (
                    self.config.project_id,
                    self.config.datasets.public,
                    "OldManagedView",
                ),
                (
                    "unrelated-project",
                    "unrelated_dataset",
                    "UnrelatedView",
                ),
            },
        )

    def test_skips_api_calls_for_empty_views_list(self) -> None:
        """Test that _configure_authorized_views skips BigQuery API calls
        when the views list is empty.
        """
        manager = self._make_manager(configure_authorized_views=True)
        client = self._inject_mock_client(manager)
        manager._configure_authorized_views([])
        client.get_dataset.assert_not_called()
        client.update_dataset.assert_not_called()


@unittest.skipIf(not have_valid_google_credentials(), "Missing valid Google credentials")
class DatasetBuilderBigQueryTestCase(unittest.TestCase):
    """Integration tests for creating BigQuery tables and views."""

    def _make_integration_manager(self, exists_ok: bool = False) -> DatasetBuildManager:
        """Create a DatasetBuildManager configured for integration tests."""
        manager = DatasetBuildManager(
            config=self.config,
            schema=self.schema,
            exists_ok=exists_ok,
        )
        if not _SEARCH_INDEXES_ENABLED:
            for builder in manager._builders.values():
                builder.build_search_indexes = Mock(return_value=[])
        return manager

    def _list_search_indexes(self, dataset_name: str) -> list[tuple[str, str]]:
        """List search indexes as (table_name, index_name) pairs."""
        query = (
            f"SELECT table_name, index_name "
            f"FROM `{self.project_id}.{dataset_name}.INFORMATION_SCHEMA.SEARCH_INDEXES`"
        )
        rows = self.client.query(query).result()
        return [(row.table_name, row.index_name) for row in rows]

    def setUp(self) -> None:
        self.schema = DatasetBuilderTestMixin._load_test_schema()
        self.client = bigquery.Client()
        self.project_id = self.client.project

        # Create configuration with unique dataset names for testing.
        suffix = uuid.uuid4().hex[:8]
        self.datasets = Datasets(
            internal=f"test_dataset_builder_internal_{suffix}",
            public=f"test_dataset_builder_public_{suffix}",
            staging=f"test_dataset_builder_staging_{suffix}",
        )
        self.config = DatasetBuilderTestMixin._make_test_config(
            project_id=self.project_id,
            datasets=self.datasets,
        )

    def tearDown(self) -> None:
        for dataset_type in DatasetType:
            dataset_name = self.datasets.name_for(dataset_type)
            dataset_fqn = f"{self.project_id}.{dataset_name}"
            try:
                self.client.delete_dataset(dataset_fqn, delete_contents=True, not_found_ok=True)
            except Exception as e:
                print(f"Failed to delete {dataset_fqn}: {type(e).__name__}: {e}", file=sys.stderr)

    def test_build_datasets_creates_expected_bigquery_objects(self) -> None:
        """Test that build_datasets creates expected tables and views.

        Integration test verifying that tables are created with correct
        fields and clustering, and views have correct columns and FROM
        clauses.
        """
        manager = self._make_integration_manager()
        manager.build_datasets()

        # The datasets themselves should have been created by build_datasets.
        for dataset_type in DatasetType:
            dataset_name = self.datasets.name_for(dataset_type)
            self.client.get_dataset(f"{self.project_id}.{dataset_name}")

        internal_tables = list(self.client.list_tables(f"{self.project_id}.{self.datasets.internal}"))
        public_tables = list(self.client.list_tables(f"{self.project_id}.{self.datasets.public}"))
        staging_tables = list(self.client.list_tables(f"{self.project_id}.{self.datasets.staging}"))

        self.assertEqual(len(internal_tables), 3)
        self.assertEqual(len(public_tables), 3)
        self.assertEqual(len(staging_tables), 3)

        # Public dataset should have a properly structured DiaObject table.
        public_dia_object = self.client.get_table(
            f"{self.project_id}.{self.datasets.public}.{ApdbTables.DiaObject.value}"
        )
        self.assertEqual(public_dia_object.table_type, "TABLE")
        public_dia_object_field_names = [field.name for field in public_dia_object.schema]
        self.assertNotIn("validityEndMjdTai", public_dia_object_field_names)
        self.assertIn("geo_point", public_dia_object_field_names)

        # Public dataset should have views with fully qualified columns and
        # correct FROM clauses.
        for table in (ApdbTables.DiaSource, ApdbTables.DiaForcedSource):
            public_view = self.client.get_table(f"{self.project_id}.{self.datasets.public}.{table.value}")
            self.assertEqual(public_view.table_type, "VIEW")
            assert public_view.view_query is not None
            self.assertIn(
                f"FROM `{self.project_id}.{self.datasets.internal}.{table.value}`",
                public_view.view_query,
            )

        # Staging dataset should have all APDB tables with apdb_replica_chunk
        # added.
        for table in (ApdbTables.DiaObject, ApdbTables.DiaSource, ApdbTables.DiaForcedSource):
            staging_table = self.client.get_table(f"{self.project_id}.{self.datasets.staging}.{table.value}")
            self.assertEqual(staging_table.table_type, "TABLE")
            chunk_field = next(field for field in staging_table.schema if field.name == "apdb_replica_chunk")
            self.assertEqual(chunk_field.field_type, "INTEGER")
            self.assertEqual(chunk_field.mode, "REQUIRED")

        # Internal dataset should have all APDB tables with geo_point and
        # geo_point clustering.
        for table in (ApdbTables.DiaObject, ApdbTables.DiaSource, ApdbTables.DiaForcedSource):
            internal_table = self.client.get_table(
                f"{self.project_id}.{self.datasets.internal}.{table.value}"
            )
            self.assertEqual(internal_table.table_type, "TABLE")
            geo_point = next(field for field in internal_table.schema if field.name == "geo_point")
            self.assertEqual(geo_point.field_type, "GEOGRAPHY")
            self.assertEqual(geo_point.mode, "REQUIRED")
            self.assertEqual(internal_table.clustering_fields, ["geo_point"])

    def test_build_datasets_exists_ok_allows_repeated_creation(self) -> None:
        """Test that build_datasets allows repeated calls with exists_ok.

        Integration test verifying that repeated calls to build_datasets
        succeed when exists_ok=True and datasets already exist.
        """
        manager = self._make_integration_manager(exists_ok=True)
        manager.build_datasets()
        manager.build_datasets()

    def test_build_datasets_without_exists_ok_fails_on_repeated_creation(self) -> None:
        """Test that build_datasets fails without exists_ok on repeat.

        Integration test verifying that calling build_datasets twice without
        exists_ok=True raises DatasetBuilderError on the second call.
        """
        manager = self._make_integration_manager()

        manager.build_datasets()

        with self.assertRaisesRegex(DatasetBuilderError, r"already exists"):
            manager.build_datasets()

    @unittest.skipUnless(
        _SEARCH_INDEXES_ENABLED,
        "Set DAX_PPDB_TESTS_SEARCH_INDEXES_ENABLED=1 to run search index integration tests",
    )
    def test_build_datasets_creates_search_indexes(self) -> None:
        """Test that build_datasets creates search indexes."""
        manager = self._make_integration_manager()
        manager.build_datasets()

        internal_indexes = self._list_search_indexes(self.datasets.internal)
        public_indexes = self._list_search_indexes(self.datasets.public)

        self.assertGreaterEqual(len(internal_indexes), 3)
        self.assertGreaterEqual(len(public_indexes), 1)


if __name__ == "__main__":
    unittest.main()
