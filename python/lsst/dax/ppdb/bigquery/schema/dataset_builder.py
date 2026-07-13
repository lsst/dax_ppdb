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

__all__ = [
    "BaseDatasetBuilder",
    "DatasetBuildManager",
    "DatasetBuilder",
    "DatasetBuilderError",
    "InternalDatasetBuilder",
    "PublicDatasetBuilder",
    "SearchIndexDataType",
    "SearchIndexDefinition",
    "StagingDatasetBuilder",
]

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from enum import Enum
from types import MappingProxyType
from typing import ClassVar

from felis import Schema
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from pydantic import BaseModel

from lsst.dax.apdb import ApdbTables

from ..ppdb_bigquery_config import DatasetType, PpdbBigQueryConfig
from .felis_converter import FelisConverter

_LOG = logging.getLogger(__name__)

_DIA_TABLES: tuple[str, ...] = (
    ApdbTables.DiaObject.value,
    ApdbTables.DiaSource.value,
    ApdbTables.DiaForcedSource.value,
)


def _update_schema_fields(table: bigquery.Table, *new_fields: bigquery.SchemaField) -> None:
    """Add new fields to the schema of a BigQuery table.

    Raises
    ------
    ValueError
        Raised if any of the new fields already exist in the table schema.
    """
    existing_field_names = {field.name for field in table.schema}
    duplicate_field_names = sorted({field.name for field in new_fields if field.name in existing_field_names})
    if duplicate_field_names:
        raise ValueError(
            f"Cannot add fields to table {table.table_id}: "
            f"the following fields already exist in the schema: {', '.join(duplicate_field_names)}."
        )
    schema_fields = list(table.schema)
    schema_fields.extend(new_fields)
    table.schema = schema_fields


def _geo_point_field() -> bigquery.SchemaField:
    """Create a BigQuery schema field for an internal geography column used for
    spatial query optimization.
    """
    return bigquery.SchemaField(
        "geo_point",
        "GEOGRAPHY",
        mode="REQUIRED",
        description="Internal geography column for optimization of spatial queries.",
    )


class DatasetBuilderError(RuntimeError):
    """Raised when dataset builder operations fail."""


class SearchIndexDataType(Enum):
    """Supported BigQuery search-index data types."""

    STRING = "STRING"
    INT64 = "INT64"
    TIMESTAMP = "TIMESTAMP"


class SearchIndexDefinition(BaseModel, frozen=True):
    """Definition for a search index to create on a BigQuery table."""

    table_name: str
    """Name of the table on which to create the search index."""

    field_data_types: tuple[tuple[str, SearchIndexDataType], ...]
    """Tuple of field names and their corresponding data types to include in
    the search index, e.g., (("field1", SearchIndexDataType.STRING),
    ("field2", SearchIndexDataType.INT64)).
    """

    @property
    def index_name(self) -> str:
        """The BigQuery search index name for this definition."""
        index_fields = "_".join(field_name for field_name, _ in self.field_data_types)
        return f"{self.table_name}_{index_fields}_idx"


class DatasetBuilder(ABC):
    """Build the BigQuery objects for a specific type of dataset.

    These objects will be used by the `DatasetBuildManager` to create the
    datasets using the BigQuery client.

    Parameters
    ----------
    config
        The PPDB BigQuery configuration containing dataset names and other
        settings.
    converter
        The FelisConverter instance to use for converting Felis schema objects
        to BigQuery table definitions.
    """

    dataset_type: ClassVar[DatasetType]
    """Dataset type this builder handles."""

    def __init__(self, config: PpdbBigQueryConfig, converter: FelisConverter) -> None:
        self._config = config
        self._converter = converter

    @abstractmethod
    def build_tables(self) -> list[bigquery.Table]:
        """Build BigQuery table objects for a dataset type.

        Returns
        -------
        `list` [`google.cloud.bigquery.Table`]
            BigQuery table objects to create for the dataset.
        """
        raise NotImplementedError("build_tables() must be implemented by subclasses of DatasetBuilder.")

    @abstractmethod
    def build_views(self) -> list[bigquery.Table]:
        """Build BigQuery views for a dataset type.

        Returns
        -------
        `list` [`google.cloud.bigquery.Table`]
            BigQuery view objects to create for the dataset.
        """
        raise NotImplementedError("build_views() must be implemented by subclasses of DatasetBuilder.")

    @abstractmethod
    def build_search_indexes(self) -> list[SearchIndexDefinition]:
        """Build search indexes for a dataset type.

        Returns
        -------
        list[`SearchIndexDefinition`]
            Search index definitions to create for the dataset.
        """
        raise NotImplementedError(
            "build_search_indexes() must be implemented by subclasses of DatasetBuilder."
        )


class BaseDatasetBuilder(DatasetBuilder):
    """Base implementation of DatasetBuilder with no-op optional methods."""

    def build_tables(self) -> list[bigquery.Table]:
        return []

    def build_views(self) -> list[bigquery.Table]:
        return []

    def build_search_indexes(self) -> list[SearchIndexDefinition]:
        return []


class StagingDatasetBuilder(BaseDatasetBuilder):
    """Builder for the staging dataset type."""

    dataset_type = DatasetType.STAGING

    def build_tables(self) -> list[bigquery.Table]:
        """Create BigQuery tables for the staging dataset type."""
        # Get the base DIA table definitions.
        tables = self._converter.convert_tables(
            _DIA_TABLES,
            dataset_fqn=self._config.fqn_for(self.dataset_type),
        )

        # Add the apdb_replica_chunk field to each staging table.
        for table in tables:
            apdb_replica_chunk_field = bigquery.SchemaField(
                "apdb_replica_chunk",
                "INT64",
                mode="REQUIRED",
                description="APDB replica chunk this row belongs to.",
            )
            _update_schema_fields(table, apdb_replica_chunk_field)

        # Add the table which will hold the raw update records.
        updates_table = bigquery.Table(
            self._config.fqn_for(self.dataset_type, "updates"),
            schema=[
                bigquery.SchemaField("apdb_replica_chunk", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("update_time_ns", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("update_order", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("json_payload", "STRING", mode="REQUIRED"),
            ],
        )
        tables.append(updates_table)

        return tables


class InternalDatasetBuilder(BaseDatasetBuilder):
    """Builder for the internal dataset type."""

    dataset_type = DatasetType.INTERNAL

    def build_tables(self) -> list[bigquery.Table]:
        """Create BigQuery tables for the internal dataset type."""
        # Convert the base DIA tables.
        tables = self._converter.convert_tables(
            _DIA_TABLES,
            dataset_fqn=self._config.fqn_for(self.dataset_type),
        )

        # Add an internal geography column for spatial query optimization to
        # each internal table and set clustering on that column.
        for table in tables:
            geo_point_field = _geo_point_field()
            _update_schema_fields(table, geo_point_field)
            table.clustering_fields = [geo_point_field.name]

        return tables

    def build_search_indexes(self) -> list[SearchIndexDefinition]:
        """Create search indexes for the internal dataset type."""
        return [
            SearchIndexDefinition(
                table_name=table_name,
                field_data_types=(("diaObjectId", SearchIndexDataType.INT64),),
            )
            for table_name in _DIA_TABLES
        ]


class PromotionDatasetBuilder(BaseDatasetBuilder):
    """Builder for the promotion dataset type.

    This dataset type is used as a workspace for the promotion process. It does
    not currently define any tables or views but uses copies of tables from
    other datasets.
    """

    dataset_type = DatasetType.PROMOTION


class PublicDatasetBuilder(BaseDatasetBuilder):
    """Builder for the public dataset type."""

    dataset_type = DatasetType.PUBLIC

    def _create_explicit_view(self, table_name: str) -> bigquery.Table:
        """Create an explicit view selecting all columns from the corresponding
        internal table.

        Parameters
        ----------
        table_name
            Name of the source table and resulting view.
        """
        public_dataset_fqn = self._config.fqn_for(self.dataset_type)
        internal_dataset_fqn = self._config.fqn_for(DatasetType.INTERNAL)
        column_names = [column.name for column in self._converter.find_table(table_name).columns]
        column_list = ", ".join(f"`{column_name}`" for column_name in column_names)

        view = bigquery.Table(f"{public_dataset_fqn}.{table_name}")
        view.view_query = f"SELECT {column_list} FROM `{internal_dataset_fqn}.{table_name}`"
        return view

    def build_tables(self) -> list[bigquery.Table]:
        """Create BigQuery tables for the public dataset type.

        Notes
        -----
        DiaObject is defined using a table rather than a view in this dataset,
        because using ``validityEndMjdTai IS NULL`` as a filter in a plain view
        definition would be inefficient. A materialized view would also be
        cumbersome, as its references would be invalidated by table swap
        operations during the promotion process.
        """
        (dia_object_table,) = self._converter.convert_tables(
            [ApdbTables.DiaObject.value],
            dataset_fqn=self._config.fqn_for(self.dataset_type),
        )

        # Omit the validityEndMjdTai column from the table definition, as it
        # will always be null.
        if not any(field.name == "validityEndMjdTai" for field in dia_object_table.schema):
            raise DatasetBuilderError(
                f"Expected column validityEndMjdTai not found in table {dia_object_table.table_id}."
            )
        dia_object_table.schema = [
            field for field in dia_object_table.schema if field.name != "validityEndMjdTai"
        ]

        # Add geography column and clustering for spatial query optimization.
        geo_point_field = _geo_point_field()
        _update_schema_fields(dia_object_table, geo_point_field)
        dia_object_table.clustering_fields = [geo_point_field.name]

        return [dia_object_table]

    def build_views(self) -> list[bigquery.Table]:
        """Create BigQuery views for the public dataset type."""
        return [
            self._create_explicit_view(table_name)
            for table_name in (ApdbTables.DiaSource.value, ApdbTables.DiaForcedSource.value)
        ]

    def build_search_indexes(self) -> list[SearchIndexDefinition]:
        """Create search indexes for the public dataset type."""
        return [
            SearchIndexDefinition(
                table_name=ApdbTables.DiaObject.value,
                field_data_types=(("diaObjectId", SearchIndexDataType.INT64),),
            )
        ]


class DatasetBuildManager:
    """Manage dataset builders and dispatch build operations by dataset type.

    Parameters
    ----------
    config
        The PPDB BigQuery configuration containing dataset names and other
        settings.
    schema
        The Felis schema object containing table definitions.
    exists_ok
        If True, do not fail if a BigQuery table or view already exists; skip
        creating it.
    configure_authorized_views
        If True, configure internal dataset access entries for public
        authorized views after build.
    create_search_indexes
        If True, create search indexes for each dataset. If False, skip search
        index creation entirely.
    """

    _BUILDER_TYPES: Mapping[DatasetType, type[DatasetBuilder]] = MappingProxyType(
        {
            StagingDatasetBuilder.dataset_type: StagingDatasetBuilder,
            InternalDatasetBuilder.dataset_type: InternalDatasetBuilder,
            PromotionDatasetBuilder.dataset_type: PromotionDatasetBuilder,
            PublicDatasetBuilder.dataset_type: PublicDatasetBuilder,
        }
    )

    def __init__(
        self,
        config: PpdbBigQueryConfig,
        schema: Schema,
        exists_ok: bool = False,
        configure_authorized_views: bool = False,
        create_search_indexes: bool = True,
    ) -> None:
        self._config = config
        self._schema = schema
        self._exists_ok = exists_ok
        self._configure_authorized_views_enabled = configure_authorized_views
        self._create_search_indexes_enabled = create_search_indexes
        self._converter = FelisConverter(schema=self._schema)

        # Initialize the builders for all supported dataset types.
        self._builders: dict[DatasetType, DatasetBuilder] = {
            dataset_type: builder_type(config=self._config, converter=self._converter)
            for dataset_type, builder_type in self._BUILDER_TYPES.items()
        }

        # Initialize the BigQuery client.
        try:
            self._client = bigquery.Client(project=self._config.project_id)
        except Exception as e:
            raise DatasetBuilderError(f"Failed to initialize BigQuery client: {e}") from e

    def build_datasets(self) -> None:
        """Create the BigQuery datasets and populate them with the objects
        produced by the builders.

        Raises
        ------
        DatasetBuilderError
            Raised if any part of the build process fails, including dataset
            creation, table/view creation, search index creation, or authorized
            view configuration.
        """
        public_views: list[bigquery.Table] = []

        try:
            # Create the datasets themselves before populating them.
            self._create_datasets()

            for dataset_type, builder in self._builders.items():
                # Build the tables for this dataset type and then create them
                # in BigQuery.
                tables = builder.build_tables()
                self._create_resources(
                    resources=tables,
                    resource_kind="table",
                )

                # Build the views for this dataset type and then create them
                # in BigQuery.
                views = builder.build_views()
                self._create_resources(
                    resources=views,
                    resource_kind="view",
                )
                if dataset_type is DatasetType.PUBLIC:
                    public_views.extend(views)

                # Build the search index definitions for this dataset type and
                # then create them in BigQuery.
                if self._create_search_indexes_enabled:
                    self._create_search_indexes(builder=builder, dataset_type=dataset_type)
        except DatasetBuilderError:
            raise
        except Exception as e:
            raise DatasetBuilderError(f"Failed to build datasets: {e}") from e

        # After all datasets are built, optionally configure authorized
        # view entries for the public views on the internal dataset.
        try:
            if self._configure_authorized_views_enabled:
                self._configure_authorized_views(public_views)
        except DatasetBuilderError:
            raise
        except Exception as e:
            raise DatasetBuilderError(f"Failed to configure authorized views: {e}") from e

    def _create_datasets(self) -> None:
        """Create the BigQuery datasets for all supported dataset types.

        Raises
        ------
        DatasetBuilderError
            Raised if a dataset already exists and ``exists_ok`` is not set.
        """
        for dataset_type in self._builders:
            dataset_fqn = self._config.fqn_for(dataset_type)

            _LOG.debug("Creating dataset: %s", dataset_fqn)
            try:
                self._client.create_dataset(bigquery.Dataset(dataset_fqn))
            except Conflict as e:
                if self._exists_ok:
                    _LOG.info("Dataset %s already exists and exists_ok=True; skipping.", dataset_fqn)
                    continue
                raise DatasetBuilderError(f"Dataset {dataset_fqn!r} already exists.") from e
            _LOG.debug("Created dataset: %s", dataset_fqn)

    def _create_resources(
        self,
        resources: list[bigquery.Table],
        resource_kind: str,
    ) -> None:
        """Create BigQuery resources.

        Parameters
        ----------
        resources
            List of BigQuery table or view objects to create.
        resource_kind
            String label for the kind of resource being created, used in log
            messages.
        """
        if not resources:
            _LOG.debug("No %ss to create", resource_kind)
            return

        for resource in resources:
            _LOG.debug("Creating %s: %s", resource_kind, resource.to_api_repr())
            try:
                self._client.create_table(resource, exists_ok=self._exists_ok)
            except Conflict as e:
                raise DatasetBuilderError(
                    f"{resource_kind.capitalize()} {resource.table_id!r} already exists."
                ) from e
            _LOG.debug("Created %s: %s", resource_kind, resource.table_id)

    def _create_search_indexes(
        self,
        builder: DatasetBuilder,
        dataset_type: DatasetType,
    ) -> None:
        """Create search indexes for a dataset type."""
        dataset_fqn = self._config.fqn_for(dataset_type)
        for definition in builder.build_search_indexes():
            field_names = ", ".join(field_name for field_name, _ in definition.field_data_types)
            data_types = ", ".join(f"'{data_type.value}'" for _, data_type in definition.field_data_types)
            try:
                self._client.query(
                    f"""CREATE SEARCH INDEX {definition.index_name}
                    ON `{dataset_fqn}.{definition.table_name}`({field_names})
                    OPTIONS (data_types = [{data_types}]);
                    """
                ).result()  # Wait for the operation to complete.
            except Conflict as e:
                if self._exists_ok:
                    _LOG.info(
                        "Search index already exists for %s and exists_ok=True; skipping.",
                        dataset_type.value,
                    )
                    continue
                raise DatasetBuilderError("Search index already exists.") from e

    def _configure_authorized_views(self, public_views: list[bigquery.Table]) -> None:
        """Configure authorized view access entries on the internal dataset for
        the public views.

        Parameters
        ----------
        public_views
            List of public view objects to configure as authorized views on the
            internal dataset.

        Notes
        -----
        The complexity of this method is due to the fact that we need to
        preserve any existing entries on the internal dataset which are not
        managed by this tool, while replacing any existing entries for managed
        views. The list cannot be appended in place, so we need to build a new,
        complete list of access entries and then update the dataset with it.
        """
        if not public_views:
            _LOG.warning("No public views found for authorized-view configuration")
            return

        internal_dataset_fqn = self._config.fqn_for(DatasetType.INTERNAL)
        internal_dataset = self._client.get_dataset(internal_dataset_fqn)

        # Build a set of public view references managed by this tool.
        managed_view_refs = {(view.project, view.dataset_id, view.table_id) for view in public_views}

        # Keep any entries that are not managed by this tool.
        retained_entries: list[bigquery.AccessEntry] = []
        for entry in internal_dataset.access_entries:
            # Not a view entry, so keep it.
            if entry.entity_type != "view":
                retained_entries.append(entry)
                continue

            # Get the view reference for this entry.
            existing_view_ref = (
                entry.entity_id.get("projectId"),
                entry.entity_id.get("datasetId"),
                entry.entity_id.get("tableId"),
            )
            # Skip managed views; we'll rebuild them below.
            if existing_view_ref in managed_view_refs:
                continue
            # Keep unrelated/unmanaged views.
            retained_entries.append(entry)

        # Rebuild managed entries from the current set of public views.
        managed_view_entries: list[bigquery.AccessEntry] = []
        for project_id, dataset_id, table_id in sorted(managed_view_refs):
            managed_view_entries.append(
                bigquery.AccessEntry(
                    role=None,
                    entity_type="view",
                    entity_id={
                        "projectId": project_id,
                        "datasetId": dataset_id,
                        "tableId": table_id,
                    },
                )
            )

        # Combine the retained and managed entries and update the dataset.
        internal_dataset.access_entries = retained_entries + managed_view_entries
        self._client.update_dataset(internal_dataset, ["access_entries"])
        _LOG.debug(
            "Configured %d authorized PPDB view entries on dataset %s",
            len(managed_view_entries),
            internal_dataset_fqn,
        )
