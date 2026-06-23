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

from felis import Schema

from ..bigquery.ppdb_bigquery_config import PpdbBigQueryConfig
from ..bigquery.schema import DatasetBuildManager
from ..ppdb_config import PpdbConfig


def create_datasets(
    config: str,
    exists_ok: bool = False,
    configure_authorized_views: bool = False,
    disable_search_indexes: bool = False,
) -> None:
    """Create all BigQuery datasets for the PPDB and populate them with
    tables and views derived from a Felis schema.
    """
    # Load the PPDB config.
    try:
        ppdb_config = PpdbConfig.from_uri(config)
    except Exception as e:
        raise RuntimeError(f"Failed to load configuration from {config}: {e}") from e

    # Check the config type.
    if not isinstance(ppdb_config, PpdbBigQueryConfig):
        raise TypeError(
            f"Configuration loaded from '{config}' has wrong type, "
            f"expected PpdbBigQueryConfig, got: {type(ppdb_config)}"
        )

    # Load the Felis schema from the URI in the config.
    try:
        schema = Schema.from_uri(ppdb_config.felis_schema_uri, context={"id_generation": True})
    except Exception as e:
        raise RuntimeError(f"Failed to load schema from {ppdb_config.felis_schema_uri}: {e}") from e

    # Build all datasets.
    build_manager = DatasetBuildManager(
        ppdb_config,
        schema,
        exists_ok=exists_ok,
        configure_authorized_views=configure_authorized_views,
        create_search_indexes=not disable_search_indexes,
    )
    build_manager.build_datasets()
