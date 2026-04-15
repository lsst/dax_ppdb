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

__all__ = ["create_bigquery"]

import logging

import yaml

from ..bigquery import PpdbBigQuery

_LOG = logging.getLogger(__name__)


def create_bigquery(
    output_config: str,
    db_url: str,
    project_id: str,
    dataset_id: str,
    bucket_name: str,
    object_prefix: str,
    replication_dir: str,
    db_drop: bool,
    *,
    db_schema: str | None = None,
    felis_path: str | None = None,
    felis_schema: str | None = None,  # TODO: Remove this eventually (DM-52584)
    stage_chunk_topic: str | None = None,
    parq_batch_size: int | None = None,
    parq_compression: str | None = None,
    delete_existing_dirs: bool = False,
    validate_config: bool = True,
) -> None:
    """Create new BigQuery configuration and initialize SQL database.

    Parameters
    ----------
    output_config
        Name of the new configuration file for created BigQuery PPDB instance.
    db_url
        Database URL in SQLAlchemy format for PPDB instance.
    project_id
        GCP project ID.
    dataset_id
        BigQuery dataset name, e.g., 'my_project:my_dataset'.
    bucket_name
        GCS bucket name to use for Parquet output.
    object_prefix
        Object prefix to use in GCS bucket for Parquet output.
    replication_dir
        Directory used for replication staging area.
    db_drop
        If True then drop existing db tables.
    db_schema
        Database schema name for PPDB instance.
    felis_path
        Path to Felis database. If `None`, defaults to the default path in SDM
        Schemas.
    felis_schema
        Felis schema name within the YAML file.
    stage_chunk_topic
        Pub/Sub topic to use for staging chunks.
    parq_batch_size
        Number of rows to use when batching Parquet output.
    parq_compression
        Compression codec to use for Parquet output.
    delete_existing_dirs
        If True then delete existing replication staging directories.
    validate_config
        If `True`, validate the configuration against GCP resources.
    """
    bq_config = PpdbBigQuery.init_bigquery(
        db_url=db_url,
        db_schema=db_schema,
        db_drop=db_drop,
        felis_path=felis_path,
        felis_schema=felis_schema,
        replication_dir=replication_dir,
        delete_existing_dirs=delete_existing_dirs,
        stage_chunk_topic=stage_chunk_topic,
        parq_batch_size=parq_batch_size,
        parq_compression=parq_compression,
        bucket_name=bucket_name,
        object_prefix=object_prefix,
        project_id=project_id,
        dataset_id=dataset_id,
        validate_config=validate_config,
    )
    _LOG.info("Created BigQuery configuration: %s", bq_config)

    config_dict = bq_config.model_dump(exclude_unset=True, exclude_defaults=True)
    config_dict["implementation_type"] = "bigquery"
    with open(output_config, "w") as config_file:
        yaml.dump(config_dict, config_file)
