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

import json
import logging
import posixpath
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

import google.auth
import yaml
from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import GoogleAuthError
from google.cloud import pubsub_v1
from google.cloud.storage import Bucket, Client
from pydantic import BaseModel

from lsst.resources import ResourcePath, ResourcePathExpression

from .ppdb_bigquery_config import Datasets
from .schema.constants import SSO_TABLES

__all__ = ["SSOUploadError", "SSOUploader", "SSOUploaderConfig"]

_LOG = logging.getLogger(__name__)


class SSOUploadError(RuntimeError):
    """Raised when uploading an SSO parquet file to Google Cloud Storage
    fails.
    """


class SSOUploaderConfig(BaseModel):
    """Configuration for the SSOUploader class."""

    bucket_name: str
    """Target bucket for uploading the SSO parquet files."""

    object_prefix: str = "sso"
    """Prefix for the uploaded objects in the bucket."""

    pubsub_topic: str | None = "load-sso-topic"
    """Pub/Sub topic to publish a message to after successful upload."""

    project_id: str | None = None
    """Google Cloud project ID. If not provided, the project ID from the
    default credentials will be used.
    """

    dataset_id: str = Datasets().internal
    """BigQuery dataset ID associated with the uploaded SSO files."""

    allow_partial_upload: bool = False
    """Whether to allow partial uploads even if some SSO tables are missing."""

    append_unique_prefix: bool = True
    """Whether to append a unique, time-based path segment to object_prefix
    for each upload, preventing silent overwrite of a previous run's data.
    """

    @classmethod
    def from_uri(cls, uri: ResourcePathExpression) -> Self:
        """Load configuration from a URI.

        Parameters
        ----------
        uri
            URI of the YAML configuration file.

        Returns
        -------
        Self
            An instance of the configuration class populated with values from
            the YAML file.
        """
        config_data = yaml.safe_load(ResourcePath(uri).read())
        return cls(**config_data)


class SSOUploader:
    """Class to upload SSO data to Google Cloud Storage for ingest into
    BigQuery.

    Parameters
    ----------
    config
        Configuration for the SSO uploader.
    file_map
        A mapping of SSO table names to the corresponding parquet file paths.
    """

    def __init__(
        self,
        config: ResourcePathExpression | SSOUploaderConfig,
        file_map: Mapping[str, Path],
    ) -> None:
        if not isinstance(config, SSOUploaderConfig):
            config = SSOUploaderConfig.from_uri(config)
        self._file_map = file_map
        self._config = config
        self._uploaded = False
        try:
            self._check_file_map(self.file_map, allow_partial_upload=config.allow_partial_upload)
        except ValueError as e:
            raise SSOUploadError(f"Invalid file_map: {e}") from e

    @classmethod
    def from_directory(
        cls,
        directory: Path,
        config: SSOUploaderConfig,
    ) -> Self:
        """Build an SSOUploader by scanning a directory for parquet files
        named `{table_name}.parquet` for each table in SSO_TABLES.

        Parameters
        ----------
        directory
            Path to the directory containing the parquet files.
        config
            Configuration for the SSO uploader.
        """
        if not directory.is_dir():
            raise SSOUploadError(f"Provided path is not a directory: {directory}")
        file_map = {
            table_name: path
            for table_name in SSO_TABLES
            if (path := directory / f"{table_name}.parquet").is_file()
        }
        return cls(config, file_map)

    @property
    def config(self) -> SSOUploaderConfig:
        """Return the configuration for the SSO uploader."""
        return self._config

    @property
    def file_map(self) -> Mapping[str, Path]:
        """Return the mapping of SSO table names to parquet file paths."""
        return self._file_map

    @classmethod
    def _check_file_map(cls, file_map: Mapping[str, Path], allow_partial_upload: bool = False) -> None:
        """Check the validity of the mapping between the SSO tables and the
        corresponding parquet file paths.

        Parameters
        ----------
        file_map
            A mapping of SSO table names to the corresponding parquet file
            paths.
        allow_partial_upload
            Whether to allow partial uploads even if some SSO tables are
            missing.
        """
        if not file_map:
            raise ValueError("file_map is empty; no SSO tables to upload.")

        # Check that table names are valid and point to existing parquet files.
        for table_name, parquet_path in file_map.items():
            if table_name not in SSO_TABLES:
                raise ValueError(f"Invalid SSO table name: {table_name}")
            if not parquet_path.is_file():
                raise ValueError(f"Parquet file does not exist: {parquet_path}")

        # Check for duplicate parquet file paths.
        uniq_parquet_paths = set(file_map.values())
        if len(uniq_parquet_paths) != len(file_map):
            raise ValueError("Duplicate parquet file paths found in file_map.")

        # Check for missing SSO tables, which is only an error if
        # allow_partial_upload is False.
        missing_tables = set(SSO_TABLES) - set(file_map.keys())
        if missing_tables:
            if not allow_partial_upload:
                raise ValueError(f"Missing parquet files for SSO tables: {missing_tables}")
            else:
                _LOG.warning(
                    "Partial upload allowed. Missing parquet files for SSO tables: %s", missing_tables
                )

    def upload(self) -> None:
        """Upload the SSO files to Google Cloud Storage.

        Raises
        ------
        SSOUploadError
            Raised if this uploader instance has already run once, if
            uploading a file fails, or if publishing the Pub/Sub message
            fails. Any files uploaded during a failed attempt are removed
            from Google Cloud Storage before the error is raised.

        Notes
        -----
        This method may only be called once per SSOUploader instance.
        Subsequent calls will raise an SSOUploadError.
        """
        if self._uploaded:
            raise SSOUploadError("upload() has already been called on this SSOUploader instance")
        self._uploaded = True

        object_prefix = self.config.object_prefix
        if self.config.append_unique_prefix:
            object_prefix = posixpath.join(object_prefix, self._generate_unique_prefix())

        client = Client()
        bucket = client.bucket(self.config.bucket_name)

        uploaded_object_names: list[str] = []
        try:
            for table_name, file_path in self.file_map.items():
                object_name = posixpath.join(object_prefix, f"{table_name}.parquet")
                blob = bucket.blob(object_name)
                try:
                    _LOG.info("Uploading %s to gs://%s/%s", file_path, self.config.bucket_name, object_name)
                    blob.upload_from_filename(str(file_path))
                except (GoogleAPIError, OSError) as e:
                    raise SSOUploadError(
                        f"Failed to upload {file_path} to gs://{self.config.bucket_name}/{object_name}"
                    ) from e
                uploaded_object_names.append(object_name)

            _LOG.info(
                "Uploaded %d SSO parquet files to gs://%s/%s",
                len(self.file_map),
                self.config.bucket_name,
                object_prefix,
            )

            self._publish(object_prefix)
        except Exception:
            self._cleanup(bucket, uploaded_object_names)
            raise

    @staticmethod
    def _generate_unique_prefix() -> str:
        """Generate a unique, lexicographically sortable path segment based
        on the current UTC time, with millisecond precision.
        """
        # Drop last three digits for millisecond precision from microseconds.
        return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")[:-3]

    def _cleanup(self, bucket: Bucket, object_names: list[str]) -> None:
        """Delete previously uploaded objects from Google Cloud Storage
        after a failed upload attempt.

        Parameters
        ----------
        bucket
            The Google Cloud Storage bucket containing the objects to
            delete.
        object_names
            The names of the objects to delete.
        """
        for object_name in object_names:
            try:
                bucket.blob(object_name).delete()
                _LOG.info("Deleted gs://%s/%s during cleanup", self.config.bucket_name, object_name)
            except GoogleAPIError:
                _LOG.exception(
                    "Failed to delete gs://%s/%s during cleanup; manual removal may be required",
                    self.config.bucket_name,
                    object_name,
                )

    def _publish(self, object_prefix: str) -> None:
        """Publish a message to the configured Pub/Sub topic after successful
        upload of SSO files.

        Parameters
        ----------
        object_prefix
            The effective object prefix used for the uploaded files.

        Raises
        ------
        SSOUploadError
            Raised if publishing the message fails.
        """
        if not self.config.pubsub_topic:
            _LOG.warning("No Pub/Sub topic specified; skipping publish step.")
            return

        message_data = {
            "bucket": self.config.bucket_name,
            "object_prefix": object_prefix,
            "uploaded_tables": list(self.file_map.keys()),
            "dataset_id": self.config.dataset_id,
        }

        try:
            project_id = self.config.project_id
            if project_id is None:
                # Get the project ID from the default credentials if not
                # provided in the config.
                _, project_id = google.auth.default()
            if project_id is None:
                # If the project ID still can't be determined, raise an error.
                raise SSOUploadError(
                    "Google Cloud project ID could not be determined from the config or default credentials"
                )
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(project_id, self.config.pubsub_topic)
            future = publisher.publish(topic_path, json.dumps(message_data).encode("utf-8"))
            future.result()  # Wait for the publish call to complete.
            _LOG.info("Published message to Pub/Sub topic %s: %s", self.config.pubsub_topic, message_data)
        except (GoogleAPIError, GoogleAuthError) as e:
            raise SSOUploadError(
                f"Failed to publish message to Pub/Sub topic {self.config.pubsub_topic}: {message_data}"
            ) from e
