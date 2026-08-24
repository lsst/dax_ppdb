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
from pathlib import Path

import google.auth
from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import GoogleAuthError
from google.cloud import pubsub_v1
from google.cloud.storage import Bucket, Client

from .schema.constants import SSO_TABLES

__all__ = ["SSOUploadError", "SSOUploader"]

_LOG = logging.getLogger(__name__)


class SSOUploadError(RuntimeError):
    """Raised when uploading an SSO parquet file to Google Cloud Storage
    fails.
    """


class SSOUploader:
    """Class to upload SSO data to Google Cloud Storage for ingest into
    BigQuery.

    Parameters
    ----------
    file_map
        A mapping of SSO table names to the corresponding parquet file paths.
    bucket_name
        The target Google Cloud Storage bucket for uploading the parquet files.
    object_prefix
        The prefix to use for the uploaded objects in the bucket.
    allow_partial_upload
        Whether to allow partial uploads even if some SSO tables are missing.
    pubsub_topic
        The Pub/Sub topic to publish a message to after successful upload. This
        may be ommitted if no message is to be published.
    """

    def __init__(
        self,
        file_map: Mapping[str, Path],
        bucket_name: str,
        object_prefix: str,
        dataset_id: str,
        allow_partial_upload: bool = False,
        pubsub_topic: str | None = None,
    ) -> None:
        try:
            self._check_file_map(file_map, allow_partial_upload=allow_partial_upload)
        except ValueError as e:
            raise SSOUploadError(f"Invalid file_map: {e}") from e
        self.file_map = file_map
        self.bucket_name = bucket_name
        self.object_prefix = object_prefix
        self.pubsub_topic = pubsub_topic
        self.dataset_id = dataset_id

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
            Raised if uploading a file or publishing the Pub/Sub message
            fails. Any files uploaded during the failed attempt are removed
            from Google Cloud Storage before the error is raised.
        """
        client = Client()
        bucket = client.bucket(self.bucket_name)

        uploaded_object_names: list[str] = []
        try:
            for table_name, file_path in self.file_map.items():
                object_name = posixpath.join(self.object_prefix, f"{table_name}.parquet")
                blob = bucket.blob(object_name)
                try:
                    _LOG.info("Uploading %s to gs://%s/%s", file_path, self.bucket_name, object_name)
                    blob.upload_from_filename(str(file_path))
                except (GoogleAPIError, OSError) as e:
                    raise SSOUploadError(
                        f"Failed to upload {file_path} to gs://{self.bucket_name}/{object_name}"
                    ) from e
                uploaded_object_names.append(object_name)

            _LOG.info(
                "Uploaded %d SSO parquet files to gs://%s/%s",
                len(self.file_map),
                self.bucket_name,
                self.object_prefix,
            )

            self._publish()
        except Exception:
            self._cleanup(bucket, uploaded_object_names)
            raise

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
                _LOG.info("Deleted gs://%s/%s during cleanup", self.bucket_name, object_name)
            except GoogleAPIError:
                _LOG.exception(
                    "Failed to delete gs://%s/%s during cleanup; manual removal may be required",
                    self.bucket_name,
                    object_name,
                )

    def _publish(self) -> None:
        """Publish a message to the specified Pub/Sub topic after successful
        upload of SSO files.

        Raises
        ------
        SSOUploadError
            Raised if publishing the message fails.
        """
        if not self.pubsub_topic:
            _LOG.warning("No Pub/Sub topic specified; skipping publish step.")
            return

        message_data = {
            "bucket": self.bucket_name,
            "object_prefix": self.object_prefix,
            "uploaded_tables": list(self.file_map.keys()),
            "dataset_id": self.dataset_id,
        }

        try:
            _, project_id = google.auth.default()
            assert project_id is not None, "Failed to determine Google Cloud project ID"
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(project_id, self.pubsub_topic)
            future = publisher.publish(topic_path, json.dumps(message_data).encode("utf-8"))
            future.result()  # Wait for the publish call to complete
            _LOG.info("Published message to Pub/Sub topic %s: %s", self.pubsub_topic, message_data)
        except (GoogleAPIError, GoogleAuthError) as e:
            raise SSOUploadError(
                f"Failed to publish message to Pub/Sub topic {self.pubsub_topic}: {message_data}"
            ) from e
