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

import glob
import json
import logging
import posixpath
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar, Self

import yaml
from google.api_core.exceptions import DeadlineExceeded, GoogleAPIError, PreconditionFailed
from google.cloud import pubsub_v1
from google.cloud.storage import Bucket, Client
from pydantic import BaseModel, ConfigDict

from lsst.resources import ResourcePath, ResourcePathExpression

from ...gcp import get_project_id
from ..schema.constants import SSO_TABLES
from .manifest import FileEntry, Manifest

__all__ = ["UploadError", "Uploader", "UploaderConfig"]

_LOG = logging.getLogger(__name__)

_PUBSUB_TOPIC = "load-sso-topic"

# Subscription to the load-sso-result topic, where the load-sso function
# publishes the result of each load.
_RESULT_SUBSCRIPTION = "load-sso-result-sub"


def _create_file_map(directory: Path, files: Mapping[str, str]) -> dict[str, list[Path]]:
    """Find each table's parquet files from the configured paths and
    patterns.

    Parameters
    ----------
    directory
        Base directory for relative paths and patterns.
    files
        Path or glob pattern of each table's parquet files, by table name. A
        leading ``~`` is expanded to the home directory, and an absolute path
        is used as it is.

    Returns
    -------
    `dict` [ `str`, `list` [ `pathlib.Path` ] ]
        The matching paths of each table, sorted so that the same input always
        gets the same part numbers. A table whose pattern matches nothing has
        an empty list.
    """
    file_map: dict[str, list[Path]] = {}
    # Iterate over each table and its corresponding file pattern.
    for table_name, pattern in files.items():
        # Expand a leading ~ to the home directory.
        path = Path(pattern).expanduser()
        # A relative path is under the directory. An absolute one is kept.
        if not path.is_absolute():
            path = directory / path
        # Find the matching files, in a stable order.
        file_map[table_name] = sorted(Path(match) for match in glob.glob(str(path), recursive=True))
    return file_map


class UploadError(RuntimeError):
    """Raised when uploading an SSO parquet file to Google Cloud Storage
    fails.
    """


class UploaderConfig(BaseModel):
    """Configuration for the Uploader class."""

    model_config = ConfigDict(extra="forbid")
    """Pydantic model configuration. Unknown keys, such as a misspelled
    field, are rejected rather than ignored.
    """

    bucket_name: str
    """Target bucket for uploading the SSO parquet files."""

    files: dict[str, str]
    """Path or glob pattern of each table's parquet files, by table name.

    A relative path is relative to the directory given to
    `Uploader.from_directory`, and an absolute path is used as it is. A
    leading ``~`` is expanded to the home directory. A table with a single
    file can use its plain path, such as ``mpc_orbits.parquet``, and a table
    split into several files a pattern, such as ``SSObservation/*.parquet``.
    File names don't need to match the table names, as the key names the
    table. There are no defaults: a table without an entry is not uploaded.
    """

    allow_partial_upload: bool = False
    """Whether to allow upload even if some SSO tables are missing."""

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


class Uploader:
    """Class to upload SSO data to Google Cloud Storage for ingest into
    BigQuery.

    Parameters
    ----------
    config
        Configuration for the SSO uploader.
    file_map
        A mapping of SSO table names to the paths of their parquet files.

    Raises
    ------
    UploadError
        Raised if ``file_map`` is invalid, or if no Google Cloud project can
        be found.
    """

    PULL_MAX_MESSAGES: ClassVar[int] = 10
    """Most load results taken by one pull request in `wait_for_load`."""

    PULL_TIMEOUT_SECONDS: ClassVar[int] = 60
    """Longest a single pull request in `wait_for_load` waits for a load
    result, in seconds.
    """

    WAIT_TIMEOUT: ClassVar[timedelta] = timedelta(minutes=5)
    """How long `wait_for_load` waits for the result by default, the same as
    the ``load-sso`` function's timeout.
    """

    def __init__(
        self,
        config: UploaderConfig,
        file_map: Mapping[str, Sequence[Path]],
    ) -> None:
        self._file_map = file_map
        self._config = config
        self._uploaded = False
        self._object_prefix: str | None = None
        try:
            self._check_file_map(self._file_map, allow_partial_upload=config.allow_partial_upload)
        except ValueError as e:
            raise UploadError(f"Invalid file_map: {e}") from e
        # The project of the Pub/Sub topic and subscription.
        try:
            self._project_id = get_project_id()
        except RuntimeError as e:
            raise UploadError(str(e)) from e

    @classmethod
    def from_directory(
        cls,
        directory: Path,
        config: UploaderConfig,
    ) -> Self:
        """Build an `Uploader` from the files that the configuration lists
        for each table, relative to a directory.

        Parameters
        ----------
        directory
            Path to the directory containing the parquet files.
        config
            Configuration for the SSO uploader. Its relative ``files`` paths
            and patterns are matched under ``directory``, and absolute ones as
            they are.

        Raises
        ------
        UploadError
            Raised if ``directory`` isn't a directory, or for the reasons the
            `Uploader` constructor raises.
        """
        if not directory.is_dir():
            raise UploadError(f"Provided path is not a directory: {directory}")
        file_map = _create_file_map(directory, config.files)
        return cls(config, file_map)

    @classmethod
    def _check_file_map(
        cls, file_map: Mapping[str, Sequence[Path]], allow_partial_upload: bool = False
    ) -> None:
        """Check the validity of the mapping between the SSO tables and the
        paths of their parquet files.

        Parameters
        ----------
        file_map
            A mapping of SSO table names to the paths of their parquet files.
        allow_partial_upload
            Whether to allow partial uploads even if some SSO tables are
            missing.
        """
        if not file_map:
            raise ValueError("file_map is empty; no SSO tables to upload.")

        # Check that table names are valid and point to existing parquet files.
        for table_name, parquet_paths in file_map.items():
            if table_name not in SSO_TABLES:
                raise ValueError(f"Invalid SSO table name: {table_name}")
            # A pattern that matches nothing is likely a typo or a missing
            # directory, so fail rather than upload nothing for the table.
            if not parquet_paths:
                raise ValueError(f"No parquet files found for SSO table: {table_name}")
            for parquet_path in parquet_paths:
                # A pattern such as SSObservation/* can match a directory.
                if parquet_path.is_dir():
                    raise ValueError(
                        f"Parquet path for SSO table {table_name} is a directory, not a file: {parquet_path}"
                    )
                if not parquet_path.is_file():
                    raise ValueError(f"Parquet file does not exist: {parquet_path}")

        # Check that no parquet file is listed for more than one table.
        all_parquet_paths = [path for parquet_paths in file_map.values() for path in parquet_paths]
        if len(set(all_parquet_paths)) != len(all_parquet_paths):
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
        UploadError
            Raised if this uploader instance has already uploaded
            successfully, if the upload prefix already holds objects, if a
            parquet file can't be read, if uploading a file or the manifest
            fails, or if publishing the Pub/Sub message fails. Any files
            uploaded during a failed attempt are removed from Google Cloud
            Storage before the error is raised.

        Notes
        -----
        Each table's files are uploaded as
        ``<prefix>/<table>/part-NNNNN.parquet``, numbered in the order of
        their local paths, whatever their local names. A `Manifest` listing
        them is uploaded last as ``<prefix>/manifest.json``.

        This method may only succeed once per Uploader instance. After a
        failed upload it may be called again.
        """
        if self._uploaded:
            raise UploadError("This Uploader instance has already uploaded its files")

        # Build the manifest first, reading each file's row count, size and
        # checksum, so that a bad parquet file fails before anything is
        # uploaded.
        tables: dict[str, dict[str, FileEntry]] = {}
        for table_name, file_paths in self._file_map.items():
            tables[table_name] = {}
            for index, file_path in enumerate(file_paths):
                try:
                    tables[table_name][f"part-{index:05d}.parquet"] = FileEntry.from_path(file_path)
                except (ValueError, OSError) as e:
                    raise UploadError(
                        f"Invalid parquet file for SSO table {table_name}: {file_path}: {e}"
                    ) from e
        manifest = Manifest(tables=tables)

        # The upload's prefix in the bucket: the current UTC time to the
        # millisecond, such as 20261118T120000123.
        object_prefix = self._generate_unique_prefix()

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self._project_id, _PUBSUB_TOPIC)

        client = Client()
        bucket = client.bucket(self._config.bucket_name)

        # Refuse a prefix that already holds objects, such as from another
        # upload started at the same moment, before writing anything to it.
        try:
            existing = list(bucket.list_blobs(prefix=f"{object_prefix}/", max_results=1))
        except GoogleAPIError as e:
            raise UploadError(f"Failed to check gs://{self._config.bucket_name}/{object_prefix}/") from e
        if existing:
            raise UploadError(
                f"Upload prefix gs://{self._config.bucket_name}/{object_prefix}/ already holds objects"
            )

        uploaded_object_names: list[str] = []
        try:
            # Upload each file in the manifest under its part name.
            for table_name, entries in manifest.tables.items():
                for file_name, entry in entries.items():
                    source_path = entry.source_path
                    object_name = posixpath.join(object_prefix, table_name, file_name)
                    blob = bucket.blob(object_name)
                    try:
                        _LOG.info(
                            "Uploading %s to gs://%s/%s", source_path, self._config.bucket_name, object_name
                        )
                        # Write only if the object doesn't exist yet, so that
                        # an upload started at the same moment can't
                        # overwrite it.
                        blob.upload_from_filename(source_path, if_generation_match=0)
                    except PreconditionFailed as e:
                        raise UploadError(
                            f"Object already exists at gs://{self._config.bucket_name}/{object_name}, "
                            "so another upload is using this prefix"
                        ) from e
                    except (GoogleAPIError, OSError) as e:
                        raise UploadError(
                            f"Failed to upload {source_path} to gs://{self._config.bucket_name}/{object_name}"
                        ) from e
                    uploaded_object_names.append(object_name)

            _LOG.info(
                "Uploaded %d SSO parquet files to gs://%s/%s",
                len(uploaded_object_names),
                self._config.bucket_name,
                object_prefix,
            )

            # The manifest goes last, so that its presence means the upload is
            # complete.
            manifest_name = posixpath.join(object_prefix, Manifest.FILE_NAME)
            try:
                bucket.blob(manifest_name).upload_from_string(
                    manifest.model_dump_json(indent=2),
                    content_type="application/json",
                    if_generation_match=0,
                )
            except (GoogleAPIError, OSError) as e:
                raise UploadError(
                    f"Failed to upload the manifest to gs://{self._config.bucket_name}/{manifest_name}"
                ) from e
            uploaded_object_names.append(manifest_name)

            self._publish(publisher, topic_path, object_prefix)
        except Exception:
            self._cleanup(bucket, uploaded_object_names)
            raise

        self._uploaded = True
        # Kept so that wait_for_load can pick this upload's result out of the
        # load results, which name the upload by its prefix.
        self._object_prefix = object_prefix

    def wait_for_load(self, timeout: timedelta | None = None) -> str:
        """Wait for the ``load-sso`` function to load the uploaded files.

        Parameters
        ----------
        timeout
            How long to wait for the result of the load. If `None`,
            `WAIT_TIMEOUT`.

        Returns
        -------
        `str`
            ID of the BigQuery job that ran the load's transaction.

        Raises
        ------
        UploadError
            Raised if nothing has been uploaded, if the load failed, if no
            result arrived within ``timeout``, or if reading the results
            fails.

        Notes
        -----
        The function publishes the result of each load to the
        ``load-sso-result`` topic, as JSON with ``object_prefix``,
        ``status`` (``succeeded`` or ``failed``), and ``job_id`` or
        ``error``. This method pulls from the topic's subscription until the
        result for this upload arrives. Results for other uploads are
        acknowledged and ignored, since they're left over from earlier
        loads.
        """
        if self._object_prefix is None:
            raise UploadError("wait_for_load() was called before upload(). Call upload() first.")
        folder = f"gs://{self._config.bucket_name}/{self._object_prefix}"

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(self._project_id, _RESULT_SUBSCRIPTION)
        if timeout is None:
            timeout = self.WAIT_TIMEOUT
        deadline = time.monotonic() + timeout.total_seconds()
        with subscriber:
            while (remaining := deadline - time.monotonic()) > 0:
                try:
                    response = subscriber.pull(
                        subscription=subscription_path,
                        max_messages=self.PULL_MAX_MESSAGES,
                        timeout=min(remaining, self.PULL_TIMEOUT_SECONDS),
                    )
                    if response.received_messages:
                        subscriber.acknowledge(
                            subscription=subscription_path,
                            ack_ids=[received.ack_id for received in response.received_messages],
                        )
                except DeadlineExceeded:
                    # No result arrived during this pull.
                    continue
                except GoogleAPIError as e:
                    raise UploadError(f"Failed to read the load results from {subscription_path}") from e

                for received in response.received_messages:
                    result = json.loads(received.message.data)
                    if result["object_prefix"] != self._object_prefix:
                        # A result for another upload, such as an earlier one.
                        continue
                    if result["status"] == "failed":
                        raise UploadError(f"Loading {folder} failed: {result['error']}")
                    return result["job_id"]

        raise UploadError(
            f"No load result for {folder} within {timeout}. Check the logs of the load-sso function."
        )

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
                _LOG.info("Deleted gs://%s/%s during cleanup", self._config.bucket_name, object_name)
            except GoogleAPIError:
                _LOG.exception(
                    "Failed to delete gs://%s/%s during cleanup; manual removal may be required",
                    self._config.bucket_name,
                    object_name,
                )

    def _publish(self, publisher: pubsub_v1.PublisherClient, topic_path: str, object_prefix: str) -> None:
        """Publish a message to the configured Pub/Sub topic after successful
        upload of SSO files.

        The message holds only the bucket and the object prefix. The loader
        reads the tables and files from the manifest under the prefix.

        Parameters
        ----------
        publisher
            Client that publishes the message.
        topic_path
            Full path of the Pub/Sub topic.
        object_prefix
            The effective object prefix used for the uploaded files.

        Raises
        ------
        UploadError
            Raised if publishing the message fails.
        """
        message_data = {
            "bucket": self._config.bucket_name,
            "object_prefix": object_prefix,
        }

        try:
            future = publisher.publish(topic_path, json.dumps(message_data).encode("utf-8"))
            future.result()  # Wait for the publish call to complete.
            _LOG.info("Published message to Pub/Sub topic %s: %s", _PUBSUB_TOPIC, message_data)
        except GoogleAPIError as e:
            raise UploadError(
                f"Failed to publish message to Pub/Sub topic {_PUBSUB_TOPIC}: {message_data}"
            ) from e
