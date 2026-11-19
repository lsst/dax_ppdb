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

__all__ = ["LoadError", "Loader"]

import logging
import posixpath
from datetime import timedelta
from typing import Any

from cloudevents.http import CloudEvent
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud.bigquery.retry import DEFAULT_RETRY

from ...gcp import NETWORK_ERRORS, CloudEventLogger, DecodeMessageDataError, decode_message_data
from ..manifest import InvalidManifestError
from ..ppdb_bigquery_config import DatasetType, PpdbBigQueryConfig
from ..schema.constants import SSO_TABLES
from ..table_refs import TableRefs
from .manifest import Manifest
from .result_publisher import ResultPublisher

# Pub/Sub topic for the result of load operations, used to wait for the load
# to finish.
_RESULT_TOPIC = "load-sso-result"


class LoadError(Exception):
    """Raised when loading SSO data into BigQuery failed."""


class Loader:
    """Loads SSO parquet files from Cloud Storage into the internal SSO
    tables, replacing their contents.

    Parameters
    ----------
    config
        PPDB configuration providing the project ID and dataset names.
    client
        BigQuery client that runs the jobs.
    job_timeout
        Limit on each load job and on the transaction. BigQuery cancels a job
        that runs longer.
    retry_timeout
        Limit on how long each BigQuery API request is retried after a
        temporary error.
    result_topic
        Name of the Pub/Sub topic for the result of each load.

    Notes
    -----
    Each table is first loaded into its own landing table, and then all of
    them are copied into the internal tables in one transaction, so a failed
    load leaves every internal table unchanged. The internal tables are never
    dropped or replaced, so their clustering and search indexes are kept.
    """

    def __init__(
        self,
        config: PpdbBigQueryConfig,
        client: bigquery.Client,
        job_timeout: timedelta,
        retry_timeout: timedelta,
        result_topic: str = _RESULT_TOPIC,
    ):
        self._config = config
        self._table_refs = TableRefs(config)
        self._client = client
        self._result_publisher = ResultPublisher(config.project_id, result_topic)
        self._job_timeout_ms = int(job_timeout.total_seconds() * 1000)
        self._retry = DEFAULT_RETRY.with_timeout(retry_timeout.total_seconds())

    def load_from_event(self, event: CloudEvent, log: logging.Logger) -> None:
        """Load the SSO files named in a Pub/Sub message, and publish the
        result.

        Parameters
        ----------
        event
            The CloudEvent delivered by the Pub/Sub trigger. The message data
            should include ``bucket`` and ``object_prefix``, the location of
            the upload in Cloud Storage.
        log
            Logger of the function, which the load request's log entries are
            written with.

        Notes
        -----
        This never raises for a bad message or a failed load, so the
        function acknowledges the message and Pub/Sub doesn't deliver it
        again. A bad message is logged and has no result, since it names no
        upload. A load that fails or succeeds is logged, and its result is
        published.
        """
        # Fields added to every log entry, including those of _load().
        log_fields: dict[str, Any] = {}
        logger = CloudEventLogger(log, event["id"], log_fields)

        try:
            data = decode_message_data(logger, event)
        except DecodeMessageDataError:
            # Already logged by decode_message_data.
            return

        try:
            bucket = data["bucket"]
            object_prefix = data["object_prefix"]
        except KeyError as e:
            logger.log_event(
                logging.ERROR,
                "Missing key in load SSO message",
                "missing_key_in_pubsub_message",
                error=e,
                pubsub_message=data,
            )
            return
        log_fields["gcs_bucket"] = bucket
        log_fields["gcs_object_prefix"] = object_prefix

        try:
            job_id = self._load(logger, bucket, object_prefix)
        except LoadError as e:
            self._result_publisher.publish_failed(logger, object_prefix, str(e))
        else:
            self._result_publisher.publish_succeeded(logger, object_prefix, job_id)

    def _load(self, logger: CloudEventLogger, bucket: str, object_prefix: str) -> str:
        """Load SSO parquet files into the internal tables from Cloud Storage.

        Parameters
        ----------
        logger
            Logger for the load request, e.g. with the ID of the Pub/Sub
            message that triggered it.
        bucket
            Name of the Cloud Storage bucket holding the upload.
        object_prefix
            Prefix of the upload in the bucket, under which the manifest and
            each table's ``<table>/part-NNNNN.parquet`` files are.

        Returns
        -------
        `str`
            ID of the BigQuery job that ran the transaction.

        Raises
        ------
        LoadError
            Raised if loading failed for any reason, such as a missing or
            invalid manifest, a parquet file that doesn't match the table
            schema, a row count that doesn't match the manifest, or a Google
            Cloud error.

        Notes
        -----
        A failed load isn't retried. Each BigQuery and Cloud Storage request
        is still retried after a temporary error, but a failed job isn't run
        again.
        """
        try:
            folder = f"gs://{bucket}/{object_prefix}"
            manifest = Manifest.from_uri(folder)
            # Ensure that only data for SSO tables is loaded.
            other_tables = sorted(set(manifest.tables) - set(SSO_TABLES))
            if other_tables:
                raise InvalidManifestError(
                    f"Manifest in {folder} names tables that aren't SSO tables: {other_tables}"
                )
            logger.log_event(
                logging.INFO,
                "Loading SSO tables",
                "sso_load_started",
                folder=folder,
                tables=list(manifest.tables),
            )

            # Get the internal table schemas to use for the landing tables.
            schemas = {
                table_name: self._client.get_table(
                    self._table_refs.internal(table_name), retry=self._retry
                ).schema
                for table_name in manifest.tables
            }

            # Submit a load job for each table.
            landing_tables: dict[str, str] = {}
            jobs: dict[str, bigquery.LoadJob] = {}
            for table_name in manifest.tables:
                landing_tables[table_name] = self._config.fqn_for(
                    DatasetType.LANDING, f"{table_name}_sso_{object_prefix}"
                )
                # Create a list of part files to load from the manifest.
                source_uris = [
                    posixpath.join(folder, table_name, file_name) for file_name in manifest.tables[table_name]
                ]
                jobs[table_name] = self._client.load_table_from_uri(
                    source_uris,
                    landing_tables[table_name],  # Destination landing table
                    job_config=bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.PARQUET,
                        schema=schemas[table_name],
                        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        job_timeout_ms=self._job_timeout_ms,
                    ),
                    job_id_prefix=f"load_sso_{table_name}_",
                    retry=self._retry,
                )
            logger.log_event(
                logging.INFO,
                "Submitted landing load jobs",
                "landing_jobs_submitted",
                landing_tables=landing_tables,
                job_ids={table_name: job.job_id for table_name, job in jobs.items()},
            )
            # Wait for the jobs only after all are submitted, so they run
            # concurrently.
            for load_job in jobs.values():
                load_job.result(retry=self._retry)

            # Build the transaction. For each table, replace the internal
            # table's rows with the landing table's, then check the row count
            # against the manifest.
            statements = ["BEGIN TRANSACTION;"]
            for table_name, landing in landing_tables.items():
                internal = self._table_refs.internal(table_name)
                columns = ", ".join(f"`{field.name}`" for field in schemas[table_name])
                statements += [
                    f"TRUNCATE TABLE `{internal}`;",
                    f"INSERT INTO `{internal}` ({columns}) SELECT {columns} FROM `{landing}`;",
                    f"ASSERT (SELECT COUNT(*) FROM `{internal}`) = {manifest.row_count(table_name)} "
                    f"AS 'row count in internal table {table_name} does not match the manifest';",
                ]
            statements.append("COMMIT TRANSACTION;")

            # Submit the transaction as one query job. If any statement fails,
            # including an ASSERT, the whole transaction is rolled back
            # automatically.
            transaction_job = self._client.query(
                "\n".join(statements),
                job_config=bigquery.QueryJobConfig(job_timeout_ms=self._job_timeout_ms),
                job_id_prefix="load_sso_replace_",
                retry=self._retry,
                job_retry=None,
            )
            logger.log_event(
                logging.INFO,
                "Submitted the SSO transaction",
                "sso_transaction_submitted",
                job_id=transaction_job.job_id,
            )
            # Wait for the transaction to finish. If it fails, it isn't run
            # again.
            transaction_job.result(retry=self._retry)

            # Drop the landing tables. If this fails, the tables expire later.
            for landing_table in landing_tables.values():
                try:
                    self._client.delete_table(landing_table, not_found_ok=True, retry=self._retry)
                except (GoogleAPICallError, *NETWORK_ERRORS) as e:
                    logger.log_event(
                        logging.WARNING,
                        "Could not drop landing table",
                        "landing_table_not_dropped",
                        error=e,
                        landing_table=landing_table,
                    )

            logger.log_event(logging.INFO, "Loaded SSO tables", "sso_loaded", job_id=transaction_job.job_id)
            return transaction_job.job_id
        except Exception as e:
            logger.log_event(logging.ERROR, "SSO load failed", "sso_load_failed", error=e)
            raise LoadError(str(e)) from e
