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

__all__ = ["DiaSourceMinSspExporter", "string_to_bigquery_compression"]

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from cloudevents.http import CloudEvent
from google.cloud import bigquery
from google.cloud.bigquery.retry import DEFAULT_RETRY
from pydantic import BaseModel

from ...gcp import CloudEventLogger, DecodeMessageDataError, StorageClient, decode_message_data
from ..ppdb_bigquery_config import DatasetType, PpdbBigQueryConfig

# DiaSource columns that SSP needs.
_COLUMNS = ("diaSourceId", "ra", "dec", "midpointMjdTai", "visit", "detector")

# Format of promotion_time in the promotion-succeeded message.
_PROMOTION_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def string_to_bigquery_compression(value: str) -> str:
    """Convert a string to a BigQuery compression type for Parquet exports.

    Parameters
    ----------
    value
        Name of the compression type, in any case, such as ``snappy``.

    Returns
    -------
    `str`
        The compression type, as `google.cloud.bigquery.Compression` names
        it.

    Raises
    ------
    ValueError
        Raised if ``value`` isn't a compression type that the BigQuery client
        knows.

    Notes
    -----
    BigQuery itself checks whether Parquet supports the type when the export
    runs.
    """
    try:
        return bigquery.Compression[value.upper()].value
    except KeyError:
        raise ValueError(f"Unknown BigQuery compression type: {value}") from None


class _Manifest(BaseModel):
    """Contents of the ``manifest.json`` written last in each export's
    folder.

    Notes
    -----
    ``row_count`` and ``file_count`` come from BigQuery's statistics for the
    export job, so a reader can check the files found in the folder against
    them.
    """

    exported_at: datetime
    """Time the export started, in UTC."""

    row_count: int
    """Number of rows exported."""

    file_count: int
    """Number of Parquet files written."""


class DiaSourceMinSspExporter:
    """Exports the DiaSource columns that SSP needs from the public
    ``DiaSource`` view to Parquet files in Cloud Storage.

    Parameters
    ----------
    config
        PPDB configuration providing the project ID and dataset names.
    client
        BigQuery client that runs the jobs.
    bucket
        Name of the Cloud Storage bucket for the exports.
    compression
        Compression type of the Parquet files, as returned by
        `string_to_bigquery_compression`.
    job_timeout
        Limit on each BigQuery job. BigQuery cancels a job that runs longer.
    retry_timeout
        Limit on how long each BigQuery API request is retried after a
        temporary error.

    Notes
    -----
    Each export is the whole table, written to a folder named for the
    message's ``promotion_time``, such as
    ``20261202T223512Z/part-000000000000.parquet``, with a ``manifest.json``
    written last, so a folder with a manifest holds a complete export. A
    failed export is only logged.

    A message for a folder that already has a manifest is skipped, so a
    finished export is never overwritten. Otherwise the folder's objects,
    left by a failed or interrupted export, are deleted before exporting.
    """

    def __init__(
        self,
        config: PpdbBigQueryConfig,
        client: bigquery.Client,
        bucket: str,
        compression: str,
        job_timeout: timedelta,
        retry_timeout: timedelta,
    ):
        self._config = config
        self._client = client
        self._bucket = bucket
        self._storage = StorageClient(bucket)
        self._compression = compression
        self._job_timeout_ms = int(job_timeout.total_seconds() * 1000)
        self._retry = DEFAULT_RETRY.with_timeout(retry_timeout.total_seconds())

    def export_from_event(self, event: CloudEvent, log: logging.Logger) -> None:
        """Export the table when triggered by a Pub/Sub message.

        Parameters
        ----------
        event
            The CloudEvent delivered by the Pub/Sub trigger. The message data
            should include ``promotion_time``, which names the export's
            folder.
        log
            Logger of the function, which the export request's log entries
            are written with.

        Notes
        -----
        This never raises for a bad message or a failed export, so the
        function acknowledges the message and Pub/Sub doesn't deliver it
        again. A failed export is logged, which reaches Sentry.
        """
        # Fields added to every log entry, including those of _export().
        log_fields: dict[str, Any] = {"gcs_bucket": self._bucket}
        logger = CloudEventLogger(log, event["id"], log_fields)

        try:
            data = decode_message_data(logger, event)
        except DecodeMessageDataError:
            # Already logged by decode_message_data.
            return

        try:
            promotion_time = datetime.strptime(data["promotion_time"], _PROMOTION_TIME_FORMAT)
        except (KeyError, ValueError, TypeError) as e:
            logger.log_event(
                logging.ERROR,
                "Missing or invalid promotion_time in promotion message",
                "invalid_promotion_time",
                error=e,
                pubsub_message=data,
            )
            return
        folder = promotion_time.strftime("%Y%m%dT%H%M%SZ")
        log_fields["gcs_folder"] = folder

        try:
            self._export(logger, folder)
        except Exception as e:
            logger.log_event(logging.ERROR, "DiaSource export failed", "diasource_export_failed", error=e)

    def _export(self, logger: CloudEventLogger, folder: str) -> None:
        """Export the table to Parquet files in a folder of the bucket, and
        write the export's manifest last.

        Parameters
        ----------
        logger
            Logger for the export request.
        folder
            Folder of the export in the bucket.

        Raises
        ------
        Exception
            Raised if the export failed for any reason, such as a BigQuery or
            Cloud Storage error.

        Notes
        -----
        The row and file counts are read from the job's
        ``exportDataStatistics``, which the BigQuery client doesn't expose
        through its public API.
        """
        if self._storage.bucket.blob(f"{folder}/manifest.json").exists():
            logger.log_event(
                logging.WARNING,
                "The export folder already has a manifest, so it isn't exported again",
                "diasource_export_exists",
            )
            return

        # Delete any files left by a failed or interrupted export.
        self._storage.delete_recursive(f"{folder}/")

        export_time = datetime.now(UTC).replace(microsecond=0)
        table = self._config.fqn_for(DatasetType.PUBLIC, "DiaSource")
        uri = f"gs://{self._bucket}/{folder}/part-*.parquet"
        columns = ", ".join(f"`{column}`" for column in _COLUMNS)
        export_job = self._client.query(
            f"EXPORT DATA OPTIONS (uri = '{uri}', format = 'PARQUET', "
            f"compression = '{self._compression}', overwrite = true) "
            f"AS SELECT {columns} FROM `{table}`",
            job_config=bigquery.QueryJobConfig(job_timeout_ms=self._job_timeout_ms),
            job_id_prefix="export_diasource_min_ssp_",
            retry=self._retry,
            job_retry=None,
        )
        logger.log_event(
            logging.INFO,
            "Submitted the DiaSource export",
            "diasource_export_submitted",
            job_id=export_job.job_id,
            uri=uri,
        )
        export_job.result(retry=self._retry)

        statistics = export_job._properties["statistics"]["query"]["exportDataStatistics"]
        manifest = _Manifest(
            exported_at=export_time,
            row_count=int(statistics["rowCount"]),
            file_count=int(statistics["fileCount"]),
        )
        self._storage.upload_from_string(f"{folder}/manifest.json", manifest.model_dump_json(indent=2))

        logger.log_event(
            logging.INFO,
            "Exported DiaSource",
            "diasource_exported",
            job_id=export_job.job_id,
            row_count=manifest.row_count,
            file_count=manifest.file_count,
        )
