# This file is part of dax_ppdbx_gcp
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
    "QueryRunner",
]

import logging

from google.cloud import bigquery


class QueryRunner:
    """Class to run BigQuery queries with logging.

    Parameters
    ----------
    project_id
        Google Cloud project ID.
    dataset_id
        BigQuery dataset ID.
    """

    def __init__(self, project_id: str, dataset_id: str):
        self._bq_client = bigquery.Client(project=project_id)
        self._dataset = self._bq_client.get_dataset(f"{project_id}.{dataset_id}")
        self._location = self._dataset.location

    @property
    def dataset(self) -> bigquery.Dataset:
        """Dataset reference (`bigquery.Dataset`, read-only)."""
        return self._dataset

    @property
    def location(self) -> str:
        """Dataset location, typically the region where it is hosted (`str`,
        read-only).
        """
        return self._location

    @classmethod
    def log_job(
        cls,
        job: bigquery.job.QueryJob
        | bigquery.job.LoadJob
        | bigquery.job.CopyJob
        | bigquery.job.ExtractJob
        | bigquery.job.UnknownJob,
        label: str,
        level: int = logging.DEBUG,
    ) -> None:
        """Log details of a BigQuery job.

        Parameters
        ----------
        job
            The BigQuery job to log.
        label
            A label for the job, typically indicating the type of operation
            (e.g., "insert", "delete", "copy").
        level
            The logging level to use for the log message. Defaults to
            `logging.DEBUG`.
        """
        logging.log(
            level,
            "BQ %s: job_id=%s location=%s state=%s bytes_processed=%s bytes_billed=%s slot_millis=%s "
            "dml_rows=%s reference_tables=%s",
            label,
            job.job_id,
            job.location,
            job.state,
            getattr(job, "total_bytes_processed", None),
            getattr(job, "total_bytes_billed", None),
            getattr(job, "slot_millis", None),
            getattr(job, "num_dml_affected_rows", None),
            getattr(job, "referenced_tables", None),
        )

    def run_job(
        self, label: str, sql: str, job_config: bigquery.QueryJobConfig | None = None
    ) -> bigquery.job.QueryJob:
        """Run a BigQuery job with the given SQL and configuration.

        Parameters
        ----------
        label
            A label for the job, typically indicating the type of operation
            (e.g., "insert", "delete", "copy").
        sql
            The SQL query to execute.
        job_config
            Configuration for the job, such as query parameters or write
            dispositions. If not provided, a default configuration will be
            used.

        Returns
        -------
        `bigquery.job.QueryJob`
            The BigQuery job object representing the executed query. This can
            be used to check the status of the job, retrieve results, or log
            additional details.
        """
        job = self._bq_client.query(sql, job_config=job_config, location=self.dataset.location)

        # Wait for the job to complete.
        job.result()

        # Log the job details.
        self.log_job(job, label)

        return job
