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

__all__ = ["ResultPublisher"]

import logging
from typing import Any

from google.api_core.exceptions import GoogleAPIError

from ...gcp import CloudEventLogger, Publisher


class ResultPublisher:
    """Publishes the result of each SSO load to the ``load-sso-result``
    topic, so the uploader can wait for it.

    Parameters
    ----------
    project_id
        Google Cloud project of the topic.
    topic_name
        Name of the topic.

    Notes
    -----
    Each result is JSON with ``object_prefix``, ``status`` (``succeeded`` or
    ``failed``), and ``job_id`` or ``error``.
    `~lsst.dax.ppdb.bigquery.sso.Uploader.wait_for_load` reads it. A failure
    to publish is logged, not raised, because raising from the function would
    make Pub/Sub deliver the load message again.
    """

    def __init__(self, project_id: str, topic_name: str):
        self._publisher = Publisher(project_id, topic_name)

    def publish_succeeded(self, logger: CloudEventLogger, object_prefix: str, job_id: str) -> None:
        """Publish the result of a load that succeeded.

        Parameters
        ----------
        logger
            Logger for the load request.
        object_prefix
            Prefix of the upload in the bucket.
        job_id
            ID of the BigQuery job that ran the load's transaction.
        """
        self._publish(logger, {"object_prefix": object_prefix, "status": "succeeded", "job_id": job_id})

    def publish_failed(self, logger: CloudEventLogger, object_prefix: str, error: str) -> None:
        """Publish the result of a load that failed.

        Parameters
        ----------
        logger
            Logger for the load request.
        object_prefix
            Prefix of the upload in the bucket.
        error
            Why the load failed.
        """
        self._publish(logger, {"object_prefix": object_prefix, "status": "failed", "error": error})

    def _publish(self, logger: CloudEventLogger, result: dict[str, Any]) -> None:
        """Publish a result and wait for it to be sent, logging a failure.

        Parameters
        ----------
        logger
            Logger for the load request.
        result
            The result to publish as JSON.
        """
        try:
            self._publisher.publish(result).result()
        except (GoogleAPIError, TimeoutError) as e:
            logger.log_event(
                logging.WARNING,
                "Failed to publish the SSO load result",
                "sso_result_not_published",
                error=e,
                result=result,
            )
