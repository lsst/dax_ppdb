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

import http.client
import logging
import ssl
from typing import Any

import httplib2
from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import TransportError
from googleapiclient.errors import HttpError

from .cloud_event_logger import CloudEventLogger

__all__ = ["handle_request_error"]


def handle_request_error(logger: CloudEventLogger, e: Exception) -> bool:
    """Log a failed request (e.g. a Dataflow job launch or BigQuery call)
    and report whether it is retryable.
    """
    extra_fields: dict[str, Any] = {}
    if isinstance(e, HttpError):
        # Retryable if the HTTP status code indicates a transient error:
        # - 429 Too Many Requests
        # - 500 Internal Server Error
        # - 503 Service Unavailable
        retryable = e.resp.status in (429, 500, 503)
        extra_fields["http_status"] = e.resp.status
    elif isinstance(
        e,
        (
            GoogleAPICallError,
            ConnectionError,
            TimeoutError,
            ssl.SSLError,
            http.client.HTTPException,
            httplib2.ServerNotFoundError,
            TransportError,
        ),
    ):
        # Covers transient network failures, e.g. ConnectionResetError,
        # TLS errors, truncated responses, DNS lookup failures, and
        # credential-refresh transport errors.
        retryable = True
    else:
        # Other types of submission errors are considered non-retryable.
        retryable = False

    if retryable:
        level = logging.WARNING
        log_message = "Retryable error in request"
        event_name = "retryable_error"
    else:
        level = logging.ERROR
        log_message = "Non-retryable error in request"
        event_name = "non_retryable_error"

    logger.log_event(
        level,
        log_message,
        event_name,
        error=e,
        **extra_fields,
    )
    return retryable
