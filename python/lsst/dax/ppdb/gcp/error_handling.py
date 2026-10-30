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

"""Logging and classification of Dataflow job launch, BigQuery, Cloud Storage
and database errors as retryable or non-retryable.
"""

import http.client
import logging
import ssl
from typing import Any

import httplib2
import requests
import sqlalchemy.exc
from google.api_core.exceptions import GoogleAPICallError, ServerError, TooManyRequests
from google.auth.exceptions import TransportError
from googleapiclient.errors import HttpError

from .cloud_event_logger import CloudEventLogger

__all__ = [
    "handle_bigquery_error",
    "handle_database_error",
    "handle_dataflow_launch_error",
    "handle_storage_error",
]

# BigQuery error reasons worth retrying, e.g. a 403 quotaExceeded.
_TRANSIENT_BIGQUERY_REASONS = frozenset(
    {
        "backendError",
        "internalError",
        "jobBackendError",
        "jobInternalError",
        "quotaExceeded",
        "rateLimitExceeded",
    }
)


def _is_transient_google_api_error(e: Exception) -> bool:
    """Return whether an error from a Google Cloud client library is worth
    retrying whatever the service: a 429 or 5xx response, or a network error.
    """
    return isinstance(
        e,
        (
            TooManyRequests,
            ServerError,
            ConnectionError,
            TimeoutError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            TransportError,
        ),
    )


def _has_transient_bigquery_reason(e: Exception) -> bool:
    """Return whether a BigQuery error has a reason worth retrying, e.g. a 403
    with reason ``quotaExceeded``.
    """
    if not isinstance(e, GoogleAPICallError):
        return False
    reasons = {d.get("reason") for d in e.errors if isinstance(d, dict)}
    return bool(reasons & _TRANSIENT_BIGQUERY_REASONS)


def handle_bigquery_error(logger: CloudEventLogger, e: Exception, operation: str) -> bool:
    """Log a failed BigQuery operation and report whether it is retryable.

    Parameters
    ----------
    logger
        Logger used to report the error.
    e
        The exception raised by the failed operation.
    operation
        What was being done, for the log message, e.g. "copying chunk 42 to
        staging".

    Returns
    -------
    `bool`
        `True` if the error is retryable, `False` otherwise.
    """
    # Retryable: 429 and 5xx responses, network errors, and errors with a
    # quota, rate limit or backend reason. Anything else, e.g. a missing table,
    # an invalid query or a failed ASSERT, is not retryable.
    if _is_transient_google_api_error(e) or _has_transient_bigquery_reason(e):
        logger.log_event(
            logging.WARNING,
            f"Retryable BigQuery error while {operation}",
            "retryable_bigquery_error",
            error=e,
        )
        return True

    logger.log_event(
        logging.ERROR,
        f"Non-retryable BigQuery error while {operation}",
        "non_retryable_bigquery_error",
        error=e,
    )
    return False


def handle_storage_error(logger: CloudEventLogger, e: Exception, operation: str) -> bool:
    """Log a failed Cloud Storage operation and report whether it is
    retryable.

    Parameters
    ----------
    logger
        Logger used to report the error.
    e
        The exception raised by the failed operation.
    operation
        What was being done, for the log message, e.g. "reading the chunk
        manifest".

    Returns
    -------
    `bool`
        `True` if the error is retryable, `False` otherwise.
    """
    # Retryable: 429 and 5xx responses, and network errors. Anything else,
    # e.g. a missing object or missing permissions, is not retryable.
    if _is_transient_google_api_error(e):
        logger.log_event(
            logging.WARNING,
            f"Retryable Cloud Storage error while {operation}",
            "retryable_storage_error",
            error=e,
        )
        return True

    logger.log_event(
        logging.ERROR,
        f"Non-retryable Cloud Storage error while {operation}",
        "non_retryable_storage_error",
        error=e,
    )
    return False


def handle_dataflow_launch_error(logger: CloudEventLogger, e: Exception) -> bool:
    """Log a failed Dataflow job launch and report whether it is retryable.

    Parameters
    ----------
    logger
        Logger used to report the error.
    e
        The exception raised by the launch request.

    Returns
    -------
    `bool`
        `True` if the error is retryable, `False` otherwise.
    """
    extra_fields: dict[str, Any] = {}
    if isinstance(e, HttpError):
        # Errors returned by the Dataflow API, by HTTP status code.
        # Retryable:
        # - 429 Too Many Requests
        # - 5xx server errors, such as 500 Internal Server Error and
        #   503 Service Unavailable
        # Permanent:
        # - All other 4xx errors, such as 400 Bad Request, 403 Forbidden and
        #   404 Not Found
        retryable = e.resp.status == 429 or e.resp.status >= 500
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
        # Transient failures while sending the launch request: other Google
        # API errors, connection resets, TLS errors, truncated responses, DNS
        # lookup failures, and credential-refresh transport errors.
        retryable = True
    else:
        # Any other error is not retryable.
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


def handle_database_error(logger: CloudEventLogger, e: sqlalchemy.exc.SQLAlchemyError) -> bool:
    """Log a failed database operation and report whether it is retryable.

    Parameters
    ----------
    logger
        Logger used to report the error.
    e
        The exception raised by the failed database operation.

    Returns
    -------
    `bool`
        `True` if the error is retryable, `False` otherwise.
    """
    # Transient errors: operational errors (e.g. dropped connections, statement
    # timeouts, and deadlocks), connection pool timeouts, disconnects, and any
    # error that invalidated the connection. Other errors, e.g. integrity or
    # programming errors, are not retryable.
    if isinstance(
        e,
        (
            sqlalchemy.exc.OperationalError,
            sqlalchemy.exc.TimeoutError,
            sqlalchemy.exc.DisconnectionError,
        ),
    ) or (isinstance(e, sqlalchemy.exc.DBAPIError) and e.connection_invalidated):
        logger.log_event(
            logging.WARNING,
            "Retryable database error",
            "retryable_database_error",
            error=e,
        )
        return True

    logger.log_event(
        logging.ERROR,
        "Non-retryable database error",
        "non_retryable_database_error",
        error=e,
    )
    return False
