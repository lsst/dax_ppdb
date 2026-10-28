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

"""Logging and classification of Dataflow job launch and database errors as
retryable or non-retryable.
"""

import http.client
import logging
import ssl
from typing import Any

import httplib2
import sqlalchemy.exc
from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import TransportError
from googleapiclient.errors import HttpError

from .cloud_event_logger import CloudEventLogger

__all__ = ["handle_database_error", "handle_dataflow_launch_error"]


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
