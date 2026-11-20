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

"""Logging and classification of database errors as retryable or
non-retryable.
"""

__all__ = ["DATABASE_ERRORS", "NETWORK_ERRORS", "handle_database_error"]

import logging

import aiohttp
import requests
import sqlalchemy.exc
from google.api_core.exceptions import RetryError
from google.auth.exceptions import GoogleAuthError, TransportError
from google.cloud.sql.connector.exceptions import (
    AutoIAMAuthNotSupported,
    CacheClosedError,
    ClosedConnectorError,
    CloudSQLIPTypeError,
    DnsResolutionError,
    IncompatibleDriverError,
    RefreshNotValidError,
)

from .cloud_event_logger import CloudEventLogger

# Errors from a database operation. Besides SQLAlchemy's own errors, opening a
# Cloud SQL connection through the Cloud SQL Python Connector can raise
# authentication, HTTP and network errors and the connector's own errors, which
# SQLAlchemy doesn't wrap.
DATABASE_ERRORS = (
    sqlalchemy.exc.SQLAlchemyError,
    GoogleAuthError,
    aiohttp.ClientError,
    OSError,
    AutoIAMAuthNotSupported,
    CacheClosedError,
    ClosedConnectorError,
    CloudSQLIPTypeError,
    DnsResolutionError,
    IncompatibleDriverError,
    RefreshNotValidError,
)

# Network errors, or the client library running out of its own retries.
NETWORK_ERRORS = (
    RetryError,
    ConnectionError,
    TimeoutError,
    TransportError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


def handle_database_error(logger: CloudEventLogger, e: Exception) -> bool:
    """Log a failed database operation and report whether it is retryable.

    Parameters
    ----------
    logger
        Logger used to report the error.
    e
        The exception raised by the failed database operation, one of
        `DATABASE_ERRORS`.

    Returns
    -------
    `bool`
        `True` if the error is retryable, `False` otherwise.
    """
    if isinstance(
        e,
        (
            sqlalchemy.exc.OperationalError,
            sqlalchemy.exc.TimeoutError,
            sqlalchemy.exc.DisconnectionError,
        ),
    ) or (isinstance(e, sqlalchemy.exc.DBAPIError) and e.connection_invalidated):
        # Operational errors (e.g. dropped connections, statement timeouts and
        # deadlocks), connection pool timeouts, disconnects, and any error
        # that invalidated the connection.
        retryable = True
    elif isinstance(e, (OSError, TransportError, aiohttp.ClientConnectionError)):
        # Network and TLS errors while opening a Cloud SQL connection, and
        # failures to reach the token endpoint.
        retryable = True
    elif isinstance(e, (DnsResolutionError, RefreshNotValidError)):
        # Temporary Cloud SQL Connector errors: a failed DNS lookup of the
        # instance, or a connection info refresh that returned an invalid
        # result.
        retryable = True
    elif isinstance(e, GoogleAuthError) and e.retryable:
        # Authentication errors marked as temporary, e.g. a credential refresh
        # that got a 5xx from the token endpoint.
        retryable = True
    elif isinstance(e, aiohttp.ClientResponseError):
        # Responses from the Cloud SQL Admin API: 429 and 5xx are retryable;
        # other 4xx, e.g. missing permissions or instance, are not.
        retryable = e.status == 429 or 500 <= e.status < 600
    else:
        # Any other error, e.g. integrity or programming errors, a credential
        # refresh that isn't marked as retryable, or a Cloud SQL Connector
        # configuration error or closed connector, is not retryable.
        retryable = False

    if retryable:
        logger.log_event(logging.WARNING, "Retryable database error", "retryable_database_error", error=e)
    else:
        logger.log_event(
            logging.ERROR, "Non-retryable database error", "non_retryable_database_error", error=e
        )
    return retryable
