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

from logging import Logger
from typing import Any


class CloudEventLogger:
    """Emits structured log events to Cloud Logging, correlated by a cloud
    event ID.

    Parameters
    ----------
    logger : Logger
        The underlying logger instance to use for emitting events.
    cloud_event_id : str | None
        An optional identifier for the cloud event, included in every log entry
        if provided.
    log_fields : dict[str, Any]
        Fields to include in every log entry (e.g. ``event_id``). This dict
        is held by reference, so mutating it (e.g. via ``.update(...)``)
        changes what's included in subsequent calls to `log_event`.
    """

    def __init__(
        self,
        logger: Logger,
        cloud_event_id: str | None = None,
        log_fields: dict[str, Any] | None = None,
    ):
        self._logger = logger
        self._cloud_event_id = cloud_event_id
        self._log_fields = log_fields or {}

    def log_event(
        self,
        level: int,
        message: str,
        event_name: str,
        *,
        error: BaseException | None = None,
        **fields: Any,
    ) -> None:
        """Emit a structured log entry under Cloud Logging ``json_fields``."""
        json_fields = {"event": event_name, **self._log_fields, **fields}
        if error is not None:
            json_fields["error"] = str(error)
            json_fields["error_type"] = type(error).__name__
        if self._cloud_event_id is not None:
            json_fields["cloud_event_id"] = self._cloud_event_id
        self._logger.log(
            level,
            message,
            exc_info=error,
            stacklevel=2,
            extra={"json_fields": json_fields},
        )
