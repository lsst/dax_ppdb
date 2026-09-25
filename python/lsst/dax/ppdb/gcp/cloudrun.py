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

import base64
import binascii
import json
import logging
from typing import Any

from cloudevents.http import CloudEvent

from .cloud_event_logger import CloudEventLogger

__all__ = ["DecodeMessageDataError", "decode_message_data"]


class DecodeMessageDataError(Exception):
    """Raised if the data payload of a Pub/Sub CloudEvent cannot be decoded,
    parsed, or validated.

    Notes
    -----
    The failure is logged by `decode_message_data` before this is raised.
    """


def decode_message_data(logger: CloudEventLogger, event: CloudEvent) -> dict[str, Any]:
    """Decode, parse, and validate the data payload of a Pub/Sub CloudEvent.

    Parameters
    ----------
    logger
        Logger used to report a failure.
    event
        The CloudEvent delivered by the Pub/Sub trigger.

    Returns
    -------
    data
        The decoded JSON message data.

    Raises
    ------
    DecodeMessageDataError
        Raised if decoding, parsing, or validation failed.
    """
    try:
        message = base64.b64decode(event.data["message"]["data"]).decode("utf-8")
    except (KeyError, binascii.Error, UnicodeDecodeError) as e:
        logger.log_event(
            logging.ERROR,
            "Malformed or missing Pub/Sub data payload",
            "malformed_pubsub_payload",
            error=e,
            pubsub_event=event.data,
        )
        raise DecodeMessageDataError from e

    try:
        data = json.loads(message)
    except json.JSONDecodeError as e:
        logger.log_event(
            logging.ERROR,
            "Failed to decode JSON from Pub/Sub message",
            "json_decode_error",
            error=e,
            pubsub_message=message,
        )
        raise DecodeMessageDataError from e

    if not isinstance(data, dict):
        logger.log_event(
            logging.ERROR,
            "Pub/Sub message is not a JSON object",
            "invalid_payload_type",
            pubsub_message=data,
        )
        raise DecodeMessageDataError("Pub/Sub message is not a JSON object") from None

    return data
