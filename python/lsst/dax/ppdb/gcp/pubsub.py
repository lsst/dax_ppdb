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

"""Publisher for sending JSON messages to a Google Cloud Pub/Sub topic."""

__all__ = ["Publisher"]

import json
import logging

from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future

_LOG = logging.getLogger(__name__)


class Publisher:
    """Publisher for a Google Cloud Pub/Sub topic.

    Parameters
    ----------
    project_id
        The Google Cloud project ID where the Pub/Sub topic is located.
    topic_name
        The name of the Pub/Sub topic to which messages will be published.
    """

    def __init__(self, project_id: str, topic_name: str):
        self.publisher = PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)

    def publish(self, message: dict) -> Future:
        """Publish a message to the topic.

        Parameters
        ----------
        message
            The message to publish, which will be converted to JSON.

        Returns
        -------
        ``google.cloud.pubsub_v1.publisher.futures.Future``
            Future that resolves to the published message ID.
        """
        try:
            return self.publisher.publish(self.topic_path, json.dumps(message).encode("utf-8"))
        except Exception:
            _LOG.exception("Failed to publish message to topic %s: %s", self.topic_path, message)
            raise
