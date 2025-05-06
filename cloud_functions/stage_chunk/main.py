# This file is part of dax_ppdb
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
import json
import logging
import os
import posixpath
from datetime import datetime, timezone

import google.auth
from google.api_core.exceptions import GoogleAPICallError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Helper function to require environment variables
def require_env(var_name: str) -> str:
    """Require an environment variable to be set."""
    value = os.getenv(var_name)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {var_name}")
    return value


# Read required environment variables
PROJECT_ID = require_env("PROJECT_ID")
DATAFLOW_TEMPLATE_PATH = require_env("DATAFLOW_TEMPLATE_PATH")
REGION = require_env("REGION")
SERVICE_ACCOUNT_EMAIL = require_env("SERVICE_ACCOUNT_EMAIL")
DATASET_ID = require_env("DATASET_ID")
TEMP_LOCATION = require_env("TEMP_LOCATION")

_credentials, _ = google.auth.default()
_dataflow_client = build("dataflow", "v1b3", credentials=_credentials)


def trigger_stage_chunk(event, context):
    """
    Cloud Function to launch a Dataflow job to stage PPDB data.

    Parameters
    ----------
    event : dict
        The dictionary with data specific to this type of event. The `data`
        field contains a base64-encoded string representing a JSON message
        with `bucket` and `name` fields.

    context : google.cloud.functions.Context
        Metadata of triggering event including `event_id`.
    """
    try:
        message = base64.b64decode(event["data"]).decode("utf-8")
        data = json.loads(message)
    except Exception:
        logging.exception("Malformed or missing Pub/Sub data payload")
        return

    try:
        bucket = data["bucket"]
        name = data["name"]
    except KeyError:
        logging.exception("Missing required key in Pub/Sub message")
        return

    input_path = f"gs://{bucket}/{name}"
    chunk_id = posixpath.basename(name)
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")
    job_name = f"stage-chunk-{chunk_id}-{timestamp}"

    launch_body = {
        "launchParameter": {
            "jobName": job_name,
            "containerSpecGcsPath": DATAFLOW_TEMPLATE_PATH,
            "parameters": {"input_path": input_path, "dataset_id": DATASET_ID},
            "environment": {
                "serviceAccountEmail": SERVICE_ACCOUNT_EMAIL,
                "tempLocation": TEMP_LOCATION,
            },
        }
    }

    logging.info(f"Launching Dataflow job {job_name} for input path {input_path}")
    logging.info(f"Triggered by event ID: {context.event_id}")
    logging.info(f"Dataflow launch body: {json.dumps(launch_body, indent=2)}")

    try:
        request = (
            _dataflow_client.projects()
            .locations()
            .flexTemplates()
            .launch(projectId=PROJECT_ID, location=REGION, body=launch_body)
        )
        response = request.execute()

        if "job" not in response:
            logging.error("Dataflow API response missing 'job' field: %s", response)
            return

        job_id = response.get("job", {}).get("id", "unknown")
        logging.info(f"Dataflow job launched: {job_id}")

    except HttpError as e:
        if e.resp.status in [429, 500, 503]:
            logging.warning("Retryable HTTP error (%s): %s", e.resp.status, e)
            raise  # Will trigger retry
        else:
            logging.error("Non-retryable HTTP error: %s", e)
            return  # Acknowledge message

    except GoogleAPICallError:
        logging.exception("Retryable GCP API error")
        raise  # Will trigger retry

    except Exception:
        logging.exception("Unexpected error during job submission")
        return  # Acknowledge message

    return
