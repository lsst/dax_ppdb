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

import google.auth
from googleapiclient.discovery import build
import base64
import json
import os
import logging

logging.basicConfig(level=logging.INFO)


# Helper function to require environment variables
def require_env(var_name: str) -> str:
    """Require an environment variable to be set.

    Parameters
    ----------
    var_name : `str`
        The name of the environment variable to check.

    Returns
    -------
    str
        The value of the environment variable.
    """
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


def trigger_stage_chunk(event, context):
    """Triggered by a Pub/Sub message."""
    try:
        message = base64.b64decode(event["data"]).decode("utf-8")
        data = json.loads(message)
    except Exception:
        logging.exception("Failed to decode Pub/Sub message")
        raise

    bucket = data.get("bucket")
    name = data.get("name")

    input_path = f"gs://{bucket}/" + name

    credentials, _ = google.auth.default()
    dataflow = build("dataflow", "v1b3", credentials=credentials)

    chunk_id = os.path.basename(name)
    job_name = f"stage-chunk-{chunk_id}"

    launch_body = {
        "launchParameter": {
            "jobName": job_name,
            "containerSpecGcsPath": DATAFLOW_TEMPLATE_PATH,
            "parameters": {
                "input_path": input_path,
                "dataset_id": DATASET_ID,
            },
            "environment": {
                "serviceAccountEmail": SERVICE_ACCOUNT_EMAIL,
                "tempLocation": TEMP_LOCATION,
            },
        }
    }

    logging.info(f"Launching Dataflow job {job_name} for input path {input_path}")

    request = (
        dataflow.projects()
        .locations()
        .flexTemplates()
        .launch(projectId=PROJECT_ID, location=REGION, body=launch_body)
    )
    try:
        response = request.execute()
    except Exception:
        logging.exception("Failed to launch Dataflow job")
        raise

    logging.info(f"Dataflow launch response: {response}")
