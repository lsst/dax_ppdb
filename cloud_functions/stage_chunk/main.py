import functions_framework
import google.auth
from googleapiclient.discovery import build
import os
import logging


# Helper function to require environment variables
def require_env(var_name):
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


@functions_framework.cloud_event
def trigger_stage_chunk(cloud_event):
    """Triggered by a finalized object in GCS."""
    data = cloud_event.data

    bucket = data.get("bucket")
    name = data.get("name")  # object path inside bucket

    if not name.endswith(".ready"):
        logging.info(f"Ignoring non-.ready file: {name}")
        return  # Ignore anything that's not a .ready file

    # Extract parent directory path
    input_path = f"gs://{bucket}/" + os.path.dirname(name)

    credentials, _ = google.auth.default()
    dataflow = build("dataflow", "v1b3", credentials=credentials)

    # Generate a job name from the chunk ID
    chunk_id = os.path.basename(os.path.dirname(name))
    job_name = f"stage-chunk-{chunk_id}"

    launch_body = {
        "launchParameter": {
            "jobName": job_name,
            "containerSpecGcsPath": DATAFLOW_TEMPLATE_PATH,
            "parameters": {
                "input_path": input_path,
                "dataset_id": DATASET_ID,
            },
            "environment": {"serviceAccountEmail": SERVICE_ACCOUNT_EMAIL, "tempLocation": TEMP_LOCATION},
        }
    }

    logging.info(f"Launching Dataflow job {job_name} for input path {input_path}")

    request = (
        dataflow.projects()
        .locations()
        .flexTemplates()
        .launch(projectId=PROJECT_ID, location=REGION, body=launch_body)
    )
    response = request.execute()

    logging.info(f"Dataflow launch response: {response}")
