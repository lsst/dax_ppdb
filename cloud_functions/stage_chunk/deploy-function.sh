#!/usr/bin/env bash

set -e -x
set -o pipefail

if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS is not set. Please set it to your service account key file."
  exit 1
fi

if [ -z "$PROJECT_ID" ]; then
  echo "PROJECT_ID is not set. Please set it to your Google Cloud project ID."
  exit 1
fi

if [ -z "$BUCKET" ]; then
  echo "BUCKET is not set. Please set it to your Google Cloud Storage bucket name."
  exit 1
fi

if [ -z "$REGION" ]; then
  echo "REGION is not set. Please set it to your Google Cloud region."
  exit 1
fi

if [ -z "$DATAFLOW_TEMPLATE_PATH" ]; then
  echo "DATAFLOW_TEMPLATE_PATH is not set. Please set it to your Dataflow template path."
  exit 1
fi

if [ -z "$SERVICE_ACCOUNT_EMAIL" ]; then
  echo "SERVICE_ACCOUNT_EMAIL is not set. Please set it to your Google Cloud service account email."
  exit 1
fi

if [ -z "$DATASET_ID" ]; then
  echo "DATASET_ID is not set. Please set it to your Google Cloud dataset ID."
  exit 1
fi

if [ -z "$TEMP_LOCATION" ]; then
  echo "TEMP_LOCATION is not set. Please set it to your Google Cloud temporary location."
  exit 1
fi

# Deploy the Cloud Function
gcloud functions deploy trigger_stage_chunk \
  --runtime=python311 \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=${BUCKET}" \
  --set-env-vars "PROJECT_ID=${PROJECT_ID},REGION=${REGION},DATAFLOW_TEMPLATE_PATH=${DATAFLOW_TEMPLATE_PATH},SERVICE_ACCOUNT_EMAIL=${SERVICE_ACCOUNT_EMAIL},DATASET_ID=${DATASET_ID},TEMP_LOCATION=${TEMP_LOCATION}" \
  --region=${REGION} \
  --source=. \
  --entry-point=trigger_stage_chunk \
  --service-account=${SERVICE_ACCOUNT_EMAIL}
