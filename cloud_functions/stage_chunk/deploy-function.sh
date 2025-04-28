#!/usr/bin/env bash

set -euxo pipefail

if [ -z "$GOOGLE_APPLICATION_CREDENTIALS+x" ]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS is not set. Please set it to your service account key file."
  exit 1
fi

if [ -z "$PROJECT_ID+x" ]; then
  echo "PROJECT_ID is not set. Please set it to your Google Cloud project ID."
  exit 1
fi

if [ -z "$REGION+x" ]; then
  echo "REGION is not set. Please set it to your Google Cloud region."
  exit 1
fi

if [ -z "$DATAFLOW_TEMPLATE_PATH+x" ]; then
  echo "DATAFLOW_TEMPLATE_PATH is not set. Please set it to your Dataflow template path."
  exit 1
fi

if [ -z "$SERVICE_ACCOUNT_EMAIL+x" ]; then
  echo "SERVICE_ACCOUNT_EMAIL is not set. Please set it to your Google Cloud service account email."
  exit 1
fi

if [ -z "$DATASET_ID+x" ]; then
  echo "DATASET_ID is not set. Please set it to your Google Cloud dataset ID."
  exit 1
fi

if [ -z "$TEMP_LOCATION+x" ]; then
  echo "TEMP_LOCATION is not set. Please set it to your Google Cloud temporary location."
  exit 1
fi

# Deploy the Cloud Function
gcloud functions deploy trigger_stage_chunk \
  --runtime=python311 \
  --region=${REGION} \
  --source=. \
  --entry-point=trigger_stage_chunk \
  --service-account=${SERVICE_ACCOUNT_EMAIL} \
  --trigger-topic=stage-chunk-topic \
  --set-env-vars "PROJECT_ID=${PROJECT_ID},REGION=${REGION},SERVICE_ACCOUNT_EMAIL=${SERVICE_ACCOUNT_EMAIL},DATASET_ID=${DATASET_ID},TEMP_LOCATION=${TEMP_LOCATION},DATAFLOW_TEMPLATE_PATH=${DATAFLOW_TEMPLATE_PATH}" \
  --gen2
