#!/usr/bin/env bash

set -euxo pipefail

if [ -z "${GOOGLE_APPLICATION_CREDENTIALS+x}" ]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS is not set. Please set it to your service account key file."
  exit 1
fi

if [ -z "${PROJECT_ID+x}" ]; then
  echo "PROJECT_ID is not set. Please set it to your Google Cloud project ID."
  exit 1
fi

if [ -z "${REGION+x}" ]; then
  echo "REGION is not set. Please set it to your Google Cloud region."
  exit 1
fi

if [ -z "${BUCKET+x}" ]; then
  echo "BUCKET is not set. Please set it to your Google Cloud Storage bucket name."
  exit 1
fi

echo "Teardown started..."
echo "Project ID: ${PROJECT_ID}"
echo "Bucket: ${BUCKET}"
echo "Region: ${REGION}"

# Delete Cloud Function
gcloud functions delete trigger_stage_chunk --region=${REGION} --quiet || true

# Delete Flex Template JSON
gsutil rm -f gs://${BUCKET}/templates/stage_chunk_flex_template.json || true

# Delete Container Image (optional)
gcloud container images delete "gcr.io/${PROJECT_ID}/stage-chunk-image" --quiet --force-delete-tags || true

echo "Teardown complete."