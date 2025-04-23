#!/usr/bin/env bash

set -e -x

# === CONFIGURATION ===
PROJECT_ID="${PROJECT_ID:-ppdb-dev-438721}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-ppdb-storage-manager@${PROJECT_ID}.iam.gserviceaccount.com}"
BUCKET_NAME="${BUCKET_NAME:-rubin-ppdb-test-bucket-1}"
REGION="${REGION:-us-central1}"

# === DEPLOY GEN1 CLOUD FUNCTION ===
gcloud functions deploy ingest \
  --runtime python311 \
  --trigger-resource "${BUCKET_NAME}" \
  --trigger-event google.storage.object.finalize \
  --entry-point ingest \
  --region "${REGION}" \
  --no-gen2 \
  --service-account="${SERVICE_ACCOUNT}" \
  --source=.
