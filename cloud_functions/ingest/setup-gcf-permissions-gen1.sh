#!/usr/bin/env bash
set -e -x

# === CONFIGURATION ===
PROJECT_ID="${PROJECT_ID:-ppdb-dev-438721}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-ppdb-storage-manager@${PROJECT_ID}.iam.gserviceaccount.com}"
BUCKET_NAME="${BUCKET_NAME:-rubin-ppdb-test-bucket-1}"

# === IAM BINDINGS FOR YOUR SERVICE ACCOUNT ===
echo "Granting roles to $SERVICE_ACCOUNT"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/cloudfunctions.developer"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/iam.serviceAccountUser"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/logging.logWriter"

echo "All required IAM roles granted for Gen 1 Cloud Function deployment."
