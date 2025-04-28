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

if [ -z "$BUCKET+x" ]; then
  echo "BUCKET is not set. Please set it to your Google Cloud Storage bucket name."
  exit 1
fi

echo "Creating Dataflow Flex Template..."
gcloud dataflow flex-template build "gs://${BUCKET}/templates/stage_chunk_flex_template.json" \
  --image "gcr.io/${PROJECT_ID}/stage-chunk-image" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata.json"

echo "Flex Template created and pushed to gs://${BUCKET}/templates/stage_chunk_flex_template.json"