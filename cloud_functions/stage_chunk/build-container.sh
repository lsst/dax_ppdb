#!/usr/bin/env bash

set -e -x

if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS is not set. Please set it to your service account key file."
  exit 1
fi

if [ -z "$PROJECT_ID" ]; then
  echo "PROJECT_ID is not set. Please set it to your Google Cloud project ID."
  exit 1
fi

gcloud builds submit --tag "gcr.io/${PROJECT_ID}/stage-chunk-image" .
