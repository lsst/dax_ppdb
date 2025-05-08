#!/usr/bin/env bash

set -euxo pipefail

if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS is unset or empty. Please set it to your service account key file."
  exit 1
fi

if [ -z "${GCP_PROJECT:-}" ]; then
  echo "GCP_PROJECT is unset or empty. Please set it to your Google Cloud project ID."
  exit 1
fi

gcloud builds submit --tag "gcr.io/${GCP_PROJECT}/stage-chunk-image" .
