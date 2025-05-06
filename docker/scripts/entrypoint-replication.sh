#!/usr/bin/env bash

set -euo pipefail

# Check if the command is found
command -v ppdb-replication >/dev/null 2>&1 || { echo "ppdb-replication command not found"; exit 1; }
echo "Found ppdb-replication command"

ppdb-replication "$@"
