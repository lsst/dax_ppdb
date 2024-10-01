#!/usr/bin/env bash

###############################################################################
# This a wrapper script for the ppdb-replication script, intended to be as the
# entrypoint to a Docker container. Command line configuration is managed by
# environment variables, which are defined in the Phalanx application.
###############################################################################

# Bash "strict mode", to help catch problems and bugs in the shell script.
# Every bash script you write should include this. See
# http://redsymbol.net/articles/unofficial-bash-strict-mode/ for
# details.
set -euo pipefail

# Check if the command is found
command -v ppdb-replication >/dev/null 2>&1 || { echo "ppdb-replication command not found"; exit 1; }
echo "Found ppdb-replication command"

# Function to check if an environment variable is set
check_env_var() {
    local var_name="$1"
    local var_value="${!var_name}"
    if [ -z "${var_value:-}" ]; then
        echo "$var_name is a required environment variable"
        exit 1
    fi
}

# Check if the required environment variables are set
check_env_var "PPDB_REPLICATION_APDB_CONFIG"
check_env_var "PPDB_REPLICATION_PPDB_CONFIG"

# Build the command from the environment variables
_CMD="ppdb-replication"
[ -n "${PPDB_REPLICATION_MON_LOGGER:-}" ] && _CMD="$_CMD --mon-logger $PPDB_REPLICATION_MON_LOGGER"
[ -n "${PPDB_REPLICATION_MON_RULES:-}" ] && _CMD="$_CMD --mon-rules $PPDB_REPLICATION_MON_RULES"
[ -n "${PPDB_REPLICATION_LOG_LEVEL:-}" ] && _CMD="$_CMD -l $PPDB_REPLICATION_LOG_LEVEL"
_CMD="$_CMD run"
[ "${PPDB_REPLICATION_UPDATE_EXISTING:-}" = "true" ] && _CMD="$_CMD --update"
[ -n "${PPDB_REPLICATION_MIN_WAIT_TIME:-}" ] && _CMD="$_CMD --min-wait-time $PPDB_REPLICATION_MIN_WAIT_TIME"
[ -n "${PPDB_REPLICATION_MAX_WAIT_TIME:-}" ] && _CMD="$_CMD --max-wait-time $PPDB_REPLICATION_MAX_WAIT_TIME"
[ -n "${PPDB_REPLICATION_CHECK_INTERVAL:-}" ] && _CMD="$_CMD --check-interval $PPDB_REPLICATION_CHECK_INTERVAL"
_CMD="$_CMD $PPDB_REPLICATION_APDB_CONFIG"
_CMD="$_CMD $PPDB_REPLICATION_PPDB_CONFIG"

# Run the command
echo "Running: $_CMD"
$_CMD
