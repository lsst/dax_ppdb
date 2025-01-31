#!/usr/bin/env bash

##############################################################################
# Build the PPDB Docker images - run this script from the root of the project.
#
# This should only be used for local development. The production images are
# built and pushed to the container registry from a GitHub Action.
##############################################################################

set -e

# Build the base image
docker build -t lsst/ppdb-base:local -f docker/Dockerfile.base .

# Build the CLI image
docker build --build-arg BASE_IMAGE=lsst/ppdb-base:local -t lsst/ppdb-cli:local -f docker/Dockerfile.cli .

# Build the replication image
docker build --build-arg BASE_IMAGE=lsst/ppdb-base:local -t lsst/ppdb-replication:local -f docker/Dockerfile.replication .
