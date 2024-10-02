#!/bin/bash

_url=https://github.com/lsst/sdm_schemas.git

# Make sure SDM_SCHEMAS_REF is set with default
if [ -z "$SDM_SCHEMAS_REF" ]; then
    SDM_SCHEMAS_REF=main
fi

echo "Cloning SDM schemas from $_url at $SDM_SCHEMAS_REF"

# Determine if SDM_SCHEMAS_REF is a branch or a tag
if git ls-remote --heads "$_url" "$SDM_SCHEMAS_REF" | grep -q "$SDM_SCHEMAS_REF"; then
    echo "$SDM_SCHEMAS_REF is a branch"
    git clone --depth=1 --branch "$SDM_SCHEMAS_REF" "$_url"
elif git ls-remote --tags "$_url" "$SDM_SCHEMAS_REF" | grep -q "refs/tags/$SDM_SCHEMAS_REF"; then
    echo "$SDM_SCHEMAS_REF is a tag"
    git clone --depth=1 "$_url" && \
        pushd sdm_schemas && \
        git checkout tags/"$SDM_SCHEMAS_REF" && \
        popd
else
    echo "Error: $SDM_SCHEMAS_REF is neither a branch nor a tag."
    exit 1
fi
