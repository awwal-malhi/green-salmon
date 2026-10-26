#!/usr/bin/env bash
# Phase B helper: copy the synthetic sample files into the bundle's volume after `databricks bundle deploy`.
#   scripts/upload_sample_data.sh <catalog> [schema] [volume]
# Uses whatever Databricks CLI auth is configured (profile or DATABRICKS_HOST/DATABRICKS_TOKEN env); nothing is stored here.
set -euo pipefail
cd "$(dirname "$0")/.."
catalog="${1:?usage: $0 <catalog> [schema] [volume]}"
schema="${2:-dab_connectors}"
volume="${3:-files}"
databricks fs cp -r --overwrite sample-data "dbfs:/Volumes/${catalog}/${schema}/${volume}/sample-data"
databricks fs mkdir "dbfs:/Volumes/${catalog}/${schema}/${volume}/notifications"
