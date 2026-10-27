#!/usr/bin/env bash
# Phase B helper, run after `databricks bundle deploy` (which runs scripts/build.sh): copy the plugin
# artifacts in build/plugins/<name>/<version>/ to the artifacts volume, where the jobs' --plugin-root
# points. They cannot be loaded from the bundle's /Workspace files: the JVM on dedicated (single user)
# Unity Catalog clusters cannot read workspace files, so the Java core would not find plugin JARs there.
#   scripts/publish_plugins.sh <catalog> [artifacts_schema] [target]
# Uses whatever Databricks CLI auth is configured; nothing is stored here.
set -euo pipefail
cd "$(dirname "$0")/.."
catalog="${1:?usage: $0 <catalog> [artifacts_schema] [target]}"
artifacts_schema="${2:-dab_connectors_artifacts}"
target="${3:-dev}"
[[ -d build/plugins ]] || { echo "build/plugins missing; run scripts/build.sh (or databricks bundle deploy) first" >&2; exit 1; }
dest="dbfs:/Volumes/${catalog}/${artifacts_schema}/artifacts/dab-connectors/${target}/plugins"
databricks fs rm -r "$dest" >/dev/null 2>&1 || true
databricks fs cp -r build/plugins "$dest"
databricks fs ls -l "$dest"
