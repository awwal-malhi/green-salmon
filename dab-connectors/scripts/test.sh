#!/usr/bin/env bash
# Runs every local test (no Databricks needed): Java unit tests, then the Python tests, which run
# the real plugin artifacts through the Java core in Spark local mode.
# Needs: JDK 17 (or 8/11), Maven 3.6+, Python 3.10+ with python/requirements-dev.txt installed.
set -euo pipefail
cd "$(dirname "$0")/.."

scripts/build.sh --with-tests

# Delta Lake JARs for the Python tests (Databricks Runtime has Delta built in).
mkdir -p build/test-libs
for artifact in io.delta:delta-spark_2.12:3.2.0 io.delta:delta-storage:3.2.0; do
  mvn -B -q org.apache.maven.plugins:maven-dependency-plugin:3.6.1:copy \
    -Dartifact="$artifact" -DoutputDirectory=build/test-libs
done

python -m pytest python/tests "$@"

# Offline bundle schema check, if the Databricks CLI is installed (no workspace or login needed).
if command -v databricks >/dev/null 2>&1; then
  databricks bundle schema > build/bundle-schema.json
  python scripts/validate_bundle.py build/bundle-schema.json
else
  echo "databricks CLI not found: skipping offline bundle schema check"
fi
