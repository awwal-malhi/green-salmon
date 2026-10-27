#!/usr/bin/env bash
# Builds the Java modules and stages everything the bundle deploys:
#   build/libs/                         core + plugin API + shared modules (installed on the cluster)
#   build/plugins/<name>/<version>/     one plugin artifact per version (.jar or .py), loaded at run time
# Usage: scripts/build.sh [--with-tests]
set -euo pipefail
cd "$(dirname "$0")/.."

skip_tests="-DskipTests"
if [[ "${1:-}" == "--with-tests" ]]; then
  skip_tests=""
fi
mvn -B -q -f java/pom.xml install ${skip_tests}

rm -rf build/libs build/plugins
mkdir -p build/libs build/plugins
cp java/plugin-api/target/connector-plugin-api-*.jar \
   java/shared-logging/target/shared-logging-*.jar \
   java/shared-notifications/target/shared-notifications-*.jar \
   java/connector-core/target/connector-core-*.jar \
   build/libs/

# Java plugins: artifact <name>-plugin-<version>.jar -> build/plugins/<name>/<version>/
for jar in java/plugins/*/target/*-plugin-*.jar; do
  file="$(basename "$jar")"
  if [[ ! "$file" =~ ^(.+)-plugin-([0-9][0-9A-Za-z.-]*)\.jar$ ]]; then
    echo "Unexpected plugin JAR name: $file" >&2
    exit 1
  fi
  dest="build/plugins/${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
  mkdir -p "$dest"
  cp "$jar" "$dest/"
done

# Python plugins: python/plugins/<name>/<version>/<file>.py -> build/plugins/<name>/<version>/
for dir in python/plugins/*/*/; do
  dir="${dir%/}"
  version="$(basename "$dir")"
  name="$(basename "$(dirname "$dir")")"
  dest="build/plugins/$name/$version"
  mkdir -p "$dest"
  cp "$dir"/*.py "$dest/"
done

echo "Staged:"
find build/libs build/plugins -type f | sort
