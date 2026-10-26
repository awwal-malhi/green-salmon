#!/usr/bin/env bash
# Run the end-to-end test with one command.
#
#   scripts/e2e.sh local   # Docker: SQL Server 2022 (CDC) + Service Bus emulator; no Azure needed
#   scripts/e2e.sh azure   # against the resources from infra/deploy.sh (.env.azure + SQL_ADMIN_PASSWORD)
#
# Set KEEP=1 with "local" to leave the containers running afterwards.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
target="${1:-local}"
py="${PYTHON:-$root/.venv/bin/python}"
if [[ ! -x "$py" ]]; then
  python3 -m venv "$root/.venv"
  "$root/.venv/bin/pip" install -q -r requirements.txt
  py="$root/.venv/bin/python"
fi

case "$target" in
  local)
    export LOCAL_SA_PASSWORD="${LOCAL_SA_PASSWORD:-Local-$(openssl rand -hex 12)-Aa1}"
    compose=(docker compose -f "$root/local/docker-compose.yml")
    cleanup() { [[ "${KEEP:-0}" == "1" ]] || "${compose[@]}" down -v >/dev/null 2>&1 || true; }
    trap cleanup EXIT
    "${compose[@]}" up -d
    export SQL_SERVER="localhost,14330" SQL_DATABASE="cdcdemo" SQL_USER="sa" SQL_PASSWORD="$LOCAL_SA_PASSWORD"
    export SQL_TRUST_SERVER_CERTIFICATE="yes"
    export SERVICEBUS_QUEUE="cdc-changes"
    export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
    echo "Waiting for SQL Server and the Service Bus emulator ..."
    for _ in $(seq 1 60); do
      if "$py" - <<'PY' 2>/dev/null; then break; fi
import os, dataclasses
from cdc_pipeline import db
from cdc_pipeline.config import SqlSettings
db.connect(dataclasses.replace(SqlSettings.from_env(), database="master"), attempts=1).close()
PY
      sleep 5
    done
    for _ in $(seq 1 60); do
      if "${compose[@]}" logs servicebus 2>/dev/null | grep -q "Emulator Service is Successfully Up"; then break; fi
      sleep 5
    done
    "$py" -m cdc_pipeline.setup_db --create-database
    ;;
  azure)
    [[ -f "$root/.env.azure" ]] || { echo "run infra/deploy.sh first" >&2; exit 1; }
    set -a
    # shellcheck source=/dev/null
    source "$root/.env.azure"
    set +a
    : "${SQL_PASSWORD:=${SQL_ADMIN_PASSWORD:?set SQL_ADMIN_PASSWORD}}"
    export SQL_PASSWORD
    "$py" -m cdc_pipeline.setup_db
    ;;
  *)
    echo "usage: $0 local|azure" >&2; exit 2 ;;
esac

"$py" -m pytest -v -m e2e tests
