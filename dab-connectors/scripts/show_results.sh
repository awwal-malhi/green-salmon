#!/usr/bin/env bash
# Phase B helper: print row counts of the target, quarantine and runs tables, and the runs table
# itself, using a SQL warehouse through the Statement Execution API. Stops the warehouse at the end.
#   source .env.databricks && scripts/show_results.sh
# Uses WAREHOUSE_ID if set, else the 2X-Small serverless warehouse "dab-connectors-results"
# (auto-stop 5 minutes), created on first use. Other warehouses (e.g. the larger "Serverless
# Starter Warehouse") are not used.
set -euo pipefail
cd "$(dirname "$0")/.."
catalog="${BUNDLE_VAR_catalog:?source .env.databricks first (or set BUNDLE_VAR_catalog)}"
schema="${SCHEMA:-dab_connectors}"
fq="${catalog}.${schema}"

warehouse="${WAREHOUSE_ID:-$(databricks warehouses list -o json \
  | jq -r '[.[] | select(.name == "dab-connectors-results")][0].id // empty')}"
if [[ -z "$warehouse" ]]; then
  warehouse="$(databricks warehouses create --json '{"name": "dab-connectors-results",
    "cluster_size": "2X-Small", "max_num_clusters": 1, "auto_stop_mins": 5,
    "enable_serverless_compute": true, "warehouse_type": "PRO",
    "tags": {"custom_tags": [{"key": "workstream", "value": "dab-connectors"}]}}' --no-wait -o json | jq -r .id)"
fi
trap 'databricks warehouses stop "$warehouse" --no-wait >/dev/null 2>&1 || true' EXIT

sql() {
  local body resp id state
  body="$(jq -n --arg w "$warehouse" --arg s "$1" '{warehouse_id: $w, statement: $s, wait_timeout: "50s",
    on_wait_timeout: "CONTINUE", format: "JSON_ARRAY", disposition: "INLINE"}')"
  resp="$(databricks api post /api/2.0/sql/statements --json "$body")"
  id="$(jq -r .statement_id <<<"$resp")"
  state="$(jq -r .status.state <<<"$resp")"
  while [[ "$state" == "PENDING" || "$state" == "RUNNING" ]]; do
    sleep 5
    resp="$(databricks api get "/api/2.0/sql/statements/$id")"
    state="$(jq -r .status.state <<<"$resp")"
  done
  [[ "$state" == "SUCCEEDED" ]] || { jq . <<<"$resp" >&2; exit 1; }
  { jq -r '[.manifest.schema.columns[].name] | @tsv' <<<"$resp"
    jq -r '.result.data_array[]? | map(. // "null") | @tsv' <<<"$resp"; } | column -t -s $'\t'
  echo
}

counts=""
for t in orders_csv_v1 orders_csv_v2 customers_pipe; do
  counts+="SELECT '$t' AS table_name, 'target' AS kind, count(*) AS row_count FROM $fq.$t UNION ALL "
  counts+="SELECT '${t}_quarantine', 'quarantine', count(*) FROM $fq.${t}_quarantine UNION ALL "
done
counts+="SELECT 'connector_runs', 'runs', count(*) FROM $fq.connector_runs"

echo "== Row counts in $fq"
sql "$counts"
echo "== $fq.connector_runs"
sql "SELECT run_id, plugin_name, plugin_version, plugin_kind, core_version, status, rows_read, rows_written,
       rows_quarantined, started_at FROM $fq.connector_runs ORDER BY started_at"
echo "== Quarantine reasons"
for t in orders_csv_v1 orders_csv_v2 customers_pipe; do
  sql "SELECT '${t}_quarantine' AS table_name, _plugin_version, array_join(_error_reasons, ',') AS reasons, count(*) AS n
       FROM $fq.${t}_quarantine GROUP BY ALL ORDER BY reasons"
done
