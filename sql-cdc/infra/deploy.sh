#!/usr/bin/env bash
# Create (or update in place) every Azure resource for the SQL CDC POC, then write
# connection settings for the Python code to sql-cdc/.env.azure (git-ignored, mode 600).
# Safe to re-run: Bicep incremental deployments with fixed names are idempotent.
#
# Required env:  AZURE_RESOURCE_GROUP, SQL_ADMIN_PASSWORD
# Optional env:  AZURE_LOCATION (default canadacentral), NAME_PREFIX (default gssqlcdc),
#                SQL_ADMIN_LOGIN (default cdcadmin), CLIENT_IP (default: detected public IP),
#                WHAT_IF=1 (preview changes only, deploy nothing)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"

: "${AZURE_RESOURCE_GROUP:?set AZURE_RESOURCE_GROUP}"
: "${SQL_ADMIN_PASSWORD:?set SQL_ADMIN_PASSWORD}"
location="${AZURE_LOCATION:-canadacentral}"
prefix="${NAME_PREFIX:-gssqlcdc}"
admin_login="${SQL_ADMIN_LOGIN:-cdcadmin}"
deployment_name="sql-cdc"

if [[ -z "${CLIENT_IP:-}" ]]; then
  CLIENT_IP="$(curl -fsS --max-time 10 https://api.ipify.org || true)"
  [[ -n "$CLIENT_IP" ]] || { echo "could not detect public IP; set CLIENT_IP" >&2; exit 1; }
fi

az account show --query "{subscription:name, user:user.name}" -o table
az group show --name "$AZURE_RESOURCE_GROUP" --query "{resourceGroup:name, location:location}" -o table

# The password goes in a private temp parameters file, not on the command line,
# so it does not show up in the process list.
secret_params="$(umask 077 && mktemp)"
trap 'rm -f "$secret_params"' EXIT
SECRET="$SQL_ADMIN_PASSWORD" python3 -c 'import json,os,sys; json.dump({"sqlAdminPassword":{"value":os.environ["SECRET"]}}, open(sys.argv[1],"w"))' "$secret_params"

params=(
  --resource-group "$AZURE_RESOURCE_GROUP"
  --template-file "$here/main.bicep"
  --parameters "@$secret_params"
  --parameters location="$location" namePrefix="$prefix" sqlAdminLogin="$admin_login"
               clientIpAddress="$CLIENT_IP"
)

if [[ "${WHAT_IF:-0}" == "1" ]]; then
  az deployment group what-if "${params[@]}"
  exit 0
fi

echo "Deploying (mode Incremental) to $AZURE_RESOURCE_GROUP in $location ..."
outputs="$(az deployment group create --name "$deployment_name" --mode Incremental "${params[@]}" \
  --query properties.outputs -o json)"

out() { python3 -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]]["value"])' "$1" <<<"$outputs"; }
sql_fqdn="$(out sqlServerFqdn)"
sql_db="$(out sqlDatabaseName)"
sb_ns="$(out serviceBusNamespaceName)"
queue="$(out queueName)"

rule_conn() {
  az servicebus queue authorization-rule keys list --resource-group "$AZURE_RESOURCE_GROUP" \
    --namespace-name "$sb_ns" --queue-name "$queue" --name "$1" --query primaryConnectionString -o tsv
}
send_conn="$(rule_conn "$(out publisherRuleName)")"
listen_conn="$(rule_conn "$(out consumerRuleName)")"

env_file="$root/.env.azure"
umask 077
cat > "$env_file" <<ENV
SQL_SERVER=$sql_fqdn
SQL_DATABASE=$sql_db
SQL_USER=$admin_login
SERVICEBUS_QUEUE=$queue
SERVICEBUS_SEND_CONNECTION_STRING=$send_conn
SERVICEBUS_LISTEN_CONNECTION_STRING=$listen_conn
ENV
echo "Wrote $env_file (SQL_PASSWORD is not written; keep it in SQL_ADMIN_PASSWORD / SQL_PASSWORD)."
echo "SQL server: $sql_fqdn  database: $sql_db  Service Bus: $sb_ns/$queue"
