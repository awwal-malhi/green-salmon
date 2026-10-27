#!/usr/bin/env bash
# Create (or update in place) the Azure Databricks workspace for the dab-connectors POC, check that
# it is ready for the bundle (Unity Catalog metastore, catalog, permissions), then write the
# non-secret settings the Databricks CLI needs to dab-connectors/.env.databricks (git-ignored).
# Safe to re-run: Bicep incremental deployments with fixed names are idempotent.
#
# Auth: the Azure CLI must be logged in (az login, as a user or service principal). The
# Databricks CLI reuses that login (DATABRICKS_AUTH_TYPE=azure-cli), so no Databricks token or
# secret is created or stored. An identity with Contributor on the workspace becomes a workspace
# admin on first sign-in.
#
# Required env:  AZURE_RESOURCE_GROUP
# Optional env:  AZURE_LOCATION (default canadacentral), NAME_PREFIX (default gsdabconn),
#                CATALOG (default: the workspace catalog Azure Databricks creates),
#                ARTIFACTS_SCHEMA (default dab_connectors_artifacts),
#                WHAT_IF=1 (preview changes only, deploy nothing)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"

: "${AZURE_RESOURCE_GROUP:?set AZURE_RESOURCE_GROUP}"
location="${AZURE_LOCATION:-canadacentral}"
prefix="${NAME_PREFIX:-gsdabconn}"
deployment_name="dab-connectors"

az account show --query "{subscription:name, user:user.name}" -o table
az group show --name "$AZURE_RESOURCE_GROUP" --query "{resourceGroup:name, location:location}" -o table

params=(
  --resource-group "$AZURE_RESOURCE_GROUP"
  --template-file "$here/main.bicep"
  --parameters location="$location" namePrefix="$prefix"
)

if [[ "${WHAT_IF:-0}" == "1" ]]; then
  az deployment group what-if "${params[@]}"
  exit 0
fi

echo "Deploying (mode Incremental) to $AZURE_RESOURCE_GROUP in $location (a new workspace takes a few minutes) ..."
outputs="$(az deployment group create --name "$deployment_name" --mode Incremental "${params[@]}" \
  --query properties.outputs -o json)"

out() { python3 -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]]["value"])' "$1" <<<"$outputs"; }
workspace_name="$(out workspaceName)"
workspace_id="$(out workspaceId)"
host="https://$(out workspaceUrl)"

export DATABRICKS_HOST="$host" DATABRICKS_AUTH_TYPE=azure-cli DATABRICKS_AZURE_RESOURCE_ID="$workspace_id"

# The first request signs the caller in (and provisions it as workspace admin); a new
# workspace can take a minute or two before it accepts requests.
for attempt in $(seq 1 20); do
  if me="$(databricks current-user me -o json 2>/dev/null)"; then break; fi
  [[ "$attempt" -lt 20 ]] || { echo "workspace $host did not accept requests" >&2; exit 1; }
  sleep 15
done
echo "Signed in to $host as $(jq -r .userName <<<"$me")"

# The bundle creates a schema and a volume, so the workspace needs a Unity Catalog metastore.
# New workspaces are attached to the region's metastore automatically; this script cannot do it
# (that needs a Databricks account admin).
for attempt in $(seq 1 20); do
  if metastore="$(databricks metastores current -o json 2>/dev/null)"; then break; fi
  [[ "$attempt" -lt 20 ]] || {
    echo "No Unity Catalog metastore is assigned to $workspace_name. A Databricks account admin must" >&2
    echo "assign one (account console > Catalog) before the bundle can be deployed." >&2
    exit 1
  }
  sleep 15
done
echo "Unity Catalog metastore: $(jq -r .metastore_id <<<"$metastore")"

catalog="${CATALOG:-${workspace_name//-/_}}"
databricks catalogs get "$catalog" -o json >/dev/null || {
  echo "Catalog $catalog not found. Set CATALOG to an existing catalog you can create schemas in." >&2
  exit 1
}
echo "Effective privileges on catalog $catalog for $(jq -r .userName <<<"$me"):"
databricks grants get-effective catalog "$catalog" --principal "$(jq -r .userName <<<"$me")" -o json \
  | jq -r '[.privilege_assignments[]?.privileges[]?.privilege] | unique | join(", ")' || true

# Dedicated (single user) Unity Catalog clusters refuse JAR libraries from /Workspace, so the bundle
# uploads its JARs to a volume (workspace.artifact_path). The volume must exist before the first
# `bundle deploy`, so it is created here, outside the bundle, in its own schema.
artifacts_schema="${ARTIFACTS_SCHEMA:-dab_connectors_artifacts}"
if ! databricks schemas get "$catalog.$artifacts_schema" >/dev/null 2>&1; then
  databricks schemas create "$artifacts_schema" "$catalog" \
    --comment "Bundle artifacts (JARs) for the dab-connectors POC." >/dev/null
fi
if ! databricks volumes read "$catalog.$artifacts_schema.artifacts" >/dev/null 2>&1; then
  databricks volumes create "$catalog" "$artifacts_schema" artifacts MANAGED \
    --comment "JARs uploaded by databricks bundle deploy (workspace.artifact_path)." >/dev/null
fi
echo "Artifact volume: /Volumes/$catalog/$artifacts_schema/artifacts"

env_file="$root/.env.databricks"
umask 077
{
  printf "export DATABRICKS_HOST='%s'\n" "$host"
  printf "export DATABRICKS_AUTH_TYPE='azure-cli'\n"
  printf "export DATABRICKS_AZURE_RESOURCE_ID='%s'\n" "$workspace_id"
  printf "export BUNDLE_VAR_catalog='%s'\n" "$catalog"
  printf "export BUNDLE_VAR_artifacts_schema='%s'\n" "$artifacts_schema"
} > "$env_file"
echo "Wrote $env_file (no secrets: the Databricks CLI uses your az login)."
echo "Workspace: $workspace_name  URL: $host  catalog: $catalog"
