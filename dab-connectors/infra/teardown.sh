#!/usr/bin/env bash
# Delete everything deploy.sh and the bundle created. The bundle is destroyed first (jobs, schema
# with its tables, volume, workspace files) and the artifacts schema/volume deploy.sh made, then every Azure Databricks workspace tagged
# workstream=dab-connectors in the resource group, which also deletes its managed resource group
# (VNet, NSG, storage account, access connector). The resource group itself is left in place
# (it is provided to us). Safe to re-run when nothing is left.
#
# Required env: AZURE_RESOURCE_GROUP
# Optional env: YES=1 to skip the confirmation prompt
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"
: "${AZURE_RESOURCE_GROUP:?set AZURE_RESOURCE_GROUP}"
rg="$AZURE_RESOURCE_GROUP"

# az rejects --tag together with --resource-group, so filter on the tag in the query.
tagged="tags.workstream=='dab-connectors'"
ws_ids="$(az resource list --resource-group "$rg" \
  --query "[?$tagged && type=='Microsoft.Databricks/workspaces'].id" -o tsv)"
mapfile -t ids < <(printf '%s\n' "$ws_ids" | sed '/^$/d')

if [[ ${#ids[@]} -eq 0 ]]; then
  echo "Nothing to delete in $rg."
else
  echo "Will destroy the dab-connectors bundle, then delete (with their managed resource groups):"
  printf '  %s\n' "${ids[@]}"
  if [[ "${YES:-0}" != "1" ]]; then
    read -r -p "Type 'delete' to continue: " answer
    [[ "$answer" == "delete" ]] || { echo "Aborted."; exit 1; }
  fi
  if [[ -f "$root/.env.databricks" ]]; then
    # shellcheck disable=SC1091
    source "$root/.env.databricks"
    (cd "$root" && databricks bundle destroy -t dev --auto-approve) \
      || echo "bundle destroy failed; continuing (deleting the workspace removes its jobs and files)" >&2
    databricks schemas delete "$BUNDLE_VAR_catalog.${BUNDLE_VAR_artifacts_schema:-dab_connectors_artifacts}" --force \
      || echo "could not delete the artifacts schema; continuing" >&2
  fi
  for id in "${ids[@]}"; do
    echo "Deleting $id (takes several minutes)"
    az resource delete --ids "$id"
  done
fi

if az deployment group show --resource-group "$rg" --name dab-connectors >/dev/null 2>&1; then
  az deployment group delete --resource-group "$rg" --name dab-connectors
fi
rm -f "$root/.env.databricks"

remaining="$(az resource list --resource-group "$rg" --query "length([?$tagged])" -o tsv)"
echo "Remaining dab-connectors resources in $rg: $remaining"
[[ "$remaining" == "0" ]]
