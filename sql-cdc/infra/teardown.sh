#!/usr/bin/env bash
# Delete every resource created by deploy.sh. Only resources tagged
# workstream=sql-cdc in the resource group are touched; the resource group itself
# is left in place (it is provided to us). Safe to re-run when nothing is left.
#
# Required env: AZURE_RESOURCE_GROUP
# Optional env: YES=1 to skip the confirmation prompt
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"
: "${AZURE_RESOURCE_GROUP:?set AZURE_RESOURCE_GROUP}"
rg="$AZURE_RESOURCE_GROUP"

list() {
  az resource list --resource-group "$rg" --tag workstream=sql-cdc \
    --query "[?type=='$1'].id" -o tsv
}
mapfile -t ids < <(list Microsoft.Sql/servers; list Microsoft.ServiceBus/namespaces)

if [[ ${#ids[@]} -eq 0 ]]; then
  echo "Nothing to delete in $rg."
else
  echo "Will delete (with all child databases, firewall rules, queues):"
  printf '  %s\n' "${ids[@]}"
  if [[ "${YES:-0}" != "1" ]]; then
    read -r -p "Type 'delete' to continue: " answer
    [[ "$answer" == "delete" ]] || { echo "Aborted."; exit 1; }
  fi
  for id in "${ids[@]}"; do
    echo "Deleting $id"
    az resource delete --ids "$id"
  done
fi

if az deployment group show --resource-group "$rg" --name sql-cdc >/dev/null 2>&1; then
  az deployment group delete --resource-group "$rg" --name sql-cdc
fi
rm -f "$root/.env.azure"

remaining="$(az resource list --resource-group "$rg" --tag workstream=sql-cdc --query "length(@)" -o tsv)"
echo "Remaining sql-cdc resources in $rg: $remaining"
[[ "$remaining" == "0" ]]
