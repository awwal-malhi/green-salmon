// Azure resources for the dab-connectors POC: one Azure Databricks workspace (Premium).
// Deployed into an existing resource group (incremental mode), so re-running is a no-op.
// Everything else the workspace needs (VNet, NSG, root storage account, Unity Catalog access
// connector) is created by Azure Databricks in its own managed resource group and deleted with it.

@description('Azure region. Defaults to Canada Central.')
param location string = 'canadacentral'

@description('Short prefix used in resource names.')
@minLength(3)
@maxLength(12)
param namePrefix string = 'gsdabconn'

var workspaceName = '${namePrefix}-dbw'
var managedResourceGroupName = 'databricks-rg-${workspaceName}-${uniqueString(resourceGroup().id)}'
var tags = {
  project: 'green-salmon'
  workstream: 'dab-connectors'
}

// Premium has no standing charge: Azure Databricks bills DBUs only while compute runs.
// enableNoPublicIp=true: secure cluster connectivity (the secure default). Cluster VMs get no public
// IP; Azure Databricks adds a NAT gateway + public IP to the managed resource group for outbound
// traffic, and those bill every hour even with no cluster running (about $36/month).
// storageAccountSkuName=Standard_LRS instead of the default Standard_GRS for the root storage.
resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: workspaceName
  location: location
  tags: tags
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedResourceGroupName)
    publicNetworkAccess: 'Enabled'
    parameters: {
      enableNoPublicIp: {
        value: true
      }
      storageAccountSkuName: {
        value: 'Standard_LRS'
      }
    }
    defaultCatalog: {
      initialType: 'UnityCatalog'
    }
  }
}

output workspaceName string = workspace.name
output workspaceId string = workspace.id
output workspaceUrl string = workspace.properties.workspaceUrl
output managedResourceGroupId string = workspace.properties.managedResourceGroupId
