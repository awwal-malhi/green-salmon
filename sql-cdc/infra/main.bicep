// Azure resources for the SQL CDC -> Service Bus -> SQL proof of concept.
// Deployed into an existing resource group (incremental mode), so re-running is a no-op.

@description('Azure region. Defaults to Canada Central.')
param location string = 'canadacentral'

@description('Short prefix used in resource names.')
@minLength(3)
@maxLength(12)
param namePrefix string = 'gssqlcdc'

@description('SQL server administrator login (SQL authentication).')
param sqlAdminLogin string = 'cdcadmin'

@description('SQL server administrator password. Supplied from an environment variable, never committed.')
@secure()
param sqlAdminPassword string

@description('Public IPv4 address of the machine running the scripts, allowed through the SQL firewall. Empty = no client rule.')
param clientIpAddress string = ''

@description('Service Bus queue name.')
param queueName string = 'cdc-changes'

var suffix = uniqueString(resourceGroup().id)
var sqlServerName = '${namePrefix}-sql-${suffix}'
var sqlDatabaseName = 'cdcdemo'
var serviceBusNamespaceName = '${namePrefix}-sb-${suffix}'
var tags = {
  project: 'green-salmon'
  workstream: 'sql-cdc'
}

resource sqlServer 'Microsoft.Sql/servers@2023-08-01' = {
  name: sqlServerName
  location: location
  tags: tags
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

// Azure SQL Database free offer: General Purpose serverless Gen5, 2 vCores max,
// 100,000 vCore-seconds + 32 GB data per month. When the monthly allowance is used up
// the database auto-pauses until the next month instead of billing.
resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-08-01' = {
  parent: sqlServer
  name: sqlDatabaseName
  location: location
  tags: tags
  sku: {
    name: 'GP_S_Gen5_2'
    tier: 'GeneralPurpose'
    family: 'Gen5'
    capacity: 2
  }
  properties: {
    useFreeLimit: true
    freeLimitExhaustionBehavior: 'AutoPause'
    autoPauseDelay: 60
    minCapacity: json('0.5')
    maxSizeBytes: 34359738368
    requestedBackupStorageRedundancy: 'Local'
    zoneRedundant: false
  }
}

resource clientFirewallRule 'Microsoft.Sql/servers/firewallRules@2023-08-01' = if (!empty(clientIpAddress)) {
  parent: sqlServer
  name: 'deployer-client'
  properties: {
    startIpAddress: clientIpAddress
    endIpAddress: clientIpAddress
  }
}

// Service Bus Basic: queues only, billed per operation (no base charge).
resource serviceBus 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' = {
  name: serviceBusNamespaceName
  location: location
  tags: tags
  sku: {
    name: 'Basic'
    tier: 'Basic'
  }
  properties: {
    minimumTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
    disableLocalAuth: false
  }
}

resource queue 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: serviceBus
  name: queueName
  properties: {
    lockDuration: 'PT1M'
    maxDeliveryCount: 10
    defaultMessageTimeToLive: 'P7D'
    deadLetteringOnMessageExpiration: false
    requiresDuplicateDetection: false
    requiresSession: false
    enablePartitioning: false
  }
}

// Least-privilege SAS policies scoped to the queue: one send-only, one listen-only.
resource publisherRule 'Microsoft.ServiceBus/namespaces/queues/authorizationRules@2022-10-01-preview' = {
  parent: queue
  name: 'publisher'
  properties: {
    rights: [
      'Send'
    ]
  }
}

resource consumerRule 'Microsoft.ServiceBus/namespaces/queues/authorizationRules@2022-10-01-preview' = {
  parent: queue
  name: 'consumer'
  properties: {
    rights: [
      'Listen'
    ]
  }
}

output sqlServerName string = sqlServer.name
output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName
output sqlDatabaseName string = sqlDatabase.name
output sqlAdminLogin string = sqlAdminLogin
output serviceBusNamespaceName string = serviceBus.name
output queueName string = queue.name
output publisherRuleName string = publisherRule.name
output consumerRuleName string = consumerRule.name
