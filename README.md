# green-salmon

Proof of concept: can an AI engineer (Devin) build and run data-platform work
on Azure, from scripts kept in this repo?

Two independent workstreams:

| folder | what | brief |
|---|---|---|
| `sql-cdc/` | Azure SQL change data capture → queue → a second table | [docs/task-1-sql-cdc.md](docs/task-1-sql-cdc.md) |
| `dab-connectors/` | One connector framework on Databricks with Java and Python plugins, packaged with Asset Bundles | [docs/task-2-dab-connectors.md](docs/task-2-dab-connectors.md) |

## How we work

- Every Azure resource is created by a script in this repo, reviewed in a pull
  request **before** it is run, and removable by a teardown script.
- Azure access is limited to one resource group, on a subscription with a
  spending limit.
- Synthetic data only. No secrets in the repo; credentials come from
  environment variables.
