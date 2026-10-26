# Task 1 — Azure SQL change data capture → queue → table

## Goal

Show that changes made to a table in Azure SQL Database can be captured with
change data capture (CDC), published to a message queue, and applied by a
consumer to a second table — with everything created from scripts in this repo,
so it can be rebuilt and torn down on demand.

In production the queue would be Solace. On Azure, use an Azure-native queue
instead: Azure Service Bus by default (Azure Storage Queues are acceptable if
you make the case).

## What to build (under `sql-cdc/`)

1. **Infrastructure scripts** that create everything in one resource group: an
   Azure SQL server and database (use the Azure SQL Database free offer), the
   queue, and anything else needed. Running them twice must not fail or
   duplicate anything.
2. **SQL scripts**: a source table (synthetic data, e.g. customers or orders),
   a target table of the same shape, and CDC enabled on the source.
3. **Publisher**: reads changes from CDC and sends one message per change to
   the queue.
4. **Consumer**: reads messages from the queue and applies each one (insert,
   update or delete) to the target table.
5. **End-to-end test**, runnable with one command: inserts, updates and deletes
   rows in the source, runs the publisher and consumer, and checks that the
   target table ends up identical to the source and that every change was
   delivered exactly once.
6. **Teardown script** that removes everything the scripts created.
7. **README**: how to set up, run, test and tear down, and what it costs per
   month.

## Constraints

- Region: Canada Central, unless there is a reason not to.
- Cost: well under $10/month in total. Use free or lowest tiers and state the
  expected cost.
- Python for the publisher, consumer and test. Your choice of Bicep, Terraform
  or Azure CLI for infrastructure — say why.
- Connection details and credentials come from environment variables; nothing
  secret is committed.
- Synthetic data only.

## How we work on this

**Phase A — build and review.** Write everything, test it as far as you can
without Azure, open a pull request and stop. Do not create anything in Azure in
this phase. In the PR description, list every Azure resource the scripts will
create, its tier and estimated monthly cost, and what you need from us for
Phase B.

**Phase B — run.** After the PR is reviewed we provide Azure credentials scoped
to one resource group and say "approved". Then run the scripts, run the
end-to-end test against Azure, and report the results.
