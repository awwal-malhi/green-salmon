# sql-cdc: Azure SQL change data capture → Service Bus → second table

Changes to `dbo.customers` in Azure SQL Database are captured by CDC. A **publisher**
sends one Service Bus message per change, and a **consumer** applies each message to
`dbo.customers_replica`, which has the same shape. Everything is created by scripts in this
folder and removed by `infra/teardown.sh`. Brief: [docs/task-1-sql-cdc.md](../docs/task-1-sql-cdc.md).

```
dbo.customers ──CDC capture──▶ cdc.dbo_customers_CT ──publisher──▶ Service Bus queue ──consumer──▶ dbo.customers_replica
                                                        (checkpoint)    "cdc-changes"     (ledger + row version)
```

## Layout

| path | what |
|---|---|
| `infra/main.bicep` | Azure SQL server + free-offer database, firewall rule, Service Bus Basic namespace, queue, send/listen SAS rules |
| `infra/deploy.sh` | deploy (idempotent) and write `.env.azure` with connection settings |
| `infra/teardown.sh` | delete everything `deploy.sh` created (by tag `workstream=sql-cdc`) |
| `sql/01_tables.sql` | source, target, publisher checkpoint, consumer ledger and row-version tables |
| `sql/02_enable_cdc.sql` | enable CDC on the database and on `dbo.customers` |
| `sql/99_reset.sql` | drop the POC tables (used by the e2e test) |
| `cdc_pipeline/publisher.py` | CDC → queue |
| `cdc_pipeline/consumer.py` | queue → target table |
| `cdc_pipeline/setup_db.py` | runs the SQL scripts |
| `tests/test_unit.py` | no database or queue needed |
| `tests/test_e2e.py` | end-to-end: source changes → target, exactly-once checks |
| `scripts/e2e.sh` | one-command e2e run: `local` (Docker) or `azure` |
| `local/` | Docker Compose for SQL Server 2022 (with Agent, for CDC) and the Service Bus emulator |

## Design notes

**Infrastructure: Bicep, driven by a thin Azure CLI script.** Bicep is Azure-native and
declarative. Incremental deployments with fixed names are idempotent: re-running changes
nothing. Unlike Terraform it needs no state file, so we don't need a state backend (another
storage account) or a local state file that a rebuild from another machine wouldn't have.
`WHAT_IF=1 infra/deploy.sh` previews the changes before anything is created. Resource names
get a `uniqueString(resourceGroup().id)` suffix, so they are globally unique and stay the same
on every run.

**Queue: Service Bus Basic.** Basic supports queues, dead-lettering and peek, and has no base
charge. It has no duplicate detection (that needs Standard, about $10/month), so delivery is
at least once, and the consumer makes it effectively exactly once:

- Each message carries the CDC position of its change (`__$start_lsn`, `__$seqval`), the
  operation and the full row image. Its `message_id` is derived from those values, so a
  resent change has the same id.
- The publisher stores the highest LSN it has sent in `dbo.cdc_publisher_checkpoint`, and
  only after Service Bus has accepted the messages. A crash in between re-sends the changes.
- In one SQL transaction, the consumer writes the change to the target and records it in
  `dbo.cdc_applied_changes`, whose primary key is the change key. A redelivered or resent
  message finds its ledger row and is skipped. The message is completed only after the
  commit.
- `dbo.cdc_row_version` holds the CDC position each target row reflects, kept as a tombstone
  after a delete. A change that arrives after a newer change to the same row is recorded as
  `stale` and not applied. This covers out-of-order delivery after an abandon or retry.
- Malformed messages are dead-lettered. If applying a change fails, the message is abandoned
  (retried up to 10 times, then dead-lettered) and the consumer stops.

**Database: Azure SQL Database free offer** (General Purpose serverless Gen5, max 2 vCores,
auto-pause after 60 min). Every month it includes 100,000 vCore-seconds and 32 GB of data.
With `freeLimitExhaustionBehavior: AutoPause`, the database pauses when the allowance runs out
instead of starting to bill. CDC on Azure SQL Database needs a vCore tier or S3+ (DTU), and
serverless is a vCore tier. On Azure SQL the CDC capture runs on a built-in scheduler, not
SQL Agent. The source and target tables are in the same database, which keeps us at one free
database.

## Prerequisites

- Python 3.10+, the [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
- For Azure: Azure CLI (logged in), with Contributor on the target resource group
- For the local test: Docker with Compose

```bash
cd sql-cdc
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest          # unit tests, no infrastructure
```

## Run locally (no Azure)

```bash
scripts/e2e.sh local
```

This starts SQL Server 2022 Developer with SQL Agent (for the CDC capture job) and the
official Service Bus emulator in Docker, creates the database, runs the e2e tests, and then
removes the containers. Set `KEEP=1` to keep them; remove them later with
`LOCAL_SA_PASSWORD=x docker compose -f local/docker-compose.yml down -v`.

## Set up on Azure

```bash
az login                                     # or a service principal login
export AZURE_RESOURCE_GROUP=<rg>
export SQL_ADMIN_PASSWORD='<strong password>'  # never committed; not written to .env.azure
WHAT_IF=1 infra/deploy.sh                    # preview
infra/deploy.sh                              # create / update (safe to re-run)
```

Optional settings: `AZURE_LOCATION` (default `canadacentral`), `NAME_PREFIX` (default
`gssqlcdc`), `SQL_ADMIN_LOGIN` (default `cdcadmin`), and `CLIENT_IP` (the IP allowed through
the SQL firewall; by default the script's public IP, detected automatically).

`deploy.sh` writes `.env.azure` (git-ignored, mode 600) with the SQL host and database, and
send-only and listen-only Service Bus connection strings.

## Run and test on Azure

```bash
scripts/e2e.sh azure           # setup_db + e2e tests against Azure
```

Or run each piece yourself:

```bash
set -a; source .env.azure; set +a; export SQL_PASSWORD="$SQL_ADMIN_PASSWORD"
.venv/bin/python -m cdc_pipeline.setup_db
.venv/bin/python -m cdc_pipeline.publisher            # add --follow to keep polling
.venv/bin/python -m cdc_pipeline.consumer             # add --follow to keep listening
```

The e2e test first resets the POC tables and empties the queue. Then it:

1. Makes 42 row changes to the source: single- and multi-row inserts, updates and deletes; a
   row inserted and deleted in the same transaction; and a row updated twice and then deleted.
2. Waits for CDC capture, checks that CDC recorded exactly those changes, runs the publisher
   (42 sent) and then the consumer.
3. Checks that the target matches the source row for row, and that the ledger holds exactly
   the CDC change set with no duplicates.
4. Deletes the publisher checkpoint to simulate a crash, which re-sends all 42 changes. The
   consumer must skip all 42 as duplicates, and the target and ledger must be unchanged.
5. Makes 10 more changes. Only those 10 are published and applied. The queue and the
   dead-letter queue end empty.

Separate tests cover out-of-order and duplicate delivery for a single row, and dead-lettering
of malformed messages.

## Tear down

```bash
export AZURE_RESOURCE_GROUP=<rg>
infra/teardown.sh             # asks for confirmation; YES=1 to skip
```

The script deletes the SQL server (with its database and firewall rule) and the Service Bus
namespace (with its queue and SAS rules). It finds them by the tag `workstream=sql-cdc`, so
nothing else in the resource group is touched. It also deletes the deployment record and
`.env.azure`, and it succeeds when there is nothing left to delete. The resource group itself
is kept.

## Cost (Canada Central, per month)

| resource | tier | expected cost |
|---|---|---|
| Azure SQL logical server | — | $0 (no charge for the server itself) |
| Azure SQL database `cdcdemo` | Free offer: GP serverless Gen5 2 vCore, 32 GB, auto-pause | $0 (pauses when the free allowance runs out instead of billing) |
| Service Bus namespace + queue | Basic | ≈ $0.05 per million operations; a few thousand operations per test run ≈ $0.00 |
| **Total** | | **≈ $0, under $1 even with heavy testing** |

Pricing from the Azure pricing pages at the time of writing. Check them again before Phase B,
because prices and free-offer terms can change.

## Configuration (environment variables)

See `.env.example`. SQL: `SQL_SERVER`, `SQL_DATABASE`, `SQL_USER`, `SQL_PASSWORD` (falls back
to `SQL_ADMIN_PASSWORD`), plus the optional `SQL_ODBC_DRIVER`, `SQL_ENCRYPT`,
`SQL_TRUST_SERVER_CERTIFICATE` and `SQL_AUTHENTICATION` (for example
`ActiveDirectoryServicePrincipal`). Service Bus: `SERVICEBUS_QUEUE`, plus either
`SERVICEBUS_CONNECTION_STRING` or the pair `SERVICEBUS_SEND_CONNECTION_STRING` /
`SERVICEBUS_LISTEN_CONNECTION_STRING`.

## Known limitations (POC)

- The publisher and consumer connect as the SQL server admin. Enabling CDC needs `db_owner`;
  production should use separate least-privilege users or managed identities.
- SQL authentication is used. If the subscription enforces Entra-only authentication, set
  `SQL_AUTHENTICATION` and the Bicep would need an Entra admin. Not done here.
- The firewall allows only the deploying machine's public IP. Private endpoints are out of
  scope, and the consumer runs from that machine, not in Azure.
- If the publisher falls behind CDC retention (3 days by default), it stops with
  `CdcGapError` instead of silently skipping changes.
