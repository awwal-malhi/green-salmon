# dab-connectors

One connector framework on Databricks. Every connector (CSV, pipe-delimited, and later Solace, REST, ...)
shares one **core** that validates, logs, quarantines bad rows, notifies and writes to Delta. The only
part that differs per connector is a **plugin** that turns a source into a DataFrame. Plugins can be
Java or Python, and several versions of one plugin can be deployed and run side by side.

Phase A: built and tested locally (Spark local mode). Phase B: `infra/` creates the Azure Databricks
workspace; see [Workspace](#workspace-infra) and [Phase B](#phase-b-deploy-and-run).

## Layout

```
databricks.yml, resources/      Asset Bundle: artifacts, schema + volume, jobs
infra/                          deploy.sh, main.bicep, teardown.sh: the Azure Databricks workspace
java/plugin-api                 SourcePlugin interface + PluginConfig          (jar, v1.0.0)
java/shared-logging             structured JSON logging (SLF4J + in-memory)    (jar, v1.0.0)
java/shared-notifications       Notifier + RecordingNotifier stub              (jar, v1.0.0)
java/connector-core             validation, quarantine, Delta writes, runs table, plugin loader, Py bridge (jar, v0.1.0)
java/plugins/csv-v1, csv-v2     CSV plugin, versions 1.0.0 and 2.0.0
python/connector_framework      launcher, plugin resolver/loader, Python plugin API, Py4J bridge to the core
python/plugins/pipe_delimited/1.0.0   pipe-delimited plugin (Python)
python/run_connector.py         job entry point
config/                         run configs (validation rules) per connector
sample-data/                    synthetic input files
scripts/                        build.sh, test.sh, validate_bundle.py, upload_sample_data.sh, show_results.sh
```

## Architecture

```
            job parameters: plugin_name, plugin_version, config
                                  |
                       python/run_connector.py  (launcher)
                                  |
             resolve <plugin_root>/<name>/<version>/<one artifact>
                 .jar -> Java plugin            .py -> Python plugin
                     |                                |
        PyBridge.runJarPlugin (Py4J)       plugin.read(spark, source) -> DataFrame
        PluginLoader: own URLClassLoader          |
        per JAR, ServiceLoader lookup      PyBridge.runDataFrame (Py4J)
                     \                           /
                      ConnectorRunner (Java core)
   read -> Validator.annotate -> threshold check -> valid rows (cast) -> Delta target table
                                                  -> bad rows + reasons -> Delta quarantine table
          -> run record -> Delta runs table   -> ConnectorLogger (JSON)   -> Notifier
```

* **Plugin contract.** Java: `com.greensalmon.connectors.api.SourcePlugin` (`name()`, `version()`,
  `read(SparkSession, PluginConfig)`). Python: `connector_framework.plugin_api.SourcePlugin` (`name`,
  `version`, `read(spark, options)`). A plugin only reads. It returns raw columns (strings are fine) and,
  optionally, a `_corrupt_record` column marking rows it could not parse. It does not validate, log
  run status, catch errors or write anything.
* **The core is Java for both paths.** The Python launcher always calls the Java core through Py4J: for a
  Java plugin it passes the JAR path, for a Python plugin it passes the DataFrame the plugin returned. A
  Python plugin that raises is recorded through the core too (`PyBridge.recordFailure`), so failures look
  the same whatever the plugin language.
* **Validation** (`config/*.json` -> `validation`): `required_columns` (missing column = the whole run
  fails), `not_null` (null or blank), `types` (Spark SQL types, checked with `try_cast` and applied to the
  written rows), `max_bad_row_fraction`. Every bad row gets all its reasons
  (`malformed_record`, `not_null:<col>`, `invalid_type:<col>:<type>`).
* **Errors.** Bad rows are never dropped silently: they go to `<target>_quarantine` with `_error_reasons`,
  the original row as JSON in `_record`, and run/plugin columns. If the bad fraction is above
  `max_bad_row_fraction`, bad rows are still quarantined, the target write is skipped and the run is
  `FAILED`. Any exception in a plugin or the core becomes a `FAILED` result; the job task then exits
  non-zero, so the Databricks run shows as failed.
* **Run status** is `SUCCEEDED`, `SUCCEEDED_WITH_ERRORS` (some rows quarantined) or `FAILED`; each run
  appends a row (counts, plugin, version, kind, timings, message) to the runs table if
  `target.runs_table` is set.
* **Logging**: one JSON line per event (`run_started`, `rows_validated`, `rows_quarantined`, `run_failed`,
  `run_finished`) with run id, plugin and version, through SLF4J into the driver log.
* **Notifications**: `RecordingNotifier` keeps notifications in memory and, if `notifications.dir` is set,
  writes one JSON file per notification (the job writes them to the bundle's volume). `INFO` on success,
  `WARNING` with quarantined rows, `ERROR` on failure. Replacing it with email/Teams/etc. later means
  another `Notifier` implementation; nothing else changes.
* Target rows get `_run_id`, `_plugin`, `_plugin_version`, `_ingested_at` lineage columns.

## How versions work

* Each plugin version is a separate artifact in its own directory: `build/plugins/<name>/<version>/`.
  `scripts/build.sh` stages them; the bundle syncs them to the workspace.
  ```
  build/plugins/csv/1.0.0/csv-plugin-1.0.0.jar
  build/plugins/csv/2.0.0/csv-plugin-2.0.0.jar
  build/plugins/pipe_delimited/1.0.0/pipe_delimited_plugin.py
  ```
* A job run picks `plugin_name` + `plugin_version`. Deploying a new version adds a directory; old versions
  keep working until you delete them, so switching or rolling back is a job parameter change.
* **Side by side in one JVM.** Java plugin JARs are *not* installed as cluster libraries. The core loads
  each JAR in its own `URLClassLoader` (parent = the plugin API), so csv 1.0.0 and 2.0.0, which use the
  same class name `com.greensalmon.plugins.csv.CsvSourcePlugin`, run one after another on one cluster
  without clashing. Python plugins are imported under a module name derived from name + version.
* The loaded plugin must report the same name and version as its directory, or the run fails. That
  catches a JAR copied into the wrong directory.
* The core and shared modules are cluster libraries (one version per job). Their versions are in the
  JAR names and the core reports its version via `PyBridge.coreVersion()`; it is logged with every run.
* What changed between the CSV versions (sample data has a comment line and a padded value to show it):
  v1.0.0 reads every line as data and keeps whitespace; v2.0.0 trims values, skips `#` comment lines and
  adds `_source_file`.
* Constraint of the class-loader approach: plugin code runs on the driver. Built-in Spark operations are
  fine on any cluster, but a plugin that ships its own classes to executors (UDFs) would need its JAR on
  the executors too. With the single-node compute used here the driver is the only node.

## Adding a plugin

Java:
1. Copy `java/plugins/csv-v2` to `java/plugins/<name>-v<major>`, set `artifactId`, `version` and
   `finalName` in its `pom.xml`, add the module to `java/pom.xml`.
2. Implement `SourcePlugin` (public no-arg constructor) returning `name()` and `version()` matching the
   directory you will stage it in, and list the class in
   `src/main/resources/META-INF/services/com.greensalmon.connectors.api.SourcePlugin`.
3. Depend only on `connector-plugin-api` (scope `provided`) and Spark (`provided`); anything else you need
   must be shaded into the plugin JAR.
4. Add a staging line to `scripts/build.sh` and a test like `CsvSourcePluginTest`.

Python:
1. Create `python/plugins/<name>/<version>/<name>_plugin.py` with exactly one `SourcePlugin` subclass that
   sets `name` and `version` to the directory names.
2. Return a DataFrame; mark unparseable rows in `_corrupt_record` if the source can have them.
3. Add tests like `python/tests/test_pipe_delimited_plugin.py`.

Then add a `config/<connector>.json` with the validation rules, and either run it with `connector_run`
(set `plugin_name`, `plugin_version`, `config_file`, `source_path`, `target_table`) or add a job/task in
`resources/jobs.yml`. A new plugin version is a new directory (and for Java a new module or version bump);
never overwrite a deployed version.

## Testing locally (no Databricks)

Needs JDK 17 (8 and 11 also work), Maven 3.6+, Python 3.10+.

```bash
cd dab-connectors
python -m venv .venv && . .venv/bin/activate && pip install -r python/requirements-dev.txt
scripts/test.sh
```

`scripts/test.sh` runs the Java unit tests (JUnit, Spark local mode: validator, runner error handling,
write modes, runs table, notifications, plugin loader isolation, both CSV versions), stages the
artifacts, downloads the Delta JARs, then runs the Python tests (pytest, Spark local mode with the built
core JARs on the classpath): resolver, config overrides, pipe-delimited plugin, and end-to-end runs of
csv 1.0.0, csv 2.0.0 (also both in one session) and pipe_delimited 1.0.0 through the Java core into Delta
tables, including failing plugins. It finishes with an offline check of the bundle YAML against the
Databricks CLI's bundle JSON schema if the `databricks` CLI is installed (`databricks bundle validate`
needs a workspace, so it was not run).

Results at the time of the PR: 28 Java tests and 26 Python tests pass; bundle schema check reports 0 errors.

## What the Databricks side needs

| Item | Requirement |
|---|---|
| Workspace | Azure Databricks, **Premium** tier, Unity Catalog enabled |
| Runtime | **15.4 LTS** (`15.4.x-scala2.12`: Spark 3.5.0, Scala 2.12, Delta built in). The JARs are built against Spark 3.5.0 / Scala 2.12 with Java 8 bytecode. Other 3.5.x / Scala 2.12 runtimes should work; 16.x+ (Scala 2.13 in 17) is untested. |
| Compute | Job cluster, **single node** (`num_workers: 0`), `Standard_D4ds_v5` (4 vCPU, 16 GB), on-demand. Created for each run and terminated when the run ends, so nothing idles. If you prefer an all-purpose cluster for debugging, set auto-termination (e.g. 20 minutes). |
| Access mode | **Dedicated (single user)**. The Python launcher calls the Java core through Py4J (`spark._jvm`), which standard (shared) access mode and serverless do not allow. Python plugins alone would work anywhere; Java plugins or the Java core need this. |
| Libraries | Installed by the bundle as job libraries: `connector-plugin-api`, `shared-logging`, `shared-notifications`, `connector-core` JARs. Plugins are not libraries (see versions). If the workspace restricts where JAR libraries may come from (Unity Catalog allowlist), the bundle's upload path must be allowed; to confirm in Phase B. |
| Permissions for the deploying identity | Workspace access and permission to create jobs and job clusters (unrestricted cluster creation, or a cluster policy that allows the single-node config above). `USE CATALOG` + `CREATE SCHEMA` on the target catalog (the bundle creates schema `dab_connectors` and volume `files`). If the schema is pre-created instead: `USE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`, `READ VOLUME`, `WRITE VOLUME` on it. |
| Permissions for the job run-as identity | Same as deployer by default (jobs run as the deployer). It writes the target, quarantine and runs tables, reads sample data from the volume and writes notification files to it. |
| Network / secrets | None. No external services are called; no secrets are needed by the jobs. CLI auth for deploy comes from your environment (profile, OAuth or env vars), never from this repo. |

## Estimated cost

Assumptions (Azure list prices, USD, pay-as-you-go, retrieved from the
Azure Retail Prices API during Phase A; check your region and any discount):

* VM `Standard_D4ds_v5` (Linux, Canada Central): about **$0.252/hour**.
* Premium **Jobs Compute** DBU: **$0.30/DBU-hour**. `Standard_D4ds_v5` is assumed to consume about
  1 DBU/hour (confirm on the Azure Databricks pricing page for the chosen VM).
* So one single-node cluster costs about **$0.55/hour** while running. Billing covers cluster start-up
  (typically 4-6 minutes) plus run time.

| Run | Wall time (est.) | Cost (est.) |
|---|---|---|
| `connector_run` (one plugin, sample data) | ~7 min incl. start-up | ~$0.06 |
| `connector_smoke_test` (csv 1.0.0, csv 2.0.0, pipe_delimited 1.0.0 on one cluster) | ~10 min | ~$0.09 |
| Phase B total (a few deploy/run iterations, ~10 cluster starts) | ~1.5 h of cluster time | **under $2** |

Storage (a few KB of Delta tables and files in managed storage) and the workspace itself are negligible
for this POC. Nothing runs when no job is running; there is no schedule in the bundle. The alternative
`Standard_DS3_v2` (0.75 DBU/h, $0.293/h VM) is about the same price.

## Workspace (infra/)

`infra/deploy.sh` creates the workspace with Bicep (`infra/main.bicep`), into the existing resource group,
incremental mode, fixed names, so re-running it changes nothing. `WHAT_IF=1 infra/deploy.sh` previews it.
It needs `AZURE_RESOURCE_GROUP` and an `az login` (user or service principal with Contributor on the
resource group); optional `AZURE_LOCATION` (default `canadacentral`), `NAME_PREFIX` (default `gsdabconn`),
`CATALOG`.

What it creates:

* In the resource group: Azure Databricks workspace `gsdabconn-dbw`, **Premium**, tags
  `project=green-salmon`, `workstream=dab-connectors`, public network access on, Unity Catalog as the
  initial default catalog, secure cluster connectivity (no public IP) **off**, root storage
  `Standard_LRS` (the default is GRS).
* Created by Azure Databricks in its managed resource group `databricks-rg-gsdabconn-dbw-<suffix>`
  (locked by a deny assignment, deleted with the workspace): a VNet with two subnets, an NSG, the
  workspace storage account (ADLS Gen2) and, with automatic Unity Catalog enablement, an access
  connector (managed identity) for the workspace catalog.
* Inside the workspace, by Azure Databricks: the workspace catalog (`gsdabconn_dbw`), attached to the
  region's Unity Catalog metastore, and possibly a stopped "Serverless Starter Warehouse".

Then the script signs in with the Databricks CLI using the same `az` login
(`DATABRICKS_AUTH_TYPE=azure-cli`; Contributor on the workspace makes the identity a workspace admin),
waits until the workspace answers, checks a metastore is assigned and the catalog exists, prints the
identity's privileges on it, and writes `.env.databricks` (git-ignored, mode 600) with `DATABRICKS_HOST`,
`DATABRICKS_AUTH_TYPE`, `DATABRICKS_AZURE_RESOURCE_ID` and `BUNDLE_VAR_catalog`. No token, password or
secret is created or written. If no metastore is assigned it stops: assigning one needs a Databricks
account admin.

`infra/teardown.sh` (asks for confirmation; `YES=1` skips it) runs `databricks bundle destroy`, then
deletes the workspaces tagged `workstream=dab-connectors` (which deletes the managed resource group),
the deployment record and `.env.databricks`. Nothing else in the resource group is touched.

### Cost while idle (Canada Central, list prices, USD)

| Item | Idle cost |
|---|---|
| Workspace, Premium | $0. No standing fee; Databricks bills DBUs only while compute runs. |
| VNet, subnets, NSG, access connector | $0 |
| NAT gateway / public IP | None created, because secure cluster connectivity is off. With it on, Azure Databricks adds a NAT gateway and a Standard public IP that bill around the clock (about $0.045/h + $0.005/h, roughly $36/month). With it off, each cluster VM gets a public IP only while it runs (about $0.004/h). |
| Workspace storage account (Standard LRS, hot) | The only idle charge: $0.0184-0.02 per GB-month stored plus operations ($0.0044 per 10K reads, $0.055 per 10K writes). The POC stores a few MB (DBFS root, workspace-catalog tables, sample files, notifications), so well under $0.10/month. |
| Job clusters | $0. Created per run, terminated when the run ends; no schedules in the bundle. |
| SQL warehouse (for `scripts/show_results.sh`) | $0 when stopped. Premium Serverless SQL is $0.79/DBU-h; 2X-Small = 4 DBU/h, about $3.16/h while running. The script stops it when done; auto-stop 5 min is the backstop. |
| Predictive optimization | Would run serverless maintenance on the managed tables now and then. `scripts/upload_sample_data.sh` turns it off for the bundle's schema. |

## Phase B: deploy and run

```bash
cd dab-connectors
infra/deploy.sh                                   # workspace + checks, writes .env.databricks
source .env.databricks
databricks bundle validate -t dev
databricks bundle deploy   -t dev                 # runs scripts/build.sh, uploads JARs, plugins, configs
scripts/upload_sample_data.sh "$BUNDLE_VAR_catalog"
databricks bundle run connector_smoke_test -t dev
scripts/show_results.sh                           # row counts: target, quarantine, runs tables
```

Check `<catalog>.dab_connectors.connector_runs`, the target and `_quarantine` tables, and
`/Volumes/<catalog>/dab_connectors/files/notifications/`. `databricks bundle destroy -t dev` removes the
jobs, schema (and its tables) and volume; `infra/teardown.sh` removes everything.

Things only Phase B can confirm: the bundle validates against a real workspace; workspace-file paths
(`/Workspace/...`) are readable by the core's class loader and the Python importer on the chosen runtime;
JAR library allowlisting in the workspace's metastore; actual start-up time and cost.
