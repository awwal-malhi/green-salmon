# Task 2 — Common connector framework on Databricks, packaged with Asset Bundles

## Background

We run several data connectors on Databricks: CSV files, pipe-delimited files,
Solace messages, REST APIs. Each repeats the same logic — validate, log, handle
errors, write to Delta. We want one common framework in which the only piece
that differs per connector is a plugin.

## Design, as discussed

- A **plugin** implements a known interface and returns a Spark DataFrame from
  its source. That is all it does.
- The **common core** takes that DataFrame and does validation, logging, error
  handling (bad rows go somewhere visible, never silently dropped) and the write
  to a Delta table.
- A notebook or job calls the framework with a plugin name, plugin version and
  config.
- The core is written in **Java**. An in-house Java core exists but is not
  available here: build a small stand-in with the same shape and keep it
  minimal; the real one will be swapped in later.
- Plugins can be written in **Java (a JAR) or Python (a .py file)**. Ideally one
  framework detects which kind of plugin it was given and runs it. If that is not
  workable, explain why and propose the closest alternative.
- **Logging and notifications** are shared modules that other bundles can
  depend on, rather than copies inside each connector.
- Everything is packaged and deployed with **Databricks Asset Bundles**,
  versioned so that two versions of the same plugin (e.g. CSV plugin v1 and v2)
  can be deployed and run side by side.

## What to build (under `dab-connectors/`)

1. The Java core stand-in and the plugin interface.
2. Two plugins: CSV in Java, pipe-delimited in Python.
3. Shared logging and notifications modules. Notifications can be a stub that
   records the notification — no external services.
4. Bundle configuration that deploys the core, both plugins (with CSV v1 and v2
   side by side), and a job that runs a chosen plugin and version into a Delta
   table.
5. Tests that run locally without Databricks (Spark local mode): the core's
   validation and error handling, each plugin, and the Java/Python path.
6. A README covering the architecture, how to add a plugin, how versions work,
   what is required on the Databricks side (runtime version, compute type,
   permissions), and the estimated cost to run.

## Constraints

- The workspace will be Azure Databricks. Keep compute minimal: single node,
  auto-terminating.
- Synthetic sample data only. Nothing secret is committed.

## How we work on this

**Phase A — build and review.** Build it, test it locally as far as you can,
open a pull request and stop. You will not have a Databricks workspace in this
phase. In the PR description, list what you need from us for Phase B.

**Phase B — run.** After the PR is reviewed we provide a workspace and
credentials. Then deploy the bundle, run the job for each plugin (including both
CSV versions), and report the results.
