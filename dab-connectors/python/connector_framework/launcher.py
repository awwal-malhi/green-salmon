"""Runs one plugin version through the Java core. Used by the Databricks job and from notebooks.

    from connector_framework.launcher import run
    result = run(spark, plugin_root, "csv", "2.0.0", config)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Sequence

from pyspark.sql import SparkSession

from . import config as run_config
from . import python_loader
from .java_core import JavaCore
from .resolver import JAVA, resolve

log = logging.getLogger("connector_framework")


class ConnectorRunFailed(RuntimeError):
    def __init__(self, result: dict[str, Any]):
        super().__init__(f"Connector run {result.get('run_id')} FAILED: {result.get('message')}")
        self.result = result


def run(
    spark: SparkSession,
    plugin_root: str | Path,
    plugin_name: str,
    plugin_version: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Resolve the plugin, detect Java or Python, run it through the Java core. Returns the run result."""
    artifact = resolve(plugin_root, plugin_name, plugin_version)
    request = dict(config)
    request["plugin"] = {"name": plugin_name, "version": plugin_version, "kind": artifact.kind}
    core = JavaCore(spark)
    log.info("Running %s %s (%s plugin %s) with core %s", plugin_name, plugin_version, artifact.kind,
             artifact.path, core.version)

    if artifact.kind == JAVA:
        return core.run_jar(str(artifact.path), request)

    try:
        plugin = python_loader.load(artifact)
        df = plugin.read(spark, dict(request.get("source") or {}))
    except Exception as e:  # noqa: BLE001 - every plugin failure becomes a recorded, notified FAILED run
        log.exception("Python plugin %s %s failed", plugin_name, plugin_version)
        return core.record_failure(request, f"{type(e).__name__}: {e}")
    return core.run_dataframe(df, request)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a connector plugin through the common core")
    p.add_argument("--plugin-root", required=True, help="Directory holding <name>/<version>/<artifact>")
    p.add_argument("--plugin-name", required=True)
    p.add_argument("--plugin-version", required=True)
    p.add_argument("--config-file", help="JSON run config")
    p.add_argument("--set", dest="overrides", action="append", default=[],
                   help="Override a config value: key.path=value (repeatable)")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None, spark: SparkSession | None = None) -> dict[str, Any]:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    cfg = run_config.apply_overrides(run_config.load(args.config_file), args.overrides)
    spark = spark or SparkSession.builder.getOrCreate()
    result = run(spark, args.plugin_root, args.plugin_name, args.plugin_version, cfg)
    print(json.dumps(result, indent=2))
    if result.get("status") == "FAILED":
        raise ConnectorRunFailed(result)
    return result
