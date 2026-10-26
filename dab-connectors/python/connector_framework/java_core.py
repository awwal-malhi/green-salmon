"""Calls the Java core (com.greensalmon.connectors.core.PyBridge) through Py4J."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame, SparkSession

BRIDGE_CLASS = "com.greensalmon.connectors.core.PyBridge"


class JavaCoreUnavailable(RuntimeError):
    pass


class JavaCore:
    def __init__(self, spark: SparkSession):
        self._spark = spark
        try:
            jvm = spark._jvm  # noqa: SLF001 - Py4J gateway; needs classic (non Spark Connect) compute.
        except Exception as e:  # pragma: no cover - depends on the compute type
            raise JavaCoreUnavailable(
                "No Py4J access to the JVM. Use dedicated (single user) access mode; "
                "standard/shared access mode and serverless do not allow it."
            ) from e
        if jvm is None:
            raise JavaCoreUnavailable("No Py4J access to the JVM (Spark Connect session?)")
        bridge = jvm
        for part in BRIDGE_CLASS.split("."):
            bridge = getattr(bridge, part)
        try:
            self.version = bridge.coreVersion()
        except TypeError as e:
            raise JavaCoreUnavailable(
                f"{BRIDGE_CLASS} is not on the classpath; install the connector-core, connector-plugin-api, "
                "shared-logging and shared-notifications JARs on the cluster"
            ) from e
        self._bridge = bridge

    def run_jar(self, jar_path: str, request: dict[str, Any]) -> dict[str, Any]:
        return json.loads(self._bridge.runJarPlugin(self._spark._jsparkSession, jar_path, json.dumps(request)))

    def run_dataframe(self, df: DataFrame, request: dict[str, Any]) -> dict[str, Any]:
        return json.loads(self._bridge.runDataFrame(self._spark._jsparkSession, df._jdf, json.dumps(request)))

    def record_failure(self, request: dict[str, Any], message: str) -> dict[str, Any]:
        return json.loads(self._bridge.recordFailure(self._spark._jsparkSession, json.dumps(request), message))
