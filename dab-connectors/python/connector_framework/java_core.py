"""Calls the Java core (com.greensalmon.connectors.core.PyBridge) through Py4J."""

from __future__ import annotations

import json
from pathlib import Path
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
        jar_path = self.jvm_readable_path(jar_path)
        return json.loads(self._bridge.runJarPlugin(self._spark._jsparkSession, jar_path, json.dumps(request)))

    def jvm_readable_path(self, path: str) -> str:
        """Path the JVM can open for this file, copying it into a JVM temp file if needed.

        On dedicated Unity Catalog clusters the Python process can read /Workspace and /Volumes files
        but the driver JVM cannot, so the bytes are handed over through Py4J instead.
        """
        if self._jvm_can_read(path):
            return path
        jvm = self._spark._jvm  # noqa: SLF001
        name = Path(path).name
        tmp = jvm.java.io.File.createTempFile(Path(name).stem + "-", Path(name).suffix or ".jar")
        tmp.deleteOnExit()
        out = jvm.java.io.FileOutputStream(tmp)
        try:
            out.write(bytearray(Path(path).read_bytes()))
        finally:
            out.close()
        return tmp.getAbsolutePath()

    def _jvm_can_read(self, path: str) -> bool:
        return bool(self._spark._jvm.java.io.File(path).isFile())  # noqa: SLF001

    def run_dataframe(self, df: DataFrame, request: dict[str, Any]) -> dict[str, Any]:
        return json.loads(self._bridge.runDataFrame(self._spark._jsparkSession, df._jdf, json.dumps(request)))

    def record_failure(self, request: dict[str, Any], message: str) -> dict[str, Any]:
        return json.loads(self._bridge.recordFailure(self._spark._jsparkSession, json.dumps(request), message))
