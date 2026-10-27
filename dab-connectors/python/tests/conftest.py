"""Local Spark session with the Java core on the classpath (as the cluster libraries would be) and Delta.

Run scripts/test.sh, or scripts/build.sh plus the Delta JAR download it does, before pytest.
Plugin JARs are deliberately NOT on the classpath: the core loads them from build/plugins.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

BUNDLE_ROOT = Path(__file__).resolve().parents[2]
PYTHON_DIR = BUNDLE_ROOT / "python"
BUILD = BUNDLE_ROOT / "build"
PLUGIN_ROOT = BUILD / "plugins"
SAMPLE_DATA = BUNDLE_ROOT / "sample-data"
CONFIG_DIR = BUNDLE_ROOT / "config"

sys.path.insert(0, str(PYTHON_DIR))


def _jars() -> list[str]:
    libs = sorted((BUILD / "libs").glob("*.jar"))
    delta = sorted((BUILD / "test-libs").glob("*.jar"))
    if len(libs) < 4 or len(delta) < 2:
        pytest.exit("Build output missing: run dab-connectors/scripts/test.sh (or build.sh + Delta JAR download)", 2)
    return [str(p) for p in libs + delta]


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("connector-framework-tests")
        .config("spark.jars", ",".join(_jars()))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture
def plugin_root(tmp_path) -> Path:
    """A copy of the staged plugins, so tests can add broken plugins without touching build/."""
    root = tmp_path / "plugins"
    shutil.copytree(PLUGIN_ROOT, root)
    return root
