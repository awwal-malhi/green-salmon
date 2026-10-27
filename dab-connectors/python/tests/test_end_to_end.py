"""Plugin -> Java core -> Delta, for the Java path (csv 1.0.0 and 2.0.0) and the Python path (pipe_delimited)."""

import json
import uuid

import pytest

from conftest import CONFIG_DIR, SAMPLE_DATA
from connector_framework import config as run_config
from connector_framework.java_core import JavaCore
from connector_framework.launcher import ConnectorRunFailed, main, run


@pytest.fixture
def names():
    p = "e2e_" + uuid.uuid4().hex[:8]
    return {"prefix": p, "runs": f"{p}_runs"}


def _config(file, source_path, table, runs_table, notifications_dir, *extra):
    return run_config.apply_overrides(
        run_config.load(CONFIG_DIR / file),
        [f"source.path={source_path}", f"target.table={table}", f"target.runs_table={runs_table}",
         f"notifications.dir={notifications_dir}", *extra],
    )


def _notifications(directory):
    return [json.loads(p.read_text()) for p in sorted(directory.glob("*.json"))]


def test_csv_v1_java_plugin(spark, plugin_root, names, tmp_path):
    table = f"{names['prefix']}_orders_v1"
    cfg = _config("orders_csv.json", SAMPLE_DATA / "orders", table, names["runs"], tmp_path / "n")
    result = run(spark, plugin_root, "csv", "1.0.0", cfg)

    assert result["status"] == "SUCCEEDED_WITH_ERRORS"
    assert result["plugin_kind"] == "java"
    # 10 data rows + the comment line, which v1 does not understand.
    assert (result["rows_read"], result["rows_written"], result["rows_quarantined"]) == (11, 6, 5)
    target = spark.table(table)
    assert dict(target.dtypes)["amount"] == "decimal(12,2)"
    assert dict(target.dtypes)["order_date"] == "date"
    assert "_source_file" not in target.columns
    # v1 keeps the whitespace around " C004 ".
    assert target.filter("order_id = 1004").first()["customer_id"] == " C004 "

    reasons = sorted(sorted(row["_error_reasons"]) for row in spark.table(f"{table}_quarantine").collect())
    assert reasons == sorted([
        # v1 reads the comment line as a row: it is malformed and its fields fail every check.
        sorted(["malformed_record", "not_null:order_date", "not_null:amount", "invalid_type:order_id:int"]),
        ["not_null:customer_id"],
        ["invalid_type:order_date:date"],
        ["invalid_type:amount:decimal(12,2)"],
        ["malformed_record"],
    ])
    [note] = _notifications(tmp_path / "n")
    assert note["severity"] == "WARNING"


def test_java_plugin_jar_the_jvm_cannot_read_is_copied_over_py4j(spark, plugin_root, names, tmp_path, monkeypatch):
    # Dedicated UC clusters: Python reads /Workspace and /Volumes, the driver JVM does not.
    monkeypatch.setattr(JavaCore, "_jvm_can_read", lambda self, path: False)
    jar = plugin_root / "csv" / "1.0.0" / "csv-plugin-1.0.0.jar"
    staged = JavaCore(spark).jvm_readable_path(str(jar))
    assert staged != str(jar)
    assert open(staged, "rb").read() == jar.read_bytes()

    cfg = _config("orders_csv.json", SAMPLE_DATA / "orders", f"{names['prefix']}_staged", names["runs"], tmp_path / "n")
    result = run(spark, plugin_root, "csv", "1.0.0", cfg)
    assert (result["status"], result["rows_written"], result["rows_quarantined"]) == ("SUCCEEDED_WITH_ERRORS", 6, 5)


def test_csv_v2_java_plugin(spark, plugin_root, names, tmp_path):
    table = f"{names['prefix']}_orders_v2"
    cfg = _config("orders_csv.json", SAMPLE_DATA / "orders", table, names["runs"], tmp_path / "n")
    result = run(spark, plugin_root, "csv", "2.0.0", cfg)

    assert result["status"] == "SUCCEEDED_WITH_ERRORS"
    assert (result["rows_read"], result["rows_written"], result["rows_quarantined"]) == (10, 6, 4)
    target = spark.table(table)
    assert "_source_file" in target.columns
    assert target.filter("order_id = 1004").first()["customer_id"] == "C004"
    assert set(r["_plugin_version"] for r in target.collect()) == {"2.0.0"}


def test_both_csv_versions_run_side_by_side_in_one_session(spark, plugin_root, names, tmp_path):
    results = {}
    for version in ("1.0.0", "2.0.0", "1.0.0"):
        table = f"{names['prefix']}_orders_" + version.replace(".", "_")
        cfg = _config("orders_csv.json", SAMPLE_DATA / "orders", table, names["runs"], tmp_path / "n",
                      "target.mode=overwrite")
        results.setdefault(version, []).append(run(spark, plugin_root, "csv", version, cfg))
    assert [r["rows_read"] for r in results["1.0.0"]] == [11, 11]
    assert [r["rows_read"] for r in results["2.0.0"]] == [10]
    runs = spark.table(names["runs"]).groupBy("plugin_version").count().collect()
    assert {r["plugin_version"]: r["count"] for r in runs} == {"1.0.0": 2, "2.0.0": 1}


def test_pipe_delimited_python_plugin(spark, plugin_root, names, tmp_path):
    table = f"{names['prefix']}_customers"
    cfg = _config("customers_pipe.json", SAMPLE_DATA / "customers", table, names["runs"], tmp_path / "n")
    result = run(spark, plugin_root, "pipe_delimited", "1.0.0", cfg)

    assert result["status"] == "SUCCEEDED_WITH_ERRORS"
    assert result["plugin_kind"] == "python"
    assert (result["rows_read"], result["rows_written"], result["rows_quarantined"]) == (8, 5, 3)
    assert dict(spark.table(table).dtypes)["signup_date"] == "date"
    quarantine = spark.table(f"{table}_quarantine").collect()
    assert all(row["_plugin"] == "pipe_delimited" and row["_record"] for row in quarantine)
    run_row = spark.table(names["runs"]).first()
    assert (run_row["plugin_kind"], run_row["status"]) == ("python", "SUCCEEDED_WITH_ERRORS")


def test_failing_python_plugin_is_recorded_and_notified(spark, plugin_root, names, tmp_path):
    d = plugin_root / "broken" / "1.0.0"
    d.mkdir(parents=True)
    (d / "broken_plugin.py").write_text(
        "from connector_framework.plugin_api import SourcePlugin\n"
        "class Broken(SourcePlugin):\n"
        "    name = 'broken'\n"
        "    version = '1.0.0'\n"
        "    def read(self, spark, options):\n"
        "        raise ConnectionError('source system unreachable')\n"
    )
    cfg = {"target": {"table": f"{names['prefix']}_broken", "runs_table": names["runs"]},
           "notifications": {"dir": str(tmp_path / "n")}}
    result = run(spark, plugin_root, "broken", "1.0.0", cfg)

    assert result["status"] == "FAILED"
    assert "source system unreachable" in result["message"]
    assert spark.table(names["runs"]).first()["status"] == "FAILED"
    assert _notifications(tmp_path / "n")[0]["severity"] == "ERROR"
    assert not spark.catalog.tableExists(f"{names['prefix']}_broken")


def test_bad_java_plugin_jar_is_a_recorded_failure(spark, plugin_root, names, tmp_path):
    d = plugin_root / "csv" / "7.0.0"
    d.mkdir()
    (d / "csv-plugin-7.0.0.jar").write_bytes((plugin_root / "csv" / "1.0.0" / "csv-plugin-1.0.0.jar").read_bytes())
    cfg = _config("orders_csv.json", SAMPLE_DATA / "orders", f"{names['prefix']}_x", names["runs"], tmp_path / "n")
    result = run(spark, plugin_root, "csv", "7.0.0", cfg)
    assert result["status"] == "FAILED"
    assert "expected csv 7.0.0" in result["message"]


def test_cli_entry_point_fails_the_task_on_a_failed_run(spark, plugin_root, names, tmp_path):
    args = ["--plugin-root", str(plugin_root), "--plugin-name", "csv", "--plugin-version", "2.0.0",
            "--config-file", str(CONFIG_DIR / "orders_csv.json"),
            "--set", f"source.path={SAMPLE_DATA / 'orders'}",
            "--set", f"target.table={names['prefix']}_cli",
            "--set", "validation.max_bad_row_fraction=0.1"]
    with pytest.raises(ConnectorRunFailed, match="max_bad_row_fraction"):
        main(args, spark=spark)

    ok = main(args[:-2] + ["--set", "validation.max_bad_row_fraction=0.5"], spark=spark)
    assert ok["status"] == "SUCCEEDED_WITH_ERRORS"
