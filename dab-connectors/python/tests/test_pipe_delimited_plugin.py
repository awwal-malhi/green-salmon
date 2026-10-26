import pytest

from conftest import SAMPLE_DATA
from connector_framework.plugin_api import CORRUPT_RECORD_COLUMN
from connector_framework.python_loader import PluginLoadError, load
from connector_framework.resolver import resolve


@pytest.fixture
def plugin(plugin_root):
    return load(resolve(plugin_root, "pipe_delimited", "1.0.0"))


def test_reads_pipe_delimited_file_as_strings(spark, plugin):
    df = plugin.read(spark, {"path": str(SAMPLE_DATA / "customers")})
    assert df.columns == ["customer_id", "name", "email", "signup_date", "tier", CORRUPT_RECORD_COLUMN]
    assert all(t == "string" for _, t in df.dtypes)
    rows = {r["customer_id"]: r for r in df.collect()}
    assert rows["C001"]["email"] == "ada@example.com"
    assert rows["C001"][CORRUPT_RECORD_COLUMN] is None
    assert rows["C007"][CORRUPT_RECORD_COLUMN] is not None  # extra field
    assert df.count() == 8


def test_explicit_columns_without_header(spark, plugin, tmp_path):
    f = tmp_path / "x.psv"
    f.write_text("1|a\n2|b\n")
    df = plugin.read(spark, {"path": str(f), "header": False, "columns": ["id", "v"]})
    assert [r["v"] for r in df.orderBy("id").collect()] == ["a", "b"]


def test_path_is_required(spark, plugin):
    with pytest.raises(ValueError, match="path"):
        plugin.read(spark, {})


def test_loader_checks_declared_version(plugin_root):
    src = plugin_root / "pipe_delimited" / "1.0.0" / "pipe_delimited_plugin.py"
    wrong = plugin_root / "pipe_delimited" / "1.1.0"
    wrong.mkdir()
    (wrong / "pipe_delimited_plugin.py").write_text(src.read_text())
    with pytest.raises(PluginLoadError, match="expected pipe_delimited 1.1.0"):
        load(resolve(plugin_root, "pipe_delimited", "1.1.0"))


def test_loader_needs_a_plugin_class(plugin_root):
    d = plugin_root / "empty" / "1.0.0"
    d.mkdir(parents=True)
    (d / "nothing.py").write_text("X = 1\n")
    with pytest.raises(PluginLoadError, match="exactly one SourcePlugin"):
        load(resolve(plugin_root, "empty", "1.0.0"))
