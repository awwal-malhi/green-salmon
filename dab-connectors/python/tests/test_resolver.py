import pytest

from connector_framework.resolver import JAVA, PYTHON, PluginResolutionError, available, resolve


def test_detects_java_plugin_by_jar(plugin_root):
    artifact = resolve(plugin_root, "csv", "1.0.0")
    assert artifact.kind == JAVA
    assert artifact.path.name == "csv-plugin-1.0.0.jar"


def test_detects_python_plugin_by_py(plugin_root):
    artifact = resolve(plugin_root, "pipe_delimited", "1.0.0")
    assert artifact.kind == PYTHON
    assert artifact.path.suffix == ".py"


def test_both_csv_versions_are_deployed_side_by_side(plugin_root):
    assert available(plugin_root)["csv"] == ["1.0.0", "2.0.0"]
    assert resolve(plugin_root, "csv", "2.0.0").path.name == "csv-plugin-2.0.0.jar"


def test_unknown_version_lists_what_is_deployed(plugin_root):
    with pytest.raises(PluginResolutionError, match="deployed plugins"):
        resolve(plugin_root, "csv", "3.0.0")


def test_directory_with_two_artifacts_is_ambiguous(plugin_root):
    (plugin_root / "csv" / "1.0.0" / "extra.py").write_text("")
    with pytest.raises(PluginResolutionError, match="exactly one"):
        resolve(plugin_root, "csv", "1.0.0")


def test_empty_version_directory_is_rejected(plugin_root):
    (plugin_root / "csv" / "9.9.9").mkdir()
    with pytest.raises(PluginResolutionError, match="found \\[\\]"):
        resolve(plugin_root, "csv", "9.9.9")


@pytest.mark.parametrize("name,version", [("../csv", "1.0.0"), ("csv", ".."), ("csv", "1.0.0/../../x"), ("", "1")])
def test_rejects_path_tricks(plugin_root, name, version):
    with pytest.raises(PluginResolutionError, match="Invalid"):
        resolve(plugin_root, name, version)
