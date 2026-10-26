import json

import pytest

from connector_framework import config


def test_overrides_set_nested_values_and_parse_json(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"source": {"header": True}, "validation": {"max_bad_row_fraction": 0.5}}))
    cfg = config.apply_overrides(
        config.load(path),
        ["source.path=/Volumes/c/s/v/orders", "target.table=c.s.orders", "validation.max_bad_row_fraction=0.1",
         "source.columns=[\"a\",\"b\"]"],
    )
    assert cfg["source"] == {"header": True, "path": "/Volumes/c/s/v/orders", "columns": ["a", "b"]}
    assert cfg["target"]["table"] == "c.s.orders"
    assert cfg["validation"]["max_bad_row_fraction"] == 0.1


def test_value_may_contain_equals_sign():
    assert config.apply_overrides({}, ["a=x=y"]) == {"a": "x=y"}


def test_override_without_equals_is_rejected():
    with pytest.raises(ValueError):
        config.apply_overrides({}, ["just-a-key"])


def test_shipped_configs_are_valid_json():
    from conftest import CONFIG_DIR

    for path in CONFIG_DIR.glob("*.json"):
        assert isinstance(config.load(path), dict), path
