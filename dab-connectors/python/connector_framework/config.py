"""Run config: a JSON file plus ``key.path=value`` overrides from the job."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Iterable


def load(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Run config {path} must be a JSON object")
    return data


def apply_overrides(config: dict[str, Any], overrides: Iterable[str]) -> dict[str, Any]:
    """Apply ``a.b.c=value`` overrides. Values are parsed as JSON when possible, else kept as strings."""
    result = copy.deepcopy(config)
    for override in overrides:
        if "=" not in override:
            raise ValueError(f"Override {override!r} must look like key.path=value")
        key, raw = override.split("=", 1)
        parts = [p for p in key.strip().split(".") if p]
        if not parts:
            raise ValueError(f"Override {override!r} has an empty key")
        try:
            value: Any = json.loads(raw)
        except json.JSONDecodeError:
            value = raw
        node = result
        for part in parts[:-1]:
            child = node.get(part)
            if not isinstance(child, dict):
                child = {}
                node[part] = child
            node = child
        node[parts[-1]] = value
    return result
