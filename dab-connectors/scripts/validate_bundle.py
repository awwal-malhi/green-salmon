"""Offline check of databricks.yml + resources/*.yml against the bundle JSON schema shipped with the CLI.

    databricks bundle schema > build/bundle-schema.json
    python scripts/validate_bundle.py build/bundle-schema.json

`databricks bundle validate` needs a workspace; this runs without one. The schema's "${...}" interpolation
patterns use Unicode classes (\\p{L}, \\p{N}) that Python's re lacks; they are rewritten to ASCII equivalents.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import jsonschema
import yaml

ROOT = Path(__file__).resolve().parents[1]


def _python_patterns(node):
    if isinstance(node, dict):
        if isinstance(node.get("pattern"), str):
            node["pattern"] = (node["pattern"].replace(r"[\p{L}\p{N}]", "[A-Za-z0-9]")
                               .replace(r"\p{L}", "[A-Za-z]").replace(r"\p{N}", "[0-9]"))
        for value in node.values():
            _python_patterns(value)
    elif isinstance(node, list):
        for value in node:
            _python_patterns(value)


def _merged_bundle() -> dict:
    root = yaml.safe_load((ROOT / "databricks.yml").read_text())
    resources = root.setdefault("resources", {})
    for path in sorted((ROOT / "resources").glob("*.yml")):
        for kind, items in (yaml.safe_load(path.read_text()).get("resources") or {}).items():
            resources.setdefault(kind, {}).update(items)
    return root


def main(schema_path: str) -> int:
    schema = json.loads(Path(schema_path).read_text())
    _python_patterns(schema)
    errors = list(jsonschema.Draft7Validator(schema).iter_errors(_merged_bundle()))
    for error in errors:
        print("/".join(map(str, error.absolute_path)), "-", error.message[:300])
        for cause in error.context or []:
            print("    ", "/".join(map(str, cause.absolute_path)), "-", cause.message[:200])
    print(f"bundle schema check: {len(errors)} error(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
