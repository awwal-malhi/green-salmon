"""Finds a plugin artifact by name and version and works out whether it is Java or Python.

Layout (one artifact per version directory)::

    <plugin_root>/<name>/<version>/<something>.jar   -> Java plugin
    <plugin_root>/<name>/<version>/<something>.py    -> Python plugin
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

JAVA = "java"
PYTHON = "python"

_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class PluginResolutionError(Exception):
    pass


@dataclass(frozen=True)
class PluginArtifact:
    name: str
    version: str
    kind: str
    path: Path


def _check(label: str, value: str) -> None:
    if not _SAFE.match(value) or ".." in value:
        raise PluginResolutionError(f"Invalid plugin {label} {value!r}")


def available(plugin_root: str | Path) -> dict[str, list[str]]:
    root = Path(plugin_root)
    if not root.is_dir():
        return {}
    return {
        p.name: sorted(v.name for v in p.iterdir() if v.is_dir())
        for p in sorted(root.iterdir())
        if p.is_dir()
    }


def resolve(plugin_root: str | Path, name: str, version: str) -> PluginArtifact:
    _check("name", name)
    _check("version", version)
    directory = Path(plugin_root) / name / version
    if not directory.is_dir():
        raise PluginResolutionError(
            f"No plugin {name} {version} under {plugin_root}; deployed plugins: {available(plugin_root)}"
        )
    jars = sorted(directory.glob("*.jar"))
    scripts = sorted(p for p in directory.glob("*.py") if not p.name.startswith("_"))
    candidates = jars + scripts
    if len(candidates) != 1:
        raise PluginResolutionError(
            f"Expected exactly one .jar or .py in {directory}, found {[p.name for p in candidates]}"
        )
    artifact = candidates[0]
    return PluginArtifact(name, version, JAVA if artifact.suffix == ".jar" else PYTHON, artifact)
