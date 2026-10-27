"""Loads a Python plugin from its .py file."""

from __future__ import annotations

import importlib.util
import inspect
import re
import sys

from .plugin_api import SourcePlugin
from .resolver import PluginArtifact


class PluginLoadError(Exception):
    pass


def load(artifact: PluginArtifact) -> SourcePlugin:
    # A module name per name+version, so two versions of one plugin can be loaded in one process.
    module_name = "connector_plugin_" + re.sub(r"\W", "_", f"{artifact.name}_{artifact.version}")
    spec = importlib.util.spec_from_file_location(module_name, artifact.path)
    if spec is None or spec.loader is None:
        raise PluginLoadError(f"Cannot import {artifact.path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise

    classes = [
        obj
        for _, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, SourcePlugin) and obj is not SourcePlugin and obj.__module__ == module_name
        and not inspect.isabstract(obj)
    ]
    if len(classes) != 1:
        raise PluginLoadError(
            f"Expected exactly one SourcePlugin subclass in {artifact.path}, found {[c.__name__ for c in classes]}"
        )
    plugin = classes[0]()
    if plugin.name != artifact.name or plugin.version != artifact.version:
        raise PluginLoadError(
            f"{artifact.path} contains plugin {plugin.name} {plugin.version}, "
            f"expected {artifact.name} {artifact.version}"
        )
    return plugin
