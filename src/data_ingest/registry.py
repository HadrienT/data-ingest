"""Discovery of the available sources.

Sources are found by importing every module in `data_ingest.sources` and
collecting the concrete Source subclasses, so adding a source is a matter of
dropping a file in that package. Nothing has to be registered by hand, which
is the point: a list you must remember to update is a list that goes stale.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from typing import Dict, List

from .core.source import Source
from . import sources as sources_package


def _discover() -> Dict[str, Source]:
    found: Dict[str, Source] = {}
    for module_info in pkgutil.iter_modules(sources_package.__path__):
        if module_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{sources_package.__name__}.{module_info.name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if not issubclass(obj, Source) or obj is Source or inspect.isabstract(obj):
                continue
            if obj.__module__ != module.__name__:
                continue  # imported, not defined here
            instance: Source = obj()
            if instance.name in found:
                raise RuntimeError(
                    f"Two sources claim the name {instance.name!r}: "
                    f"{type(found[instance.name]).__name__} and {obj.__name__}"
                )
            found[instance.name] = instance
    return found


def all_sources() -> List[Source]:
    return sorted(_discover().values(), key=lambda s: s.name)


def get_source(name: str) -> Source:
    found = _discover()
    if name not in found:
        available = ", ".join(sorted(found)) or "none"
        raise KeyError(f"Unknown source {name!r}. Available: {available}")
    return found[name]
