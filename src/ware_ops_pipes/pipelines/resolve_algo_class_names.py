from __future__ import annotations

import importlib
import inspect
import pkgutil
from functools import lru_cache

import ware_ops_algos.algorithms as algorithms_pkg


@lru_cache(maxsize=1)
def _class_index() -> dict[str, type]:
    classes: dict[str, type] = {}

    for module_info in pkgutil.walk_packages(
        algorithms_pkg.__path__,
        prefix=algorithms_pkg.__name__ + ".",
    ):
        module = importlib.import_module(module_info.name)

        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__.startswith(algorithms_pkg.__name__):
                classes[name] = obj

    return classes


def resolve_algorithm_class(class_name: str) -> type:
    return _class_index()[class_name]