from __future__ import annotations

import importlib
import pkgutil

from adventure.models.adventure_state import StageTemplate
import adventure.scenarios as scenarios_pkg


def load_stage_pool() -> list[StageTemplate]:
    stages: list[StageTemplate] = []
    for modinfo in pkgutil.iter_modules(scenarios_pkg.__path__):
        if modinfo.name.startswith("_"):
            continue
        module = importlib.import_module(f"adventure.scenarios.{modinfo.name}")
        for name in dir(module):
            if name.endswith("_EVENTS"):
                value = getattr(module, name)
                if isinstance(value, list):
                    stages.extend(value)
    return stages
