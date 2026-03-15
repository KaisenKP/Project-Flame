from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Dict

from services.jobs_core import JobDef

from .base import JobModule


def _discover_job_modules() -> Dict[str, JobModule]:
    modules: Dict[str, JobModule] = {}
    root = Path(__file__).resolve().parent

    for pkg in root.iterdir():
        if not pkg.is_dir() or pkg.name.startswith("_"):
            continue
        init_file = pkg / "__init__.py"
        if not init_file.exists():
            continue
        module = import_module(f"{__name__}.{pkg.name}")
        job_module = getattr(module, "JOB_MODULE", None)
        if isinstance(job_module, JobModule):
            modules[job_module.key] = job_module

    return dict(sorted(modules.items(), key=lambda item: item[0]))


JOB_MODULES: Dict[str, JobModule] = _discover_job_modules()


def get_job_def(key: str) -> JobDef | None:
    mod = JOB_MODULES.get((key or "").strip().lower())
    if mod is None:
        return None
    return mod.definition()
