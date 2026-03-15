from __future__ import annotations

from dataclasses import dataclass

from services.jobs_core import JOB_DEFS, JobDef


@dataclass(frozen=True, slots=True)
class JobModule:
    key: str

    def definition(self) -> JobDef:
        return JOB_DEFS[self.key]
