from __future__ import annotations

import random
from datetime import timedelta

from .domain import outcome_label, pick_scenario, run_condition, stage_for_progress
from .util import utc_now


def initialize_run_state(state: dict, *, duration_sec: int, seed: int, roles: dict[int, str]) -> dict:
    now = utc_now()
    tick = max(24, duration_sec // 9)
    state.update(
        {
            "run": {
                "started_at": now.isoformat(),
                "ends_at": (now + timedelta(seconds=duration_sec)).isoformat(),
                "next_tick_at": (now + timedelta(seconds=tick)).isoformat(),
                "tick_sec": tick,
                "progress": 0,
                "alarm": 8,
                "strikes": 0,
                "history": [],
                "seed": seed,
                "roles": {str(k): v for k, v in roles.items()},
                "status": "active",
                "condition": "clean",
            }
        }
    )
    return state


def _parse_iso(ts: str):
    from datetime import datetime

    return datetime.fromisoformat(ts)


def advance_run(state: dict) -> dict:
    run = state.get("run") or {}
    if run.get("status") != "active":
        return state
    now = utc_now()
    next_tick_at = _parse_iso(run["next_tick_at"])
    seed = int(run.get("seed", 1))
    while now >= next_tick_at and run.get("status") == "active":
        rng = random.Random(seed + len(run.get("history", [])) * 17)
        stage = stage_for_progress(int(run.get("progress", 0)))
        roles = set((run.get("roles") or {}).values())
        sc = pick_scenario(stage=stage, rng=rng, coverage=roles, alarm=int(run.get("alarm", 0)))
        run["progress"] = max(0, min(100, int(run.get("progress", 0)) + sc.d_progress))
        run["alarm"] = max(0, min(100, int(run.get("alarm", 0)) + sc.d_alarm))
        run["strikes"] = max(0, min(4, int(run.get("strikes", 0)) + sc.d_strikes))
        run.setdefault("history", []).append(
            {
                "stage": stage,
                "title": sc.title,
                "body": sc.body,
                "d_progress": sc.d_progress,
                "d_alarm": sc.d_alarm,
                "d_strikes": sc.d_strikes,
                "at": next_tick_at.isoformat(),
            }
        )
        run["condition"] = run_condition(alarm=int(run["alarm"]), strikes=int(run["strikes"]))
        if run["strikes"] >= 3 or run["progress"] >= 100 or now >= _parse_iso(run["ends_at"]):
            run["status"] = "complete"
            run["outcome"] = outcome_label(progress=int(run["progress"]), strikes=int(run["strikes"]), alarm=int(run["alarm"]))
            break
        next_tick_at = next_tick_at + timedelta(seconds=int(run.get("tick_sec", 30)))
        run["next_tick_at"] = next_tick_at.isoformat()
    if now >= _parse_iso(run["ends_at"]) and run.get("status") == "active":
        run["status"] = "complete"
        run["outcome"] = outcome_label(progress=int(run.get("progress", 0)), strikes=int(run.get("strikes", 0)), alarm=int(run.get("alarm", 0)))
    state["run"] = run
    return state
