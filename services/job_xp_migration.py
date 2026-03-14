# services/job_xp_migration.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Protocol, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import JobProgressRow


_META_RE = re.compile(r"^\[P(?P<p>\d+)\]\s*(?P<title>.+?)\s*$")


def encode_title(prestige: int, title: str) -> str:
    p = max(int(prestige), 0)
    t = (title or "").strip() or "Recruit"
    return f"[P{p}] {t}"


def decode_title(stored: Optional[str]) -> Tuple[int, str]:
    s = (stored or "").strip()
    if not s:
        return 0, "Recruit"
    m = _META_RE.match(s)
    if not m:
        return 0, s
    p = int(m.group("p"))
    title = (m.group("title") or "").strip() or "Recruit"
    return max(p, 0), title


class JobMetaLookup(Protocol):
    async def get_meta(self, session: AsyncSession, *, job_id: int) -> Tuple[str, object]: ...


# ------------------------------------------------------------
# Import the progression stepper from your existing engine
# ------------------------------------------------------------
def _get_advance_levels_fn():
    import services.jobs_core as jc

    # Candidate names, because your module might not match mine 1:1
    for name in ("_advance_levels", "advance_levels", "_progress_advance", "_advance"):
        fn = getattr(jc, name, None)
        if callable(fn):
            return fn

    # If we got here, we can't run migration safely.
    exported = [k for k in dir(jc) if not k.startswith("__")]
    raise ImportError(
        "Could not find a progression step function in services.jobs_core.\n"
        "I expected a callable like `_advance_levels`.\n"
        "Found these names:\n"
        + ", ".join(exported[:200])
    )


_advance_levels = _get_advance_levels_fn()


# ------------------------------------------------------------
# Deterministic forward sim: total_xp -> (prestige, level, xp_into, title)
# ------------------------------------------------------------
def _state_from_total_xp(*, tier, job_key: str, total_xp: int) -> Tuple[int, int, int, str]:
    # Start from base state. Title value here doesn't matter much; engine will update on prestige.
    state, _leveled, _prestiged = _advance_levels(
        tier=tier,
        job_key=job_key,
        prestige=0,
        title="Recruit",
        level=1,
        xp_into=0,
        add_xp=max(int(total_xp), 0),
    )
    # state is expected to have .prestige .level .xp_into .title
    return int(state.prestige), int(state.level), int(state.xp_into), str(state.title)


def _cmp_state(a: Tuple[int, int, int], b: Tuple[int, int, int]) -> int:
    # lexicographic compare by (prestige, level, xp_into)
    if a[0] != b[0]:
        return -1 if a[0] < b[0] else 1
    if a[1] != b[1]:
        return -1 if a[1] < b[1] else 1
    if a[2] != b[2]:
        return -1 if a[2] < b[2] else 1
    return 0


def _find_total_xp_for_state(*, tier, job_key: str, target: Tuple[int, int, int]) -> int:
    """
    Find the exact total_xp that results in the given (prestige, level, xp_into).
    Uses exponential search + binary search over the monotonic progress function.
    """
    # Quick win
    p0, l0, x0, _ = _state_from_total_xp(tier=tier, job_key=job_key, total_xp=0)
    if _cmp_state((p0, l0, x0), target) == 0:
        return 0

    # 1) Exponential search for an upper bound
    lo = 0
    hi = 1

    while True:
        p, l, x, _ = _state_from_total_xp(tier=tier, job_key=job_key, total_xp=hi)
        if _cmp_state((p, l, x), target) >= 0:
            break
        lo = hi
        hi *= 2
        if hi > 2_000_000_000:
            raise RuntimeError(
                f"Search blew past 2,000,000,000 total XP for job_key={job_key}. "
                f"Target={target}. Something is off."
            )

    # 2) Binary search for the smallest total_xp that reaches target
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        p, l, x, _ = _state_from_total_xp(tier=tier, job_key=job_key, total_xp=mid)
        if _cmp_state((p, l, x), target) >= 0:
            hi = mid
        else:
            lo = mid

    # Verify exact match
    p, l, x, _ = _state_from_total_xp(tier=tier, job_key=job_key, total_xp=hi)
    if _cmp_state((p, l, x), target) != 0:
        raise RuntimeError(
            f"Could not solve total_xp for target={target} job_key={job_key}. "
            f"Closest={ (p, l, x) } @ total_xp={hi}"
        )

    return hi


# ------------------------------------------------------------
# Public API used by the cog
# ------------------------------------------------------------
async def migrate_job_xp_multiplier(
    session: AsyncSession,
    *,
    factor: float = 10.0,
    lookup: JobMetaLookup,
    guild_id: int | None = None,
    dry_run: bool = True,
) -> int:
    """
    Retroactively boosts job progress as if users had earned (old_total_xp * factor).
    Uses the engine's own progression stepper + binary search so it matches your current logic.

    Returns number of rows touched.
    """
    if factor <= 0:
        return 0

    q = select(JobProgressRow)
    if guild_id is not None:
        q = q.where(JobProgressRow.guild_id == int(guild_id))

    rows = (await session.scalars(q)).all()
    touched = 0

    for row in rows:
        old_p, old_title = decode_title(row.job_title)
        old_level = max(int(row.job_level), 1)
        old_into = max(int(row.job_xp), 0)

        job_key, tier = await lookup.get_meta(session, job_id=int(row.job_id))
        key = str(job_key).strip().lower()

        target = (int(old_p), int(old_level), int(old_into))
        old_total = _find_total_xp_for_state(tier=tier, job_key=key, target=target)
        new_total = int(old_total * float(factor))

        new_p, new_lvl, new_into, new_title = _state_from_total_xp(
            tier=tier,
            job_key=key,
            total_xp=new_total,
        )

        row.job_level = int(new_lvl)
        row.job_xp = int(new_into)
        row.job_title = encode_title(int(new_p), str(new_title) or old_title or "Recruit")

        touched += 1

    if not dry_run:
        await session.flush()

    return touched