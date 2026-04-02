# services/stamina.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import StaminaRow
from services.config import VIP_ROLE_ID as CONFIG_VIP_ROLE_ID
from services.users import ensure_user_rows


UTC = timezone.utc


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _first_attr(obj_or_cls, names: list[str]) -> str | None:
    for n in names:
        if hasattr(obj_or_cls, n):
            return n
    return None


def _get_int(obj, names: list[str], default: int) -> int:
    name = _first_attr(obj, names)
    if not name:
        return default
    try:
        return int(getattr(obj, name))
    except Exception:
        return default


def _get_dt(obj, names: list[str], default: datetime) -> datetime:
    name = _first_attr(obj, names)
    if not name:
        return default
    val = getattr(obj, name)
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=UTC)
        return val
    return default


def _set(obj, names: list[str], value) -> None:
    name = _first_attr(obj, names)
    if not name:
        return
    setattr(obj, name, value)


@dataclass(frozen=True)
class StaminaSnapshot:
    current: int
    max_stamina: int
    regen_per_hour: int
    is_vip: bool
    updated_at: datetime

    @property
    def max(self) -> int:
        return self.max_stamina


class StaminaService:
    """
    Stamina system:
      - max stamina default: 100
      - regular regen: 10/hour
      - VIP regen: 30/hour

    Backward compat:
      get_snapshot supports is_vip=bool and role_ids=set[int]

    Model compat:
      We DON'T assume column names.
      We detect current stamina field + updated-at field dynamically.
    """

    _CUR_FIELDS = ["stamina", "current", "current_stamina", "stamina_current", "value"]
    # include your real schema field first: last_regen_at
    _UPDATED_FIELDS = [
        "last_regen_at",
        "updated_at",
        "last_updated_at",
        "last_refill_at",
        "refreshed_at",
        "last_updated",
    ]
    _MAX_FIELDS = ["max_stamina", "stamina_max", "max", "maximum"]

    def __init__(
        self,
        *,
        max_stamina_default: int = 100,
        regen_regular_per_hour: int = 10,
        regen_vip_per_hour: int = 30,
        vip_role_id: int | None = None,
    ):
        if vip_role_id is None:
            vip_role_id = int(CONFIG_VIP_ROLE_ID) or None

        self.max_stamina_default = int(max_stamina_default)
        self.regen_regular_per_hour = int(regen_regular_per_hour)
        self.regen_vip_per_hour = int(regen_vip_per_hour)
        self.vip_role_id = vip_role_id

    def _determine_vip(self, *, is_vip: bool | None, role_ids: set[int] | None) -> bool:
        if is_vip is not None:
            return bool(is_vip)
        if role_ids and self.vip_role_id:
            return self.vip_role_id in role_ids
        return False

    def _regen_rate(self, vip: bool) -> int:
        return self.regen_vip_per_hour if vip else self.regen_regular_per_hour

    def _apply_regen(
        self,
        *,
        cur: int,
        max_stamina: int,
        regen_per_hour: int,
        last: datetime,
    ) -> tuple[int, datetime]:
        now = utcnow()
        if last.tzinfo is None:
            last = last.replace(tzinfo=UTC)

        elapsed = now - last
        if elapsed <= timedelta(0):
            return cur, last

        minutes = int(elapsed.total_seconds() // 60)
        if minutes <= 0:
            return cur, last

        gained = int(minutes * (regen_per_hour / 60.0))
        if gained <= 0:
            return cur, last

        new_cur = min(max_stamina, cur + gained)
        advanced = last + timedelta(minutes=minutes)
        return new_cur, advanced

    async def get_snapshot(
        self,
        session: AsyncSession,
        *,
        guild_id: int,
        user_id: int,
        is_vip: bool | None = None,
        role_ids: set[int] | None = None,
    ) -> StaminaSnapshot:
        await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

        vip = self._determine_vip(is_vip=is_vip, role_ids=role_ids)
        regen = self._regen_rate(vip)

        row = await session.scalar(
            select(StaminaRow).where(
                StaminaRow.guild_id == guild_id,
                StaminaRow.user_id == user_id,
            )
        )

        if row is None:
            row = StaminaRow(guild_id=guild_id, user_id=user_id)
            _set(row, self._MAX_FIELDS, self.max_stamina_default)
            _set(row, self._CUR_FIELDS, self.max_stamina_default)
            _set(row, self._UPDATED_FIELDS, utcnow())
            session.add(row)
            await session.flush()

        max_stam = _get_int(row, self._MAX_FIELDS, self.max_stamina_default)

        last = _get_dt(row, self._UPDATED_FIELDS, utcnow())
        cur = _get_int(row, self._CUR_FIELDS, max_stam)

        new_cur, new_updated = self._apply_regen(
            cur=cur,
            max_stamina=max_stam,
            regen_per_hour=regen,
            last=last,
        )

        changed = False

        if new_cur != cur:
            _set(row, self._CUR_FIELDS, new_cur)
            changed = True

        if new_updated != last:
            _set(row, self._UPDATED_FIELDS, new_updated)
            changed = True

        if changed:
            await session.flush()

        final_cur = _get_int(row, self._CUR_FIELDS, new_cur)
        final_max = _get_int(row, self._MAX_FIELDS, max_stam)
        final_updated = _get_dt(row, self._UPDATED_FIELDS, new_updated)

        return StaminaSnapshot(
            current=final_cur,
            max_stamina=final_max,
            regen_per_hour=regen,
            is_vip=vip,
            updated_at=final_updated,
        )

    async def try_spend(
        self,
        session: AsyncSession,
        *,
        guild_id: int,
        user_id: int,
        cost: int,
        is_vip: bool | None = None,
        role_ids: set[int] | None = None,
    ) -> tuple[bool, StaminaSnapshot]:
        cost_i = max(int(cost), 0)

        snap = await self.get_snapshot(
            session,
            guild_id=guild_id,
            user_id=user_id,
            is_vip=is_vip,
            role_ids=role_ids,
        )

        if cost_i <= 0:
            return True, snap

        if snap.current < cost_i:
            return False, snap

        row = await session.scalar(
            select(StaminaRow).where(
                StaminaRow.guild_id == guild_id,
                StaminaRow.user_id == user_id,
            )
        )
        if row is None:
            return False, snap

        cur = _get_int(row, self._CUR_FIELDS, snap.current)
        new_cur = max(int(cur) - cost_i, 0)

        if new_cur != cur:
            _set(row, self._CUR_FIELDS, new_cur)
            _set(row, self._UPDATED_FIELDS, utcnow())
            await session.flush()

        return True, StaminaSnapshot(
            current=new_cur,
            max_stamina=snap.max_stamina,
            regen_per_hour=snap.regen_per_hour,
            is_vip=snap.is_vip,
            updated_at=utcnow(),
        )
