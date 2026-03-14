from __future__ import annotations

import discord

from services.config import VIP_ROLE_ID


def is_vip_member(member: discord.abc.User | discord.Member | None) -> bool:
    if VIP_ROLE_ID <= 0:
        return False
    if member is None:
        return False
    if not isinstance(member, discord.Member):
        return False
    return any(r.id == VIP_ROLE_ID for r in member.roles)
