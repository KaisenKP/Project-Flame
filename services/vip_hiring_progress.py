from __future__ import annotations

import logging
import time

import discord

from db.models import VipHiringJobRow

log = logging.getLogger(__name__)


_PROGRESS_MESSAGE_CACHE: dict[int, discord.Message] = {}
_PROGRESS_STATE_CACHE: dict[int, tuple[str, float]] = {}
_PROGRESS_EDIT_MIN_INTERVAL_SECONDS = 1.5


def _progress_signature(job: VipHiringJobRow) -> str:
    return "|".join(
        (
            str(job.status),
            str(int(job.requested_count or 0)),
            str(int(job.processed_count or 0)),
            str(int(job.success_count or 0)),
            str(int(job.skipped_count or 0)),
            str(int(job.failed_count or 0)),
            str(job.error_summary or ""),
        )
    )


def build_progress_embed(job: VipHiringJobRow) -> discord.Embed:
    e = discord.Embed(title="VIP Hiring Job", color=discord.Color.gold())
    e.add_field(name="Job", value=f"`{job.job_id}`", inline=True)
    e.add_field(name="Mode", value=str(job.mode).title(), inline=True)
    e.add_field(name="Status", value=str(job.status), inline=True)
    e.add_field(
        name="Counts",
        value=(
            f"Requested: **{int(job.requested_count or 0)}**\n"
            f"Processed: **{int(job.processed_count or 0)}**\n"
            f"Success: **{int(job.success_count or 0)}**\n"
            f"Skipped: **{int(job.skipped_count or 0)}**\n"
            f"Failed: **{int(job.failed_count or 0)}**"
        ),
        inline=False,
    )
    if job.error_summary:
        e.add_field(name="Notes", value=str(job.error_summary)[:1000], inline=False)
    return e


async def upsert_progress_message(*, bot, job: VipHiringJobRow) -> None:
    signature = _progress_signature(job)
    now = time.monotonic()
    cached_state = _PROGRESS_STATE_CACHE.get(int(job.id))
    if cached_state is not None:
        last_sig, last_edit_ts = cached_state
        if last_sig == signature:
            return
        if (now - last_edit_ts) < _PROGRESS_EDIT_MIN_INTERVAL_SECONDS and str(job.status) == "running":
            return

    channel = None
    if job.progress_message_channel_id:
        channel = bot.get_channel(int(job.progress_message_channel_id))
        if channel is None:
            try:
                channel = await bot.fetch_channel(int(job.progress_message_channel_id))
            except Exception:
                channel = None
    if channel is None:
        return

    embed = build_progress_embed(job)
    try:
        if job.progress_message_id:
            msg = _PROGRESS_MESSAGE_CACHE.get(int(job.progress_message_id))
            if msg is None:
                msg = await channel.fetch_message(int(job.progress_message_id))
                _PROGRESS_MESSAGE_CACHE[int(job.progress_message_id)] = msg
            await msg.edit(embed=embed)
        else:
            msg = await channel.send(embed=embed)
            job.progress_message_id = int(msg.id)
            _PROGRESS_MESSAGE_CACHE[int(msg.id)] = msg
        _PROGRESS_STATE_CACHE[int(job.id)] = (signature, now)
    except Exception:
        log.exception("vip_hiring_progress_update_failed job_id=%s", job.job_id)
