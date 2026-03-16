from __future__ import annotations

import asyncio
import random

import discord
from sqlalchemy import select

from adventure.decision_system import DecisionView
from adventure.models.adventure_state import ADVENTURE_STAMINA_COST, AdventureMode
from adventure.models.player_runtime import PlayerRuntime
from adventure.models.rewards import AdventureRewards
from adventure.reward_engine import RewardEngine
from adventure.scenario_registry import load_stage_pool
from adventure.stage_resolver import StageResolver
from adventure.utils.rng import roll_bp
from adventure.utils.stage_selector import pick_stage
from db.models import AdventureProfileRow
from services.db import sessions
from services.stamina import StaminaService
from services.users import ensure_user_rows


class AdventureEngine:
    def __init__(self):
        self.sessionmaker = sessions()
        self.stamina = StaminaService()
        self.reward_engine = RewardEngine()
        self.stage_resolver = StageResolver(self.reward_engine)
        self.stage_pool = load_stage_pool()

    async def get_or_create_profile(self, *, guild_id: int, user_id: int) -> AdventureProfileRow:
        async with self.sessionmaker() as session:
            await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
            row = await session.scalar(select(AdventureProfileRow).where(AdventureProfileRow.guild_id == guild_id, AdventureProfileRow.user_id == user_id))
            if row is None:
                row = AdventureProfileRow(guild_id=guild_id, user_id=user_id)
                session.add(row)
                await session.flush()
            await session.commit()
            return row

    async def set_class(self, *, guild_id: int, user_id: int, class_key: str) -> None:
        async with self.sessionmaker() as session:
            row = await session.scalar(select(AdventureProfileRow).where(AdventureProfileRow.guild_id == guild_id, AdventureProfileRow.user_id == user_id))
            if row is None:
                row = AdventureProfileRow(guild_id=guild_id, user_id=user_id)
                session.add(row)
                await session.flush()
            row.class_key = class_key
            await session.commit()

    async def can_join(self, *, guild_id: int, user: discord.abc.User) -> tuple[bool, str]:
        profile = await self.get_or_create_profile(guild_id=guild_id, user_id=int(user.id))
        if not profile.class_key:
            return False, "Choose an Adventure Class first via `/adventure`."
        role_ids = {int(r.id) for r in getattr(user, "roles", [])}
        async with self.sessionmaker() as session:
            snap = await self.stamina.get_snapshot(session, guild_id=guild_id, user_id=int(user.id), role_ids=role_ids)
            await session.commit()
        if int(snap.current) < ADVENTURE_STAMINA_COST:
            return False, f"Need **{ADVENTURE_STAMINA_COST} stamina** to join (you have {snap.current})."
        return True, "ok"

    async def build_player(self, *, guild: discord.Guild, user: discord.abc.User) -> PlayerRuntime:
        profile = await self.get_or_create_profile(guild_id=int(guild.id), user_id=int(user.id))
        return PlayerRuntime(user_id=int(user.id), display_name=user.display_name, class_key=profile.class_key, adventure_level=int(profile.adventure_level))

    async def spend_entry_stamina(self, *, guild: discord.Guild, players: list[PlayerRuntime]) -> None:
        for player in players:
            member = guild.get_member(player.user_id)
            role_ids = {int(r.id) for r in getattr(member, "roles", [])} if member else set()
            async with self.sessionmaker() as session:
                await self.stamina.try_spend(session, guild_id=int(guild.id), user_id=player.user_id, cost=ADVENTURE_STAMINA_COST, role_ids=role_ids)
                await session.commit()

    async def start_adventure(self, *, guild: discord.Guild, channel: discord.TextChannel, players: list[PlayerRuntime], mode: AdventureMode) -> None:
        await self.spend_entry_stamina(guild=guild, players=players)
        stage_total = random.randint(2, 3) if mode == AdventureMode.SOLO else random.randint(3, 5)
        rewards = AdventureRewards()
        await channel.send("The trail bends into the unknown...")
        await asyncio.sleep(0.8)
        synergy_rewards, synergy_lines = self.reward_engine.party_synergy_bonus(players=players, stage_total=stage_total)
        rewards.merge(synergy_rewards)
        for line in synergy_lines:
            await channel.send(line)
            await asyncio.sleep(0.6)
        seen_stage_keys: set[str] = set()
        recent_tags = []
        for stage_number in range(1, stage_total + 1):
            lead_level = max(p.adventure_level for p in players)
            stage = pick_stage(
                stages=self.stage_pool,
                mode=mode,
                party_size=len(players),
                adventure_level=lead_level,
                excluded_keys=seen_stage_keys,
                recent_tags=recent_tags,
            )
            seen_stage_keys.add(stage.key)
            recent_tags.append(stage.tag)
            recent_tags = recent_tags[-3:]
            await channel.send(f"## Stage {stage_number}: {stage.title}")
            for beat in stage.beats:
                await channel.send(beat)
                await asyncio.sleep(0.9)
            decision_view = DecisionView(players=players, options=stage.choices, timeout=22.0)
            prompt = await channel.send("What do you do?", view=decision_view)
            await asyncio.sleep(20)
            decision_view.stop()
            try:
                await prompt.edit(view=None)
            except Exception:
                pass
            choice = decision_view.resolve()
            await channel.send(f"🗳️ Decision: **{choice}**")
            stage_rewards, aftermath = self.stage_resolver.resolve_stage(stage=stage, chosen=choice, players=players, mode=mode)
            rewards.merge(stage_rewards)
            for line in aftermath:
                await channel.send(line)
                await asyncio.sleep(0.8)
            if roll_bp(self.reward_engine.rare_event_chance_bp(players=players, stage_tag=stage.tag)):
                rare_rewards, rare_lines = self.reward_engine.resolve_rare_event(
                    stage_tag=stage.tag,
                    party_size=len(players),
                    class_keys=[p.class_key for p in players],
                )
                rewards.merge(rare_rewards)
                for line in rare_lines:
                    await channel.send(line)
                    await asyncio.sleep(0.8)
        await self.reward_engine.grant_all_rewards(guild_id=int(guild.id), players=players, rewards=rewards)
        await channel.send(embed=self.reward_engine.summary_embed(players=players, rewards=rewards))
