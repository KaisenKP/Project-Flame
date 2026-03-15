from __future__ import annotations

import random

import discord
from sqlalchemy import select

from adventure.models.adventure_state import AdventureClass, PARTY_LOOTBOX_BONUS_BP, StageTag
from adventure.models.player_runtime import PlayerRuntime
from adventure.models.rewards import AdventureRewards
from adventure.utils.formatting import item_lines, lootbox_lines
from adventure.utils.rng import fmt_int, roll_bp
from db.models import AdventureProfileRow, LootboxInventoryRow, WalletRow
from services.db import sessions
from services.items_catalog import ITEMS
from services.items_inventory import add_item
from services.stamina import StaminaService
from services.users import ensure_user_rows
from services.xp_award import award_xp


class RewardEngine:
    def __init__(self):
        self.sessionmaker = sessions()
        self.stamina = StaminaService()

    def roll_lootbox(self, *, rewards: AdventureRewards, party_size: int, stage_tag: StageTag, class_keys: list[str]) -> None:
        chance_bp = 500 + PARTY_LOOTBOX_BONUS_BP.get(party_size, 0)
        if stage_tag in {StageTag.BOSS, StageTag.PUZZLE}:
            chance_bp += 700
        if AdventureClass.TREASURE_HUNTER.value in class_keys:
            chance_bp += 250
        if not roll_bp(chance_bp):
            return
        rarity_roll = random.randint(1, 10_000)
        if rarity_roll <= 15:
            rarity = "legendary"
        elif rarity_roll <= 140:
            rarity = "epic"
        elif rarity_roll <= 2600:
            rarity = "rare"
        else:
            rarity = "common"
        rewards.lootboxes[rarity] = rewards.lootboxes.get(rarity, 0) + 1

    def rare_event_chance_bp(self, *, players: list[PlayerRuntime], stage_tag: StageTag) -> int:
        party_size = len(players)
        lead_level = max(p.adventure_level for p in players)
        chance = 450 + min(lead_level * 12, 500)
        if party_size > 1:
            chance += 200
        if stage_tag in {StageTag.BOSS, StageTag.MYSTIC}:
            chance += 180
        return chance

    def resolve_rare_event(self, *, stage_tag: StageTag, party_size: int, class_keys: list[str]) -> tuple[AdventureRewards, list[str]]:
        rewards = AdventureRewards()
        weighted_events: list[tuple[str, int]] = [
            ("golden_merchant", 24),
            ("traveling_sage", 20),
            ("legendary_chest", 12),
            ("hidden_dungeon", 18),
            ("artifact_shard", 16),
            ("echoing_arsenal", 10),
            ("moonwell_oasis", 14),
        ]
        if stage_tag == StageTag.BOSS:
            weighted_events.append(("war_trophy", 18))
        if stage_tag == StageTag.SOCIAL:
            weighted_events.append(("patron_contract", 18))
        if party_size >= 3:
            weighted_events.append(("traveling_sage", 6))
            weighted_events.append(("patron_contract", 5))
        if AdventureClass.TREASURE_HUNTER.value in class_keys:
            weighted_events.append(("golden_merchant", 8))
            weighted_events.append(("legendary_chest", 6))
        event = random.choices([name for name, _ in weighted_events], weights=[weight for _, weight in weighted_events], k=1)[0]
        if event == "golden_merchant":
            bonus = random.randint(400, 1000)
            rewards.silver += bonus
            rewards.adventure_xp += random.randint(20, 60)
            return rewards, ["✨ Rare Event: A Golden Merchant appears out of nowhere.", f"He nods once and tosses your party **{fmt_int(bonus)} silver** for 'style points'."]
        if event == "traveling_sage":
            xp = random.randint(120, 260)
            rewards.xp += xp
            rewards.adventure_xp += random.randint(35, 75)
            return rewards, ["📜 Rare Event: A traveling sage interrupts your argument about directions.", f"After a cryptic lecture, your party gains **+{fmt_int(xp)} XP** from newfound knowledge."]
        if event == "legendary_chest":
            rewards.lootboxes["legendary"] = rewards.lootboxes.get("legendary", 0) + 1
            return rewards, ["👑 Rare Event: You unearth a rune-locked chest beneath old ruins.", "Inside is a **Legendary Lootbox** pulsing with unstable light."]
        if event == "hidden_dungeon":
            silver = random.randint(700, 1700)
            advxp = random.randint(80, 170)
            rewards.silver += silver
            rewards.adventure_xp += advxp
            return rewards, ["🕳️ Rare Event: A hidden dungeon corridor opens behind a false wall.", f"You salvage relic fragments worth **{fmt_int(silver)} silver** and **+{fmt_int(advxp)} Adventure XP**."]
        if event == "echoing_arsenal":
            xp = random.randint(80, 180)
            rewards.xp += xp
            rewards.items["training_manual"] = rewards.items.get("training_manual", 0) + 1
            return rewards, ["🛡️ Rare Event: You uncover an echoing arsenal sealed in time.", f"Ancient drills grant **+{fmt_int(xp)} XP** and a **{ITEMS['training_manual'].name}**."]
        if event == "moonwell_oasis":
            silver = random.randint(250, 620)
            advxp = random.randint(30, 70)
            rewards.silver += silver
            rewards.adventure_xp += advxp
            rewards.items["energy_drink"] = rewards.items.get("energy_drink", 0) + 1
            return rewards, ["🌙 Rare Event: A moonwell oasis appears between heartbeats.", f"You recover valuables worth **{fmt_int(silver)} silver**, gain **+{fmt_int(advxp)} Adventure XP**, and bottle an **{ITEMS['energy_drink'].name}**."]
        if event == "war_trophy":
            silver = random.randint(500, 1200)
            rewards.silver += silver
            rewards.lootboxes["rare"] = rewards.lootboxes.get("rare", 0) + 1
            return rewards, ["🏆 Rare Event: A fallen warlord cache surfaces after the battle.", f"You salvage **{fmt_int(silver)} silver** and secure a **Rare Lootbox**."]
        if event == "patron_contract":
            xp = random.randint(110, 220)
            silver = random.randint(200, 500)
            rewards.xp += xp
            rewards.silver += silver
            return rewards, ["📝 Rare Event: A hidden patron offers a high-risk contract on the spot.", f"Smart negotiation earns **+{fmt_int(xp)} XP** and **{fmt_int(silver)} silver**."]
        item_key = random.choice(["training_manual", "study_sprint_timer", "adrenaline_patch"])
        rewards.items[item_key] = rewards.items.get(item_key, 0) + 1
        return rewards, ["🧿 Rare Event: An ancient artifact shard hums in your hands.", f"The shard stabilizes into **{ITEMS[item_key].name}**."]

    async def grant_all_rewards(self, *, guild_id: int, players: list[PlayerRuntime], rewards: AdventureRewards) -> None:
        for player in players:
            async with self.sessionmaker() as session:
                await ensure_user_rows(session, guild_id=guild_id, user_id=player.user_id)
                wallet = await session.scalar(select(WalletRow).where(WalletRow.guild_id == guild_id, WalletRow.user_id == player.user_id))
                if wallet is None:
                    wallet = WalletRow(guild_id=guild_id, user_id=player.user_id, silver=0, diamonds=0)
                    session.add(wallet)
                silver_delta = int(rewards.silver)
                if silver_delta >= 0:
                    wallet.silver += silver_delta
                    if hasattr(wallet, "silver_earned"):
                        wallet.silver_earned += silver_delta
                else:
                    wallet.silver = max(int(wallet.silver) + silver_delta, 0)
                if int(rewards.xp) > 0:
                    await award_xp(session, guild_id=guild_id, user_id=player.user_id, amount=int(rewards.xp), apply_weekend_multiplier=False)
                profile = await session.scalar(select(AdventureProfileRow).where(AdventureProfileRow.guild_id == guild_id, AdventureProfileRow.user_id == player.user_id))
                if profile is None:
                    profile = AdventureProfileRow(guild_id=guild_id, user_id=player.user_id)
                    session.add(profile)
                    await session.flush()
                profile.adventure_xp += int(rewards.adventure_xp)
                profile.adventure_level = self.adventure_level_from_xp(int(profile.adventure_xp))
                profile.runs_completed += 1
                profile.total_stage_wins += 1
                for rarity, amount in rewards.lootboxes.items():
                    lb_row = await session.scalar(select(LootboxInventoryRow).where(LootboxInventoryRow.guild_id == guild_id, LootboxInventoryRow.user_id == player.user_id, LootboxInventoryRow.rarity == rarity))
                    if lb_row is None:
                        lb_row = LootboxInventoryRow(guild_id=guild_id, user_id=player.user_id, rarity=rarity, amount=0)
                        session.add(lb_row)
                    lb_row.amount += int(amount)
                for item_key, amount in rewards.items.items():
                    await add_item(session, guild_id=guild_id, user_id=player.user_id, item_key=item_key, qty=int(amount))
                if int(rewards.stamina_penalty) > 0:
                    await self.stamina.try_spend(session, guild_id=guild_id, user_id=player.user_id, cost=int(rewards.stamina_penalty))
                await session.commit()

    @staticmethod
    def adventure_level_from_xp(xp_total: int) -> int:
        xp = max(int(xp_total), 0)
        level = 1
        need = 140
        while xp >= need and level < 100:
            xp -= need
            level += 1
            need = 140 + ((level - 1) * 45)
        return level

    def summary_embed(self, *, players: list[PlayerRuntime], rewards: AdventureRewards) -> discord.Embed:
        em = discord.Embed(title="Adventure Complete", description=f"Party: **{', '.join(p.display_name for p in players)}**", color=discord.Color.green())
        em.add_field(name="Silver", value=f"{rewards.silver:+,}", inline=True)
        em.add_field(name="XP", value=f"+{fmt_int(rewards.xp)}", inline=True)
        em.add_field(name="Adventure XP", value=f"+{fmt_int(rewards.adventure_xp)}", inline=True)
        em.add_field(name="Lootboxes", value=lootbox_lines(rewards.lootboxes), inline=False)
        em.add_field(name="Items", value=item_lines(rewards.items), inline=False)
        if rewards.stamina_penalty > 0:
            em.set_footer(text=f"Extra stamina lost from failed outcomes: {rewards.stamina_penalty}")
        return em
