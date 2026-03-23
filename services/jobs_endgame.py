from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Optional, Sequence

import discord

from services.jobs_core import JobCategory, JobDef, apply_bp, clamp_int, fmt_int, roll_bp

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobPresentation:
    fantasy: str
    payout_style: str
    risk_level: str
    perk_summary: str
    danger_summary: str
    can_trigger_danger: bool


@dataclass(frozen=True)
class EndgameRandomEventDefinition:
    key: str
    name: str
    description: str
    chance_bp: int
    payout_multiplier_bp: int = 0
    bonus_silver_flat: int = 0
    stamina_delta: int = 0
    fail_override: bool | None = None


@dataclass(frozen=True)
class DangerChoice:
    key: str
    label: str
    style: discord.ButtonStyle
    description: str
    payout_multiplier_bp: int = 0
    flat_bonus: int = 0
    fail_bp: int = 0
    partial_fail_multiplier_bp: int = 0
    jackpot_bonus: int = 0
    jackpot_chance_bp: int = 0
    force_fail: bool = False


@dataclass(frozen=True)
class DangerEncounter:
    key: str
    title: str
    description: str
    stake_text: str
    choices: Sequence[DangerChoice]
    safe_choice_key: str


@dataclass(frozen=True)
class DangerResolution:
    encounter: DangerEncounter
    choice: DangerChoice
    payout: int
    failed: bool
    jackpot: bool
    choice_label: str
    summary_line: str
    detail_line: str
    timed_out: bool = False


JOB_PRESENTATIONS: dict[str, JobPresentation] = {
    "artifact_hunter": JobPresentation(
        fantasy="Legendary relic runner pulling wealth out of lost vaults.",
        payout_style="High variance with massive rare relic spikes.",
        risk_level="Medium risk",
        perk_summary="Massive jackpot potential from rare relic finds.",
        danger_summary="Danger Encounters can force greedy vault decisions.",
        can_trigger_danger=True,
    ),
    "drug_lord": JobPresentation(
        fantasy="Fictional underworld kingpin crushing the city for silver.",
        payout_style="Highest ceiling in the game with brutal swings.",
        risk_level="Extreme risk",
        perk_summary="Highest ceiling in the game, but crackdowns hit hard.",
        danger_summary="Danger Encounters force high-pressure deal and territory choices.",
        can_trigger_danger=True,
    ),
    "dragon_slayer": JobPresentation(
        fantasy="Mythic contract hunter cashing out impossible dragon kills.",
        payout_style="Elite burst income from bounty and rare part spikes.",
        risk_level="Medium-high risk",
        perk_summary="Dragon contracts and rare parts can explode your payout.",
        danger_summary="Danger Encounters force kill, loot, or survival decisions.",
        can_trigger_danger=True,
    ),
    "business_ceo": JobPresentation(
        fantasy="Corporate emperor printing silver through executive dominance.",
        payout_style="The most stable elite income path.",
        risk_level="Low risk",
        perk_summary="The most stable elite income path.",
        danger_summary="Occasional executive crisis choices with safer outcomes.",
        can_trigger_danger=True,
    ),
    "space_miner": JobPresentation(
        fantasy="Deep-space extractor chasing absurd cosmic deposits.",
        payout_style="Wild volatility with cracked jackpot ore pulls.",
        risk_level="High risk",
        perk_summary="Deep-space deposits can turn one run into a fortune.",
        danger_summary="Danger Encounters force drill, stabilize, or eject choices.",
        can_trigger_danger=True,
    ),
}


ENDGAME_EVENT_CATALOG: dict[str, tuple[EndgameRandomEventDefinition, ...]] = {
    "artifact_hunter": (
        EndgameRandomEventDefinition("ancient_vault", "Ancient Vault", "A sealed vault cracks open and boosts the haul.", 850, payout_multiplier_bp=2800),
        EndgameRandomEventDefinition("hidden_reliquary", "Hidden Reliquary", "A secret reliquary adds priceless relic stock.", 720, bonus_silver_flat=340),
        EndgameRandomEventDefinition("royal_collector", "Royal Collector", "A collector starts a bidding war for your find.", 520, payout_multiplier_bp=6500),
        EndgameRandomEventDefinition("cursed_find", "Cursed Find", "The relic is dangerous, but the payoff is huge.", 420, payout_multiplier_bp=9000),
        EndgameRandomEventDefinition("counterfeit_relic", "Counterfeit Relic", "One piece is fake and drags the sale down.", 500, payout_multiplier_bp=-3000),
        EndgameRandomEventDefinition("tomb_collapse", "Tomb Collapse", "You barely escape and only salvage part of the haul.", 380, payout_multiplier_bp=-5000),
    ),
    "drug_lord": (
        EndgameRandomEventDefinition("territory_sweep", "Territory Sweep", "A full sweep of your turf floods the stash.", 820, payout_multiplier_bp=2600),
        EndgameRandomEventDefinition("cartel_shipment", "Cartel Shipment", "A giant shipment lands clean and spikes the run.", 650, bonus_silver_flat=420),
        EndgameRandomEventDefinition("street_monopoly", "Street Monopoly", "You lock down the block and your take surges.", 480, payout_multiplier_bp=8000),
        EndgameRandomEventDefinition("corrupt_official", "Corrupt Official", "A dirty payoff keeps the money flowing.", 430, payout_multiplier_bp=4500),
        EndgameRandomEventDefinition("rival_raid", "Rival Raid", "A rival hit strips part of the take.", 520, payout_multiplier_bp=-4500),
        EndgameRandomEventDefinition("cash_warehouse_hit", "Cash Warehouse Hit", "A warehouse score detonates your payout.", 280, payout_multiplier_bp=12000),
    ),
    "dragon_slayer": (
        EndgameRandomEventDefinition("dragon_nest", "Dragon Nest", "A nest cache adds scorched treasure to the reward.", 760, payout_multiplier_bp=2400),
        EndgameRandomEventDefinition("royal_bounty", "Royal Bounty", "The crown posts a premium bounty on your target.", 650, bonus_silver_flat=360),
        EndgameRandomEventDefinition("heartscale_drop", "Heartscale Drop", "A pristine heartscale drop sells for a fortune.", 500, payout_multiplier_bp=6200),
        EndgameRandomEventDefinition("slayers_trophy", "Slayer's Trophy Bonus", "Your trophy draw turns the hunt legendary.", 360, payout_multiplier_bp=10000),
        EndgameRandomEventDefinition("burned_battlefield", "Burned Battlefield", "Fire ruins part of the contract payout.", 500, payout_multiplier_bp=-3500),
        EndgameRandomEventDefinition("ancient_wyrm", "Ancient Wyrm Hunt", "A mythic contract pays out at absurd rates.", 260, payout_multiplier_bp=11500),
    ),
    "business_ceo": (
        EndgameRandomEventDefinition("major_acquisition", "Major Acquisition", "The board closes a huge acquisition on your terms.", 780, payout_multiplier_bp=1800),
        EndgameRandomEventDefinition("executive_buyout", "Executive Buyout", "A buyout premium lands straight in your pocket.", 620, bonus_silver_flat=260),
        EndgameRandomEventDefinition("investor_surge", "Investor Surge", "Investors flood the round and smooth the quarter.", 560, payout_multiplier_bp=2200),
        EndgameRandomEventDefinition("dividend_explosion", "Dividend Explosion", "A dividend spike makes this shift elite.", 420, payout_multiplier_bp=4800),
        EndgameRandomEventDefinition("global_expansion", "Global Expansion", "A new market opens and lifts the run.", 380, payout_multiplier_bp=6000),
        EndgameRandomEventDefinition("tax_audit", "Tax Audit", "An audit clips some margin but not the whole quarter.", 420, payout_multiplier_bp=-2000),
    ),
    "space_miner": (
        EndgameRandomEventDefinition("void_crystal_vein", "Void Crystal Vein", "A radiant crystal seam blows up the pull.", 800, payout_multiplier_bp=2600),
        EndgameRandomEventDefinition("alien_core_deposit", "Alien Core Deposit", "An alien core deposit adds insane ore value.", 620, bonus_silver_flat=380),
        EndgameRandomEventDefinition("starstorm_harvest", "Starstorm Harvest", "You catch a harvest window inside a starstorm.", 460, payout_multiplier_bp=7000),
        EndgameRandomEventDefinition("reactor_surge", "Reactor Surge", "The reactor screams but output goes crazy.", 420, payout_multiplier_bp=9500),
        EndgameRandomEventDefinition("derelict_drill_site", "Derelict Drill Site", "An abandoned rig still has premium ore inside.", 500, payout_multiplier_bp=3600),
        EndgameRandomEventDefinition("hull_breach", "Hull Breach", "A breach forces you to dump part of the load.", 520, payout_multiplier_bp=-5000),
    ),
}


DANGER_TRIGGER_BP: dict[str, int] = {
    "artifact_hunter": 1400,
    "drug_lord": 1700,
    "dragon_slayer": 1350,
    "business_ceo": 700,
    "space_miner": 1500,
}


DANGER_CATALOG: dict[str, tuple[DangerEncounter, ...]] = {
    "artifact_hunter": (
        DangerEncounter(
            key="collapsing_relic_chamber",
            title="Danger Encounter • Collapsing Relic Chamber",
            description="The chamber is breaking apart and three relics are still inside.",
            stake_text="Greed can print silver here, but the trap can zero the whole run.",
            safe_choice_key="leave_now",
            choices=(
                DangerChoice("grab_larger_relic", "Grab Larger Relic", discord.ButtonStyle.danger, "Huge upside, real trap chance.", payout_multiplier_bp=6500, fail_bp=3300, jackpot_bonus=1200, jackpot_chance_bp=1200),
                DangerChoice("take_safe_relic", "Take Safe Relic", discord.ButtonStyle.primary, "Safer payout with a solid relic bonus.", payout_multiplier_bp=2200, flat_bonus=220, fail_bp=800),
                DangerChoice("leave_now", "Leave Now", discord.ButtonStyle.secondary, "Cash out the run before the vault folds.", payout_multiplier_bp=0),
            ),
        ),
        DangerEncounter(
            key="private_collector_offer",
            title="Danger Encounter • Private Collector",
            description="A masked collector offers instant silver for an unverified relic.",
            stake_text="The risky deal can explode, but a fake piece can wreck the sale.",
            safe_choice_key="cash_out_clean",
            choices=(
                DangerChoice("push_private_sale", "Push Private Sale", discord.ButtonStyle.danger, "All-in sale with jackpot odds.", payout_multiplier_bp=5000, fail_bp=2600, jackpot_bonus=1600, jackpot_chance_bp=1400),
                DangerChoice("verify_then_sell", "Verify Then Sell", discord.ButtonStyle.primary, "Moderate bonus with a much safer result.", payout_multiplier_bp=1600, flat_bonus=180, fail_bp=600),
                DangerChoice("cash_out_clean", "Cash Out Clean", discord.ButtonStyle.secondary, "Skip the gamble and bank the current haul.", payout_multiplier_bp=0),
            ),
        ),
    ),
    "drug_lord": (
        DangerEncounter(
            key="rival_ambush",
            title="Danger Encounter • Rival Ambush",
            description="A rival crew cuts into the deal right before the silver changes hands.",
            stake_text="Force the deal, pay them off, or cut the load and run.",
            safe_choice_key="cut_and_run",
            choices=(
                DangerChoice("push_deal_through", "Push Deal Through", discord.ButtonStyle.danger, "Highest ceiling, highest collapse chance.", payout_multiplier_bp=8000, fail_bp=3800, jackpot_bonus=1800, jackpot_chance_bp=1500),
                DangerChoice("pay_them_off", "Pay Them Off", discord.ButtonStyle.primary, "Smoother result with a smaller but strong take.", payout_multiplier_bp=1800, flat_bonus=320, fail_bp=1000, partial_fail_multiplier_bp=3500),
                DangerChoice("cut_and_run", "Cut And Run", discord.ButtonStyle.secondary, "Protect the run and settle for reduced profit.", payout_multiplier_bp=-1500),
            ),
        ),
        DangerEncounter(
            key="dirty_official",
            title="Danger Encounter • Dirty Official",
            description="A corrupt official offers a window to move massive money fast.",
            stake_text="If the deal lands you crush the ceiling. If it flips, the run is burned.",
            safe_choice_key="walk_from_deal",
            choices=(
                DangerChoice("double_down", "Double Down", discord.ButtonStyle.danger, "Reckless move with absurd upside.", payout_multiplier_bp=9500, fail_bp=4200, jackpot_bonus=2400, jackpot_chance_bp=1800),
                DangerChoice("smooth_it_over", "Smooth It Over", discord.ButtonStyle.primary, "Pay for a cleaner lane and keep solid upside.", payout_multiplier_bp=2600, flat_bonus=260, fail_bp=1200, partial_fail_multiplier_bp=4000),
                DangerChoice("walk_from_deal", "Walk From Deal", discord.ButtonStyle.secondary, "Bank the shift and avoid the trap.", payout_multiplier_bp=0),
            ),
        ),
    ),
    "dragon_slayer": (
        DangerEncounter(
            key="enraged_finisher",
            title="Danger Encounter • Enraged Dragon",
            description="The dragon is wounded, furious, and one strike from either death or escape.",
            stake_text="You can go for the killing blow, secure the current loot, or harvest rare parts fast.",
            safe_choice_key="secure_loot",
            choices=(
                DangerChoice("killing_blow", "Go For Kill", discord.ButtonStyle.danger, "Massive bounty ceiling if you land it.", payout_multiplier_bp=7000, fail_bp=3100, jackpot_bonus=1500, jackpot_chance_bp=1400),
                DangerChoice("harvest_organs", "Harvest Rare Part", discord.ButtonStyle.primary, "Best rare-part angle with moderate danger.", payout_multiplier_bp=3400, flat_bonus=280, fail_bp=1400),
                DangerChoice("secure_loot", "Secure Loot", discord.ButtonStyle.secondary, "Take the clean payout and leave alive.", payout_multiplier_bp=500),
            ),
        ),
        DangerEncounter(
            key="unstable_nest",
            title="Danger Encounter • Unstable Nest",
            description="The nest is full of scorched treasure and the ground is starting to crack.",
            stake_text="Stay greedy for a legendary haul or escape before the fire takes everything.",
            safe_choice_key="extract_now",
            choices=(
                DangerChoice("deep_loot", "Deep Loot", discord.ButtonStyle.danger, "Greedy pull with huge contract upside.", payout_multiplier_bp=6200, fail_bp=2800, jackpot_bonus=1100, jackpot_chance_bp=1200),
                DangerChoice("trophy_grab", "Take Trophy", discord.ButtonStyle.primary, "Grab a premium trophy with strong upside.", payout_multiplier_bp=2400, flat_bonus=220, fail_bp=900),
                DangerChoice("extract_now", "Extract Now", discord.ButtonStyle.secondary, "Lock the base win and get out.", payout_multiplier_bp=0),
            ),
        ),
    ),
    "business_ceo": (
        DangerEncounter(
            key="hostile_takeover",
            title="Danger Encounter • Hostile Takeover Window",
            description="A rival company is exposed and the board wants an instant decision.",
            stake_text="Push for the windfall, hedge for stable profit, or exit before the market bites.",
            safe_choice_key="exit_early",
            choices=(
                DangerChoice("double_down", "Double Down", discord.ButtonStyle.danger, "Biggest CEO spike, but the quarter can crater.", payout_multiplier_bp=4200, fail_bp=1800, jackpot_bonus=900, jackpot_chance_bp=900),
                DangerChoice("hedge_position", "Hedge Position", discord.ButtonStyle.primary, "Most reliable bonus outcome.", payout_multiplier_bp=1800, flat_bonus=180, fail_bp=400, partial_fail_multiplier_bp=7000),
                DangerChoice("exit_early", "Exit Early", discord.ButtonStyle.secondary, "Bank the current quarter and avoid a disaster.", payout_multiplier_bp=200),
            ),
        ),
    ),
    "space_miner": (
        DangerEncounter(
            key="volatile_ore_pocket",
            title="Danger Encounter • Volatile Ore Pocket",
            description="A glowing ore pocket is building pressure inside the rig.",
            stake_text="Overclock for a cracked haul, stabilize for safer value, or dump the load before it blows.",
            safe_choice_key="stabilize_rig",
            choices=(
                DangerChoice("overclock_drill", "Overclock Drill", discord.ButtonStyle.danger, "Highest spike potential with serious reactor risk.", payout_multiplier_bp=7800, fail_bp=3400, jackpot_bonus=1700, jackpot_chance_bp=1500),
                DangerChoice("stabilize_rig", "Stabilize Rig", discord.ButtonStyle.primary, "Safer crystal pull with a good bump.", payout_multiplier_bp=2600, flat_bonus=260, fail_bp=900),
                DangerChoice("eject_load", "Eject Load", discord.ButtonStyle.secondary, "Protect the run and settle for less.", payout_multiplier_bp=-1800),
            ),
        ),
        DangerEncounter(
            key="alien_core_exposed",
            title="Danger Encounter • Exposed Alien Core",
            description="An alien core is live in the rock and one wrong move can vaporize the cargo bay.",
            stake_text="Push for an absurd payday or play it clean and keep the rig intact.",
            safe_choice_key="seal_core",
            choices=(
                DangerChoice("rip_core_out", "Rip Core Out", discord.ButtonStyle.danger, "Extreme jackpot path with explosion odds.", payout_multiplier_bp=9000, fail_bp=3900, jackpot_bonus=2200, jackpot_chance_bp=1800),
                DangerChoice("seal_core", "Seal And Extract", discord.ButtonStyle.primary, "Strong payout with controlled risk.", payout_multiplier_bp=3000, flat_bonus=300, fail_bp=1200),
                DangerChoice("abort_site", "Abort Site", discord.ButtonStyle.secondary, "Leave with the current haul before the bay goes red.", payout_multiplier_bp=0),
            ),
        ),
    ),
}


def presentation_for(job_key: str) -> Optional[JobPresentation]:
    return JOB_PRESENTATIONS.get((job_key or "").strip().lower())


def event_defs_for_endgame(job_key: str) -> tuple[EndgameRandomEventDefinition, ...]:
    return ENDGAME_EVENT_CATALOG.get((job_key or "").strip().lower(), ())


def should_trigger_danger(job_key: str) -> bool:
    key = (job_key or "").strip().lower()
    return roll_bp(DANGER_TRIGGER_BP.get(key, 0))


def pick_danger_encounter(job_key: str) -> Optional[DangerEncounter]:
    pool = DANGER_CATALOG.get((job_key or "").strip().lower(), ())
    if not pool:
        return None
    return random.choice(pool)


def resolve_danger_choice(*, encounter: DangerEncounter, choice_key: str, payout: int) -> DangerResolution:
    payout = max(int(payout), 0)
    choice = next((item for item in encounter.choices if item.key == choice_key), encounter.choices[0])

    failed = False
    jackpot = False
    updated = payout

    if choice.force_fail:
        failed = True
        updated = 0
    elif choice.fail_bp > 0 and roll_bp(choice.fail_bp):
        failed = True
        if choice.partial_fail_multiplier_bp > 0:
            updated = max((updated * choice.partial_fail_multiplier_bp) // 10_000, 0)
            failed = False
        else:
            updated = 0
    else:
        updated = max((updated * (10_000 + int(choice.payout_multiplier_bp))) // 10_000, 0)
        updated += int(choice.flat_bonus)
        if choice.jackpot_bonus > 0 and choice.jackpot_chance_bp > 0 and roll_bp(choice.jackpot_chance_bp):
            jackpot = True
            updated += int(choice.jackpot_bonus)

    if failed:
        summary = "Danger play failed."
        detail = "The gamble blew up and the shift paid nothing."
    elif jackpot:
        summary = "Jackpot hit."
        detail = f"The danger play detonated into **+{fmt_int(choice.jackpot_bonus)}** Silver."
    elif choice.payout_multiplier_bp < 0:
        summary = "Safe escape."
        detail = "You protected the run but gave up part of the silver."
    else:
        summary = "Danger play paid off."
        detail = "Your choice converted the pressure into extra silver."

    return DangerResolution(
        encounter=encounter,
        choice=choice,
        payout=max(int(updated), 0),
        failed=failed,
        jackpot=jackpot,
        choice_label=choice.label,
        summary_line=summary,
        detail_line=detail,
    )


def build_danger_embed(*, user: discord.abc.User, d: JobDef, encounter: DangerEncounter, payout: int) -> discord.Embed:
    color = discord.Color.red() if d.category == JobCategory.HARD else discord.Color.orange()
    lines = [f"**{choice.label}** — {choice.description}" for choice in encounter.choices]
    embed = discord.Embed(
        title=encounter.title,
        description=(
            f"**{d.name}**\n"
            f"{encounter.description}\n\n"
            f"**Current haul:** **{fmt_int(payout)}** Silver\n"
            f"**At stake:** {encounter.stake_text}\n\n"
            f"**Choices**\n" + "\n".join(lines)
        ),
        color=color,
    )
    embed.set_author(name=str(user), icon_url=getattr(getattr(user, "display_avatar", None), "url", None))
    embed.set_footer(text="Choose once. Timeout auto-picks the safest option.")
    return embed


def build_danger_result_embed(*, user: discord.abc.User, d: JobDef, resolution: DangerResolution, stamina_cost: int, user_xp: int, job_xp: int, progress_after, next_job_name: str | None, xp_needed_value: int) -> discord.Embed:
    outcome = "❌ FAILED" if resolution.failed else "✅ DANGER RESOLVED"
    embed = discord.Embed(
        title=f"{d.name} Danger Result",
        description=(
            f"**{resolution.encounter.title.replace('Danger Encounter • ', '')}**\n"
            f"Choice: **{resolution.choice_label}**\n"
            f"{resolution.summary_line}\n"
            f"{resolution.detail_line}\n\n"
            f"{outcome} • 💰 **{fmt_int(resolution.payout)}** Silver • ⚡ **-{fmt_int(stamina_cost)}** Stamina"
        ),
        color=discord.Color.gold() if resolution.jackpot else (discord.Color.red() if resolution.failed else discord.Color.blurple()),
    )
    embed.set_author(name=str(user), icon_url=getattr(getattr(user, "display_avatar", None), "url", None))
    embed.add_field(
        name="Progress",
        value=(
            f"Prestige **{int(progress_after.prestige)}** • Level **{int(progress_after.level)}**\n"
            f"Job XP **{fmt_int(int(progress_after.xp))}/{fmt_int(max(int(xp_needed_value), 1))}**"
        ),
        inline=False,
    )
    embed.add_field(name="Gains", value=f"🧠 User XP: **+{fmt_int(user_xp)}**\n🧰 Job XP: **+{fmt_int(job_xp)}**", inline=False)
    if resolution.timed_out:
        embed.add_field(name="Timeout", value="You hesitated, so the safest option resolved automatically.", inline=False)
    if next_job_name:
        embed.set_footer(text=f"Next shift: {next_job_name} • Use /job to edit your 3 job slots")
    return embed


class DangerEncounterView(discord.ui.View):
    def __init__(
        self,
        *,
        owner_id: int,
        timeout_seconds: float,
        encounter: DangerEncounter,
        resolver,
    ):
        super().__init__(timeout=timeout_seconds)
        self.owner_id = int(owner_id)
        self.encounter = encounter
        self.resolver = resolver
        self._resolved = False
        self._lock = asyncio.Lock()
        self._message: Optional[discord.Message] = None
        for idx, choice in enumerate(encounter.choices):
            button = discord.ui.Button(label=choice.label, style=choice.style, row=idx // 3)
            button.callback = self._make_callback(choice.key)
            self.add_item(button)

    def bind_message(self, message: discord.Message) -> None:
        self._message = message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Only the player who triggered this encounter can choose.", ephemeral=True)
            return False
        return True

    def _disable_all(self) -> None:
        for child in self.children:
            child.disabled = True

    def _make_callback(self, choice_key: str):
        async def callback(interaction: discord.Interaction) -> None:
            async with self._lock:
                if self._resolved:
                    await interaction.response.send_message("This encounter is already resolved.", ephemeral=True)
                    return
                self._resolved = True
                self._disable_all()
            await interaction.response.defer()
            await self.resolver(interaction=interaction, choice_key=choice_key, timed_out=False, view=self)
        return callback

    async def on_timeout(self) -> None:
        async with self._lock:
            if self._resolved:
                return
            self._resolved = True
            self._disable_all()
        if self._message is not None:
            try:
                await self._message.edit(view=self)
            except Exception:
                return
        await self.resolver(interaction=None, choice_key=self.encounter.safe_choice_key, timed_out=True, view=self)
