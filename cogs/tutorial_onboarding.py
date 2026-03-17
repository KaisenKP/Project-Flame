from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

import discord
from discord import app_commands
from discord.ext import commands

TARGET_CHANNEL_ID: Final[int] = 1460858579069173852
TUTORIAL_CATEGORY_NAME: Final[str] = "Bot tutorial"
TUTORIAL_TOPIC_PREFIX: Final[str] = "bot_tutorial_user:"


@dataclass(frozen=True)
class TutorialStep:
    title: str
    description: str
    color: discord.Color


TUTORIAL_STEPS: tuple[TutorialStep, ...] = (
    TutorialStep(
        title="🎮 Level 1 — Welcome to the bot lane",
        description=(
            "You made it. This is your mini game-style tutorial, built to get you from **new** to **locked in**.\n\n"
            "**How this run works:**\n"
            "• Hit **Next** to move step-by-step\n"
            "• Every page has a quick mock example\n"
            "• You can test commands in real time and freestyle your own style"
        ),
        color=discord.Color.blurple(),
    ),
    TutorialStep(
        title="📆 Daily — your free streak fuel",
        description=(
            "Use **/daily** every day to claim free Silver.\n\n"
            "**Why it matters:** consistent claims stack your economy without sweating.\n\n"
            "**Mock flow:**\n"
            "```\n"
            "/daily\n"
            "Bot: +1,500 Silver claimed.\n"
            "```\n"
            "**Edit this strategy:** Pick a daily reminder time and never miss it."
        ),
        color=discord.Color.green(),
    ),
    TutorialStep(
        title="🛠️ Work + Jobs — the core grind loop",
        description=(
            "Think of this as your gameplay loop:\n"
            "1) Pick a role with **/job**\n"
            "2) Run **/work** on cooldown\n"
            "3) Upgrade by repeating\n\n"
            "**Mock flow:**\n"
            "```\n"
            "/job\n"
            "(choose a job)\n"
            "/work\n"
            "Bot: Shift complete. +Silver +XP\n"
            "```\n"
            "**Edit this strategy:** Test 2-3 jobs and keep the one that matches your playstyle."
        ),
        color=discord.Color.gold(),
    ),
    TutorialStep(
        title="🏢 Business — passive empire vibes",
        description=(
            "Business is for long-term scaling. Build it, upgrade it, let it cook.\n\n"
            "**Mindset:** active income from **/work**, passive growth from business systems.\n\n"
            "**Mock flow:**\n"
            "```\n"
            "Open business panel\n"
            "Buy/upgrade a business\n"
            "Collect and reinvest profits\n"
            "```\n"
            "**Edit this strategy:** Reinvest early profits before going for flex purchases."
        ),
        color=discord.Color.purple(),
    ),
    TutorialStep(
        title="🎁 Lootboxes — high risk, high drip",
        description=(
            "Lootboxes can drop strong rewards, but RNG is RNG.\n\n"
            "**Mock flow:**\n"
            "```\n"
            "Buy/open lootbox\n"
            "Bot: You pulled [item/reward]\n"
            "```\n"
            "**Edit this strategy:** Set a budget first so you don't nuke your bankroll."
        ),
        color=discord.Color.orange(),
    ),
    TutorialStep(
        title="🥈 Silver — your main currency",
        description=(
            "Silver powers almost everything: jobs, upgrades, and progression.\n\n"
            "**Good economy habits:**\n"
            "• Claim **/daily**\n"
            "• Keep **/work** on cooldown\n"
            "• Spend with purpose (growth > impulse)\n\n"
            "**Mock budget:**\n"
            "```\n"
            "50% growth (job/business)\n"
            "30% utility\n"
            "20% fun/risk\n"
            "```"
        ),
        color=discord.Color.light_grey(),
    ),
    TutorialStep(
        title="✅ Final Level — you're ready",
        description=(
            "W tutorial run. You now know the essentials: **Daily, Work, Jobs, Business, Lootboxes, Silver**.\n\n"
            "Next move: start running your own routine and optimize over time.\n"
            "If you want, you can re-open this tutorial anytime from the panel button."
        ),
        color=discord.Color.teal(),
    ),
)


class TutorialWizardView(discord.ui.View):
    def __init__(self, *, owner_id: int, step_index: int = 0):
        super().__init__(timeout=900)
        self.owner_id = int(owner_id)
        self.step_index = max(0, min(step_index, len(TUTORIAL_STEPS) - 1))
        self.is_finished = False
        self._sync_buttons()

    def _build_embed(self) -> discord.Embed:
        step = TUTORIAL_STEPS[self.step_index]
        embed = discord.Embed(title=step.title, description=step.description, color=step.color)
        embed.set_footer(text=f"Step {self.step_index + 1}/{len(TUTORIAL_STEPS)} • Tutorial mode")
        return embed

    def _sync_buttons(self) -> None:
        self.prev_btn.disabled = self.is_finished or self.step_index == 0
        self.next_btn.disabled = self.is_finished or self.step_index >= len(TUTORIAL_STEPS) - 1
        self.finish_btn.disabled = self.is_finished

    async def _finish_onboarding(self, interaction: discord.Interaction, *, manual: bool) -> None:
        self.is_finished = True
        self._sync_buttons()

        reason = "You ended the onboarding flow early. You can start again from the main onboarding panel anytime."
        if not manual:
            reason = "Onboarding complete. You reached the final tutorial step automatically."

        done_embed = discord.Embed(
            title="🎉 Onboarding finished",
            description=reason,
            color=discord.Color.brand_green(),
        )
        done_embed.set_footer(text="Tutorial controls are now locked for this session")
        await interaction.response.edit_message(embed=done_embed, view=self)
        self.stop()

    async def _reject_non_owner(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.owner_id:
            return False
        await interaction.response.send_message(
            "This tutorial panel belongs to someone else. Tap the main onboarding button to spawn your own.",
            ephemeral=True,
        )
        return True

    @discord.ui.button(label="◀ Back", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._reject_non_owner(interaction):
            return
        self.step_index = max(0, self.step_index - 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._reject_non_owner(interaction):
            return
        self.step_index = min(len(TUTORIAL_STEPS) - 1, self.step_index + 1)
        if self.step_index >= len(TUTORIAL_STEPS) - 1:
            await self._finish_onboarding(interaction, manual=False)
            return
        self._sync_buttons()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    @discord.ui.button(label="End onboarding", style=discord.ButtonStyle.danger)
    async def finish_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if await self._reject_non_owner(interaction):
            return
        await self._finish_onboarding(interaction, manual=True)


class TutorialLauncherView(discord.ui.View):
    def __init__(self, cog: "TutorialOnboardingCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Start onboarding",
        style=discord.ButtonStyle.success,
        custom_id="tutorial:onboarding:start",
        emoji="✨",
    )
    async def start_onboarding(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Use this inside the server so I can set things up.", ephemeral=True)
            return

        tutorial_channel = await self.cog.ensure_user_tutorial_channel(interaction.guild, interaction.user)

        jump_view = discord.ui.View()
        jump_view.add_item(
            discord.ui.Button(label="Jump to tutorial", style=discord.ButtonStyle.link, url=tutorial_channel.jump_url)
        )

        if interaction.response.is_done():
            await interaction.followup.send("Jump to your tutorial space:", view=jump_view, ephemeral=True)
        else:
            await interaction.response.send_message("Jump to your tutorial space:", view=jump_view, ephemeral=True)


class TutorialOnboardingCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.launcher_view = TutorialLauncherView(self)

    async def cog_load(self) -> None:
        self.bot.add_view(self.launcher_view)

    def build_panel_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Custom Bot Tutorial",
            description=(
                "We built a custom bot. Wanna integrate yourself into the system?\n"
                "Tap **Start onboarding** and I'll spawn your personal tutorial lane."
            ),
            color=discord.Color.dark_embed(),
        )
        embed.set_footer(text="Minimal guide • Step-by-step • Beginner friendly")
        return embed

    async def ensure_tutorial_category(self, guild: discord.Guild) -> discord.CategoryChannel:
        for category in guild.categories:
            if category.name.lower() == TUTORIAL_CATEGORY_NAME.lower():
                return category
        return await guild.create_category(name=TUTORIAL_CATEGORY_NAME, position=len(guild.categories))

    async def ensure_user_tutorial_channel(
        self,
        guild: discord.Guild,
        user: discord.abc.User,
    ) -> discord.TextChannel:
        category = await self.ensure_tutorial_category(guild)
        marker = f"{TUTORIAL_TOPIC_PREFIX}{user.id}"

        for channel in category.text_channels:
            if (channel.topic or "").strip() == marker:
                return channel

        base = re.sub(r"[^a-z0-9-]", "", user.display_name.lower().replace(" ", "-"))
        base = base.strip("-") or "player"
        name = f"tutorial-{base}"[:90]

        overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True),
        }

        channel = await guild.create_text_channel(
            name=name,
            category=category,
            topic=marker,
            overwrites=overwrites,
            reason=f"Tutorial onboarding for user {user.id}",
        )

        tutorial_view = TutorialWizardView(owner_id=user.id)
        await channel.send(content=f"{user.mention} welcome to your bot tutorial lane ✨", embed=tutorial_view._build_embed(), view=tutorial_view)
        return channel

    @app_commands.command(name="deploy_tutorial_panel", description="Post the onboarding tutorial embed + button.")
    @app_commands.default_permissions(manage_guild=True)
    async def deploy_tutorial_panel(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Use this inside a server.", ephemeral=True)
            return

        target = channel
        if target is None:
            target = interaction.guild.get_channel(TARGET_CHANNEL_ID)
        if not isinstance(target, discord.TextChannel):
            await interaction.response.send_message(
                f"I couldn't resolve the target channel. Pass one explicitly or verify <#{TARGET_CHANNEL_ID}> exists.",
                ephemeral=True,
            )
            return

        sent = await target.send(embed=self.build_panel_embed(), view=self.launcher_view)
        await interaction.response.send_message(
            f"Tutorial panel deployed in {target.mention}.\nMessage: {sent.jump_url}",
            ephemeral=True,
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(TutorialOnboardingCog(bot))
