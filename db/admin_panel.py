import os
from datetime import datetime

import discord
from discord import app_commands
from discord.ext import commands


def _admin_channel_id() -> int | None:
    raw = (os.getenv("ADMIN_CHANNEL_ID") or "").strip()
    return int(raw) if raw.isdigit() else None


def _admin_user_ids() -> set[int]:
    raw = (os.getenv("ADMIN_USER_IDS") or "").strip()
    out: set[int] = set()
    if raw:
        for part in raw.replace(",", " ").split():
            if part.isdigit():
                out.add(int(part))
    return out


def _panel_message_id() -> int | None:
    raw = (os.getenv("ADMIN_PANEL_MESSAGE_ID") or "").strip()
    return int(raw) if raw.isdigit() else None


class _AdjustValueModal(discord.ui.Modal):
    def __init__(self, *, title: str, field_name: str):
        super().__init__(title=title)
        self.field_name = field_name

        self.target = discord.ui.TextInput(
            label="Target user (mention or ID)",
            placeholder="@Kai or 1234567890",
            required=True,
            max_length=64,
        )
        self.amount = discord.ui.TextInput(
            label=f"{field_name} amount",
            placeholder="Example: 500 or -250",
            required=True,
            max_length=32,
        )
        self.reason = discord.ui.TextInput(
            label="Reason (optional)",
            placeholder="Event prize, compensation, etc",
            required=False,
            max_length=120,
        )

        self.add_item(self.target)
        self.add_item(self.amount)
        self.add_item(self.reason)

    @staticmethod
    def _parse_user_id(raw: str) -> int | None:
        raw = raw.strip()
        if raw.isdigit():
            return int(raw)
        if raw.startswith("<@") and raw.endswith(">"):
            inner = raw[2:-1].strip()
            if inner.startswith("!"):
                inner = inner[1:]
            if inner.isdigit():
                return int(inner)
        return None

    async def on_submit(self, interaction: discord.Interaction):
        user_id = self._parse_user_id(str(self.target.value))
        amt_raw = str(self.amount.value).strip()

        if user_id is None:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="Bad input",
                    description="Could not parse the user. Use a mention or a numeric ID.",
                ),
                ephemeral=True,
            )
            return

        try:
            amount = int(amt_raw)
        except ValueError:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="Bad input",
                    description="Amount must be a whole number.",
                ),
                ephemeral=True,
            )
            return

        reason = str(self.reason.value).strip()

        e = discord.Embed(title="Queued admin action")
        e.add_field(name="Target", value=f"`{user_id}`", inline=False)
        e.add_field(name="Field", value=f"`{self.field_name}`", inline=False)
        e.add_field(name="Amount", value=f"`{amount}`", inline=False)
        e.add_field(name="Reason", value=f"`{reason}`" if reason else "`(none)`", inline=False)
        e.set_footer(text="DB not wired yet. Next step is connecting Sparked DB + tables.")

        await interaction.response.send_message(embed=e, ephemeral=True)


class AdminPanelView(discord.ui.View):
    def __init__(self, cog: "AdminPanel"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="User Lookup", style=discord.ButtonStyle.secondary, custom_id="pulse_admin:user_lookup")
    async def user_lookup(self, interaction: discord.Interaction, button: discord.ui.Button):
        ok, msg = self.cog._guard(interaction)
        if not ok:
            await interaction.response.send_message(embed=self.cog._embed("Denied", [("Reason", msg)]), ephemeral=True)
            return

        e = self.cog._embed(
            "User Lookup",
            [
                ("How to use", "Type a user mention or ID and I’ll show Pulse stats once DB is live."),
                ("Status", "DB not wired yet."),
            ],
        )
        await interaction.response.send_message(embed=e, ephemeral=True)

    @discord.ui.button(label="Adjust XP", style=discord.ButtonStyle.primary, custom_id="pulse_admin:adjust_xp")
    async def adjust_xp(self, interaction: discord.Interaction, button: discord.ui.Button):
        ok, msg = self.cog._guard(interaction)
        if not ok:
            await interaction.response.send_message(embed=self.cog._embed("Denied", [("Reason", msg)]), ephemeral=True)
            return
        await interaction.response.send_modal(_AdjustValueModal(title="Adjust XP", field_name="xp_total"))

    @discord.ui.button(label="Adjust Balance", style=discord.ButtonStyle.primary, custom_id="pulse_admin:adjust_balance")
    async def adjust_balance(self, interaction: discord.Interaction, button: discord.ui.Button):
        ok, msg = self.cog._guard(interaction)
        if not ok:
            await interaction.response.send_message(embed=self.cog._embed("Denied", [("Reason", msg)]), ephemeral=True)
            return
        await interaction.response.send_modal(_AdjustValueModal(title="Adjust Balance", field_name="balance"))

    @discord.ui.button(label="System Status", style=discord.ButtonStyle.success, custom_id="pulse_admin:status")
    async def system_status(self, interaction: discord.Interaction, button: discord.ui.Button):
        ok, msg = self.cog._guard(interaction)
        if not ok:
            await interaction.response.send_message(embed=self.cog._embed("Denied", [("Reason", msg)]), ephemeral=True)
            return

        bot = self.cog.bot
        cmds = bot.tree.get_commands()

        e = discord.Embed(title="Pulse System Status")
        e.add_field(name="Latency", value=f"`{round(bot.latency * 1000)}ms`", inline=True)
        e.add_field(name="Guilds", value=f"`{len(bot.guilds)}`", inline=True)
        e.add_field(name="Slash Commands", value=f"`{len(cmds)}`", inline=True)
        e.add_field(name="Cogs Loaded", value=f"`{len(bot.cogs)}`", inline=True)
        e.add_field(name="Admin Channel", value=f"`{self.cog.admin_channel_id}`", inline=True)
        e.timestamp = datetime.utcnow()

        await interaction.response.send_message(embed=e, ephemeral=True)

    @discord.ui.button(label="Audit Log", style=discord.ButtonStyle.secondary, custom_id="pulse_admin:audit")
    async def audit_log(self, interaction: discord.Interaction, button: discord.ui.Button):
        ok, msg = self.cog._guard(interaction)
        if not ok:
            await interaction.response.send_message(embed=self.cog._embed("Denied", [("Reason", msg)]), ephemeral=True)
            return

        e = self.cog._embed(
            "Audit Log",
            [
                ("Plan", "Every admin edit will be recorded with before/after, actor, reason, timestamp."),
                ("Status", "DB not wired yet."),
            ],
        )
        await interaction.response.send_message(embed=e, ephemeral=True)


class AdminPanel(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.admin_channel_id = _admin_channel_id()
        self.admin_ids = _admin_user_ids()
        self.panel_message_id = _panel_message_id()

        self.view = AdminPanelView(self)
        bot.add_view(self.view)

        self._posted_once = False

    def _embed(self, title: str, fields: list[tuple[str, str]] | None = None) -> discord.Embed:
        e = discord.Embed(title=title)
        if fields:
            for name, value in fields:
                e.add_field(name=name, value=value, inline=False)
        e.timestamp = datetime.utcnow()
        return e

    def _is_allowed_actor(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return False
        actor_id = interaction.user.id
        if actor_id == interaction.guild.owner_id:
            return True
        return actor_id in self.admin_ids

    def _is_allowed_channel(self, interaction: discord.Interaction) -> bool:
        if self.admin_channel_id is None:
            return True
        return interaction.channel_id == self.admin_channel_id

    def _guard(self, interaction: discord.Interaction) -> tuple[bool, str]:
        if not interaction.guild:
            return False, "This can only be used in a server."
        if not self._is_allowed_actor(interaction):
            return False, "Not authorized."
        if not self._is_allowed_channel(interaction):
            return False, "Wrong channel. Use the admin channel."
        return True, ""

    def _panel_embed(self) -> discord.Embed:
        e = discord.Embed(title="FlameBot Admin Panel")
        e.description = (
            "Private control room.\n"
            "Buttons open modals for fast edits.\n"
            "All actions will be audit logged once DB is live."
        )
        e.add_field(name="Scope", value="Owner + whitelisted admins only", inline=False)
        e.add_field(name="Tip", value="If commands seem missing, check channel perms: Use Application Commands", inline=False)
        e.timestamp = datetime.utcnow()
        return e

    async def _ensure_panel_message(self) -> None:
        if self._posted_once:
            return

        if self.admin_channel_id is None:
            return

        channel = self.bot.get_channel(self.admin_channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(self.admin_channel_id)
            except Exception:
                return

        if not isinstance(channel, (discord.TextChannel, discord.Thread, discord.VoiceChannel)):
            return

        try:
            if self.panel_message_id:
                msg = await channel.fetch_message(self.panel_message_id)
                await msg.edit(embed=self._panel_embed(), view=self.view)
                self._posted_once = True
                return
        except Exception:
            pass

        try:
            msg = await channel.send(embed=self._panel_embed(), view=self.view)
            self._posted_once = True
            print(f"[Pulse] Admin panel posted. Set ADMIN_PANEL_MESSAGE_ID={msg.id} to make it persistent.")
        except Exception:
            return

    @commands.Cog.listener()
    async def on_ready(self):
        await self._ensure_panel_message()

    @app_commands.command(name="admin_panel", description="Post or refresh the Pulse admin panel (admin only).")
    async def admin_panel(self, interaction: discord.Interaction):
        ok, msg = self._guard(interaction)
        if not ok:
            await interaction.response.send_message(embed=self._embed("Denied", [("Reason", msg)]), ephemeral=True)
            return

        if self.admin_channel_id is None:
            await interaction.response.send_message(
                embed=self._embed("Missing config", [("ADMIN_CHANNEL_ID", "Set this env var to your admin channel ID.")]),
                ephemeral=True,
            )
            return

        await self._ensure_panel_message()
        await interaction.response.send_message(embed=self._embed("Done", [("Panel", "Admin panel is posted/refreshed.")] ), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminPanel(bot))
