# What this file is: Business logic for resolving, creating, validating, and applying self roles.
# Last change: 2026-05-29 - Initial safe role service layer.

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Collection

import discord
from discord.ext import commands

from .config import CATEGORIES, CATEGORY_ORDER, RoleCategory, RoleDefinition, SCHEMA_VERSION
from .errors import (
    MissingConfiguredRoleError,
    RoleHierarchyError,
    RolePermissionError,
    SelfRoleSetupError,
)
from .storage import SelfRolesGuildRecord, SelfRolesStorage

log = logging.getLogger(__name__)


@dataclass(slots=True)
class SelfRoleSetupSummary:
    panel_action: str = ""
    panel_channel_id: int | None = None
    panel_message_id: int | None = None
    reused_by_id: list[str] = field(default_factory=list)
    reused_saved: list[str] = field(default_factory=list)
    found_by_name: list[str] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RoleUpdateResult:
    category: RoleCategory
    added: list[discord.Role] = field(default_factory=list)
    removed: list[discord.Role] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return bool(self.added or self.removed)

    def user_message(self) -> str:
        if not self.changed:
            return "No changes needed. Your roles are already up to date."
        suffix = "role" if self.category.selection_type == "single" else "roles"
        return f"Your {self.category.success_label} {suffix} were updated."


def _normalize_role_name(value: str) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip()).casefold()
    while text and not text[0].isalnum():
        text = text[1:].lstrip()
    return text


class SelfRolesService:
    def __init__(self, bot: commands.Bot, storage: SelfRolesStorage) -> None:
        self.bot = bot
        self.storage = storage
        self._setup_locks: dict[int, asyncio.Lock] = {}

    def setup_lock_for(self, guild_id: int) -> asyncio.Lock:
        guild_id = int(guild_id)
        lock = self._setup_locks.get(guild_id)
        if lock is None:
            lock = asyncio.Lock()
            self._setup_locks[guild_id] = lock
        return lock

    async def ensure_ready(self) -> None:
        await self.storage.ensure_tables()

    def _bot_member(self, guild: discord.Guild) -> discord.Member:
        me = guild.me
        if me is None and self.bot.user is not None:
            found = guild.get_member(self.bot.user.id)
            if isinstance(found, discord.Member):
                me = found
        if me is None:
            raise SelfRoleSetupError("I could not find my bot member record in this server.")
        return me

    def validate_setup_permissions(self, guild: discord.Guild, channel: discord.TextChannel, summary: SelfRoleSetupSummary) -> None:
        me = self._bot_member(guild)
        channel_perms = channel.permissions_for(me)
        required = {
            "View Channel": channel_perms.view_channel,
            "Send Messages": channel_perms.send_messages,
            "Embed Links": channel_perms.embed_links,
            "Manage Roles": me.guild_permissions.manage_roles,
            "Use Application Commands": getattr(channel_perms, "use_application_commands", True),
        }
        missing = [name for name, ok in required.items() if not ok]
        if missing:
            raise SelfRoleSetupError("Missing required bot permission(s): " + ", ".join(missing))
        if not channel_perms.read_message_history:
            summary.warnings.append(
                "The bot is missing Read Message History in the panel channel, so editing an existing saved panel may fail."
            )

    async def resolve_configured_roles(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
    ) -> tuple[SelfRoleSetupSummary, SelfRolesGuildRecord]:
        summary = SelfRoleSetupSummary()
        self.validate_setup_permissions(guild, channel, summary)

        record = await self.storage.get(guild.id)
        resolved: dict[str, dict[str, int]] = {}
        me = self._bot_member(guild)

        for category_key in CATEGORY_ORDER:
            category = CATEGORIES[category_key]
            resolved[category.key] = {}
            saved_for_category = record.role_ids.get(category.key, {})
            for role_def in category.roles:
                role = await self._resolve_role(guild, role_def, saved_for_category, summary)
                if role is None:
                    continue
                resolved[category.key][role_def.key] = int(role.id)
                self._warn_if_unmanageable(role, me, summary)

        record.role_ids = resolved
        record.schema_version = SCHEMA_VERSION
        await self.storage.upsert(record, touch_setup=True)
        return summary, record

    async def _resolve_role(
        self,
        guild: discord.Guild,
        role_def: RoleDefinition,
        saved_for_category: dict[str, int],
        summary: SelfRoleSetupSummary,
    ) -> discord.Role | None:
        if role_def.role_id is not None:
            role = guild.get_role(int(role_def.role_id))
            if role is not None:
                summary.reused_by_id.append(f"{role_def.name} (`{role.id}`)")
                return role
            summary.warnings.append(
                f"Configured role ID for `{role_def.name}` is missing. No duplicate role was created for this ID-backed role."
            )
            return None

        saved_role_id = saved_for_category.get(role_def.key)
        if saved_role_id:
            role = guild.get_role(int(saved_role_id))
            if role is not None:
                summary.reused_saved.append(f"{role_def.name} (`{role.id}`)")
                return role

        exact = next((role for role in guild.roles if role.name == role_def.name), None)
        if exact is not None:
            summary.found_by_name.append(f"{role_def.name} (`{exact.id}`)")
            return exact

        near = self._find_near_duplicate(guild, role_def.name)
        if near is not None:
            summary.warnings.append(
                f"Possible duplicate for `{role_def.name}` found as `{near.name}`. I did not create a new role."
            )
            return None

        if not role_def.create_if_missing:
            summary.warnings.append(f"`{role_def.name}` is missing and is configured not to be created automatically.")
            return None

        try:
            role = await guild.create_role(
                name=role_def.name,
                permissions=discord.Permissions.none(),
                mentionable=False,
                reason="Self-role picker setup",
            )
        except discord.Forbidden:
            summary.warnings.append(f"I do not have permission to create `{role_def.name}`.")
            log.warning("Self-role creation forbidden in guild %s for role %s", guild.id, role_def.name)
            return None
        except discord.HTTPException as exc:
            summary.warnings.append(f"Discord rejected role creation for `{role_def.name}`.")
            log.warning("Self-role creation failed in guild %s for role %s: %s", guild.id, role_def.name, exc)
            return None

        summary.created.append(f"{role_def.name} (`{role.id}`)")
        return role

    def _find_near_duplicate(self, guild: discord.Guild, wanted_name: str) -> discord.Role | None:
        wanted = _normalize_role_name(wanted_name)
        if not wanted:
            return None
        for role in guild.roles:
            if role.name == wanted_name:
                continue
            if _normalize_role_name(role.name) == wanted:
                return role
        return None

    def _warn_if_unmanageable(
        self,
        role: discord.Role,
        me: discord.Member,
        summary: SelfRoleSetupSummary,
    ) -> None:
        if role.managed:
            summary.warnings.append(f"The bot cannot manage `{role.name}` because it is managed by an integration.")
            return
        if role >= me.top_role:
            summary.warnings.append(
                f"The bot cannot manage `{role.name}` because that role is above the bot's highest role. "
                "Move the bot role higher, then run `/setup_roles` again."
            )

    async def get_category_roles(
        self,
        guild: discord.Guild,
        category_key: str,
    ) -> tuple[RoleCategory, dict[str, discord.Role]]:
        category = CATEGORIES[category_key]
        record = await self.storage.get(guild.id)
        stored = record.role_ids.get(category.key, {})
        roles_by_key: dict[str, discord.Role] = {}
        for role_def in category.roles:
            role_id = stored.get(role_def.key)
            if not role_id:
                raise MissingConfiguredRoleError()
            role = guild.get_role(int(role_id))
            if role is None:
                raise MissingConfiguredRoleError()
            roles_by_key[role_def.key] = role
        return category, roles_by_key

    async def apply_selection(
        self,
        *,
        guild: discord.Guild,
        member: discord.Member,
        category_key: str,
        selected_keys: Collection[str],
    ) -> RoleUpdateResult:
        category, roles_by_key = await self.get_category_roles(guild, category_key)
        selected = {str(key) for key in selected_keys}
        allowed = set(roles_by_key.keys())
        if not selected.issubset(allowed):
            raise MissingConfiguredRoleError()
        if category.selection_type == "single" and len(selected) > 1:
            selected = set(list(selected)[:1])

        selected_roles = {roles_by_key[key] for key in selected}
        category_roles = set(roles_by_key.values())
        current_roles = set(member.roles)

        to_add = [role for role in selected_roles if role not in current_roles]
        to_remove = [role for role in category_roles if role in current_roles and role not in selected_roles]

        result = RoleUpdateResult(category=category, added=to_add, removed=to_remove)
        if not result.changed:
            return result

        self._validate_member_role_changes(guild, to_add + to_remove)
        try:
            if to_remove:
                await member.remove_roles(*to_remove, reason=f"Self-role picker: {category.label} update")
            if to_add:
                await member.add_roles(*to_add, reason=f"Self-role picker: {category.label} update")
        except discord.Forbidden as exc:
            raise RolePermissionError() from exc
        except discord.HTTPException as exc:
            raise RolePermissionError() from exc
        return result

    def _validate_member_role_changes(self, guild: discord.Guild, roles: list[discord.Role]) -> None:
        me = self._bot_member(guild)
        if not me.guild_permissions.manage_roles:
            raise RolePermissionError()
        for role in roles:
            if role.managed or role >= me.top_role:
                raise RoleHierarchyError()
