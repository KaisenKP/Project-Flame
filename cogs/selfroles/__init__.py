# What this file is: Extension entrypoint for the Self Roles / Role Picker feature.
# Last change: 2026-05-29 - Initial modular self-role package.

from __future__ import annotations

from .cog import SelfRolesCog


async def setup(bot):
    await bot.add_cog(SelfRolesCog(bot))
