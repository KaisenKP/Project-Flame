from __future__ import annotations

import unittest
from types import SimpleNamespace

import discord

from cogs.ban import PunishHubView


class _FakeBot:
    def get_cog(self, _name: str):
        return None


class PunishHubViewTests(unittest.TestCase):
    def test_timeout_button_does_not_override_view_timeout_property(self) -> None:
        target = SimpleNamespace(id=42, mention="<@42>")
        view = PunishHubView(bot=_FakeBot(), moderator_id=123, target=target)  # type: ignore[arg-type]
        self.assertNotIsInstance(view.timeout, discord.ui.Button)
        self.assertEqual(view.timeout, 300)


if __name__ == "__main__":
    unittest.main()
