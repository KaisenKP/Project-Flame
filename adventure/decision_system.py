from __future__ import annotations

import random

import discord

from adventure.models.player_runtime import PlayerRuntime


class DecisionButton(discord.ui.Button):
    def __init__(self, idx: int, label: str):
        super().__init__(style=discord.ButtonStyle.secondary, label=label)
        self.idx = int(idx)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DecisionView):
            return
        await view.vote(interaction, self.idx)


class DecisionView(discord.ui.View):
    def __init__(self, *, players: list[PlayerRuntime], options: list[str], timeout: float = 22.0):
        super().__init__(timeout=timeout)
        self.allowed_ids = {int(p.user_id) for p in players}
        self.options = list(options)
        self.ballots: dict[int, int] = {}
        for idx, option in enumerate(self.options):
            self.add_item(DecisionButton(idx=idx, label=option))

    async def vote(self, interaction: discord.Interaction, idx: int) -> None:
        uid = int(interaction.user.id)
        if uid not in self.allowed_ids:
            await interaction.response.send_message("You're not in this adventure.", ephemeral=True)
            return
        self.ballots[uid] = int(idx)
        await interaction.response.send_message(f"🗳️ Vote registered: **{self.options[idx]}**", ephemeral=True)

    def resolve(self) -> str:
        if not self.ballots:
            return random.choice(self.options)
        counts: dict[int, int] = {}
        for vote_idx in self.ballots.values():
            counts[vote_idx] = counts.get(vote_idx, 0) + 1
        top = max(counts.values())
        winners = [idx for idx, num in counts.items() if num == top]
        return self.options[random.choice(winners)]
