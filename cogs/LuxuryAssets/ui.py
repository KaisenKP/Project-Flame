from __future__ import annotations

from dataclasses import dataclass

import discord

from .catalog import ASSET_CATALOG, iter_active_assets
from .domain import OwnedAssetView
from .embeds import (
    build_asset_detail_embed,
    build_bank_embed,
    build_buy_confirmation_embed,
    build_collection_embed,
    build_error_embed,
    build_hub_overview_embed,
    build_shop_embed,
    build_showcase_embed,
    build_success_embed,
)
from .util import SHOWCASE_SLOTS_MAX


class ConfirmCancelView(discord.ui.View):
    def __init__(self, *, owner_id: int, timeout: float = 120):
        super().__init__(timeout=timeout)
        self.owner_id = int(owner_id)
        self.confirmed = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_id:
            await interaction.response.send_message("This confirmation is not for you.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = False
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()


@dataclass(slots=True)
class LuxuryHubData:
    balance: int
    assets: list[OwnedAssetView]
    total_asset_value: int
    net_worth: int
    capacity: object
    loan: object


class BorrowModal(discord.ui.Modal, title="Borrow Silver"):
    amount = discord.ui.TextInput(label="Amount", placeholder="Enter amount to borrow", required=True)

    def __init__(self, view: "LuxuryHubView"):
        super().__init__()
        self.view_ref = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            amount = int(str(self.amount).strip())
        except ValueError:
            await interaction.response.send_message("Please enter a valid integer amount.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Amount must be greater than 0.", ephemeral=True)
            return
        ok, message = await self.view_ref.borrow(interaction, amount)
        if ok:
            await interaction.response.send_message(embed=build_success_embed(title="✅ Loan Issued", description=message), ephemeral=True)
        else:
            await interaction.response.send_message(embed=build_error_embed(title="Loan Failed", description=message), ephemeral=True)


class RepayModal(discord.ui.Modal, title="Repay Loan"):
    amount = discord.ui.TextInput(label="Amount", placeholder="Enter amount to repay", required=True)

    def __init__(self, view: "LuxuryHubView"):
        super().__init__()
        self.view_ref = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            amount = int(str(self.amount).strip())
        except ValueError:
            await interaction.response.send_message("Please enter a valid integer amount.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Amount must be greater than 0.", ephemeral=True)
            return
        ok, message = await self.view_ref.repay(interaction, amount)
        if ok:
            await interaction.response.send_message(embed=build_success_embed(title="💸 Repayment Applied", description=message), ephemeral=True)
        else:
            await interaction.response.send_message(embed=build_error_embed(title="Repayment Failed", description=message), ephemeral=True)


class LuxuryHubView(discord.ui.View):
    def __init__(self, *, owner_id: int, controller, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.owner_id = int(owner_id)
        self.controller = controller
        self.section = "overview"
        self.collection_page = 0
        self.selected_shop_key: str | None = None
        self.selected_asset_id: int | None = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_id:
            await interaction.response.send_message("This Luxury Hub session belongs to another user.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        data: LuxuryHubData = await self.controller.get_data(interaction)

        if self.section == "overview":
            showcased = [a for a in data.assets if a.is_showcased]
            return build_hub_overview_embed(user=interaction.user, snap=await self.controller.get_overview(interaction), showcased_assets=showcased)

        if self.section == "shop":
            key = self.selected_shop_key
            if key is None:
                active = iter_active_assets()
                key = active[0].asset_key if active else None
            owned_count = await self.controller.get_owned_count(interaction, key) if key else 0
            return build_shop_embed(user=interaction.user, balance=data.balance, selected_key=key, owned_count=owned_count)

        if self.section == "collection":
            return build_collection_embed(user=interaction.user, assets=data.assets, page=self.collection_page)

        if self.section == "showcase":
            return build_showcase_embed(user=interaction.user, assets=data.assets)

        if self.section == "bank":
            return build_bank_embed(user=interaction.user, snap=data.capacity, loan=data.loan)

        return build_error_embed(title="Unknown Section", description="Please refresh this view.")

    async def refresh(self, interaction: discord.Interaction) -> None:
        self._rebuild_dynamic_controls(interaction)
        embed = await self.build_embed(interaction)
        await interaction.response.edit_message(embed=embed, view=self)

    def _rebuild_dynamic_controls(self, interaction: discord.Interaction) -> None:
        self._remove_dynamic_controls()
        if self.section == "shop":
            self.add_item(ShopAssetSelect(self, interaction))
            self.add_item(ShopBuyButton(self))
        elif self.section == "collection":
            self.add_item(CollectionAssetSelect(self, interaction))
            self.add_item(CollectionPrevButton(self))
            self.add_item(CollectionNextButton(self))
            self.add_item(CollectionInspectButton(self))
        elif self.section == "showcase":
            self.add_item(ShowcaseSlotSelect(self, interaction))
            self.add_item(ShowcaseAssetSelect(self, interaction))
            self.add_item(ShowcaseAssignButton(self))
            self.add_item(ShowcaseClearButton(self))
        elif self.section == "bank":
            self.add_item(BankBorrowButton(self))
            self.add_item(BankRepayButton(self))

    def _remove_dynamic_controls(self) -> None:
        for child in list(self.children):
            if isinstance(child, (ShopAssetSelect, ShopBuyButton, CollectionAssetSelect, CollectionPrevButton, CollectionNextButton,
                                  CollectionInspectButton, ShowcaseSlotSelect, ShowcaseAssetSelect, ShowcaseAssignButton,
                                  ShowcaseClearButton, BankBorrowButton, BankRepayButton)):
                self.remove_item(child)

    @discord.ui.button(label="Overview", style=discord.ButtonStyle.primary, row=0)
    async def overview_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "overview"
        await self.refresh(interaction)

    @discord.ui.button(label="Shop", style=discord.ButtonStyle.secondary, row=0)
    async def shop_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "shop"
        await self.refresh(interaction)

    @discord.ui.button(label="Collection", style=discord.ButtonStyle.secondary, row=0)
    async def collection_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "collection"
        await self.refresh(interaction)

    @discord.ui.button(label="Showcase", style=discord.ButtonStyle.secondary, row=0)
    async def showcase_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "showcase"
        await self.refresh(interaction)

    @discord.ui.button(label="Bank", style=discord.ButtonStyle.secondary, row=0)
    async def bank_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "bank"
        await self.refresh(interaction)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.success, row=4)
    async def refresh_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.refresh(interaction)

    async def buy_selected(self, interaction: discord.Interaction) -> tuple[bool, str]:
        if not self.selected_shop_key:
            return False, "Pick an asset from the shop list first."
        return await self.controller.buy(interaction, self.selected_shop_key)

    async def borrow(self, interaction: discord.Interaction, amount: int) -> tuple[bool, str]:
        return await self.controller.borrow(interaction, amount)

    async def repay(self, interaction: discord.Interaction, amount: int) -> tuple[bool, str]:
        return await self.controller.repay(interaction, amount)


class ShopAssetSelect(discord.ui.Select):
    def __init__(self, view: LuxuryHubView, interaction: discord.Interaction):
        assets = iter_active_assets()[:25]
        options = [
            discord.SelectOption(
                label=a.name[:100],
                value=a.asset_key,
                description=f"{a.category.value.title()} · {a.price:,} Silver"[:100],
                emoji=a.emoji,
            )
            for a in assets
        ]
        super().__init__(placeholder="Select an asset", min_values=1, max_values=1, options=options, row=1)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        self.hub.selected_shop_key = self.values[0]
        await self.hub.refresh(interaction)


class ShopBuyButton(discord.ui.Button):
    def __init__(self, view: LuxuryHubView):
        super().__init__(label="Buy Selected", style=discord.ButtonStyle.success, row=1)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        if not self.hub.selected_shop_key:
            await interaction.response.send_message("Select a shop item first.", ephemeral=True)
            return

        preview = await self.hub.controller.get_shop_confirmation(interaction, self.hub.selected_shop_key)
        if preview is None:
            await interaction.response.send_message("That asset is no longer available.", ephemeral=True)
            return

        view = ConfirmCancelView(owner_id=interaction.user.id)
        await interaction.response.send_message(embed=preview, view=view, ephemeral=True)
        await view.wait()
        if not view.confirmed:
            return

        ok, message = await self.hub.buy_selected(interaction)
        follow = interaction.followup
        if ok:
            await follow.send(embed=build_success_embed(title="Purchase Complete", description=message), ephemeral=True)
        else:
            await follow.send(embed=build_error_embed(title="Purchase Failed", description=message), ephemeral=True)


class CollectionAssetSelect(discord.ui.Select):
    def __init__(self, view: LuxuryHubView, interaction: discord.Interaction):
        self.hub = view
        assets = view.controller.peek_assets(interaction)
        start = view.collection_page * 10
        chunk = assets[start : start + 10]
        options = [
            discord.SelectOption(
                label=f"#{a.id} {ASSET_CATALOG.get(a.asset_key).name if a.asset_key in ASSET_CATALOG else a.asset_key}"[:100],
                value=str(a.id),
                description=("Seized" if a.is_seized else "Active")[:100],
            )
            for a in chunk
        ]
        if not options:
            options = [discord.SelectOption(label="No assets", value="0", description="Buy from shop first")]
        super().__init__(placeholder="Select owned asset", min_values=1, max_values=1, options=options, row=1)

    async def callback(self, interaction: discord.Interaction) -> None:
        if self.values[0] == "0":
            await interaction.response.send_message("No assets to select yet.", ephemeral=True)
            return
        self.hub.selected_asset_id = int(self.values[0])
        await interaction.response.send_message(f"Selected asset #{self.hub.selected_asset_id}.", ephemeral=True)


class CollectionPrevButton(discord.ui.Button):
    def __init__(self, view: LuxuryHubView):
        super().__init__(label="◀", style=discord.ButtonStyle.secondary, row=2)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        self.hub.collection_page = max(0, self.hub.collection_page - 1)
        await self.hub.refresh(interaction)


class CollectionNextButton(discord.ui.Button):
    def __init__(self, view: LuxuryHubView):
        super().__init__(label="▶", style=discord.ButtonStyle.secondary, row=2)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        assets = self.hub.controller.peek_assets(interaction)
        max_page = max(0, (len(assets) - 1) // 10)
        self.hub.collection_page = min(max_page, self.hub.collection_page + 1)
        await self.hub.refresh(interaction)


class CollectionInspectButton(discord.ui.Button):
    def __init__(self, view: LuxuryHubView):
        super().__init__(label="Inspect", style=discord.ButtonStyle.primary, row=2)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        if self.hub.selected_asset_id is None:
            await interaction.response.send_message("Select an asset first.", ephemeral=True)
            return
        row = await self.hub.controller.find_asset(interaction, self.hub.selected_asset_id)
        if row is None:
            await interaction.response.send_message("Asset not found.", ephemeral=True)
            return
        await interaction.response.send_message(embed=build_asset_detail_embed(user=interaction.user, row=row), ephemeral=True)


class ShowcaseSlotSelect(discord.ui.Select):
    def __init__(self, view: LuxuryHubView, interaction: discord.Interaction):
        options = [
            discord.SelectOption(label=f"Slot {slot}", value=str(slot), description="Assign/clear this slot")
            for slot in range(1, SHOWCASE_SLOTS_MAX + 1)
        ]
        super().__init__(placeholder="Select showcase slot", min_values=1, max_values=1, options=options, row=1)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        self.hub.controller.set_selected_slot(interaction, int(self.values[0]))
        await interaction.response.send_message(f"Target slot: {self.values[0]}", ephemeral=True)


class ShowcaseAssetSelect(discord.ui.Select):
    def __init__(self, view: LuxuryHubView, interaction: discord.Interaction):
        assets = [a for a in view.controller.peek_assets(interaction) if not a.is_seized]
        options = [
            discord.SelectOption(
                label=f"#{a.id} {ASSET_CATALOG.get(a.asset_key).name if a.asset_key in ASSET_CATALOG else a.asset_key}"[:100],
                value=str(a.id),
                description="Eligible for showcase",
            )
            for a in assets[:25]
        ]
        if not options:
            options = [discord.SelectOption(label="No eligible assets", value="0", description="All seized or none owned")]
        super().__init__(placeholder="Select asset for showcase", min_values=1, max_values=1, options=options, row=2)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        if self.values[0] == "0":
            await interaction.response.send_message("No available assets for showcase.", ephemeral=True)
            return
        self.hub.selected_asset_id = int(self.values[0])
        await interaction.response.send_message(f"Selected asset #{self.values[0]} for showcase.", ephemeral=True)


class ShowcaseAssignButton(discord.ui.Button):
    def __init__(self, view: LuxuryHubView):
        super().__init__(label="Assign", style=discord.ButtonStyle.success, row=3)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        asset_id = self.hub.selected_asset_id
        slot = self.hub.controller.get_selected_slot(interaction)
        if asset_id is None or slot is None:
            await interaction.response.send_message("Select both an asset and showcase slot first.", ephemeral=True)
            return
        ok, msg = await self.hub.controller.assign_showcase(interaction, asset_id=asset_id, slot=slot)
        if ok:
            await interaction.response.send_message(embed=build_success_embed(title="Showcase Updated", description=msg), ephemeral=True)
        else:
            await interaction.response.send_message(embed=build_error_embed(title="Showcase Error", description=msg), ephemeral=True)


class ShowcaseClearButton(discord.ui.Button):
    def __init__(self, view: LuxuryHubView):
        super().__init__(label="Clear Slot", style=discord.ButtonStyle.secondary, row=3)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        slot = self.hub.controller.get_selected_slot(interaction)
        if slot is None:
            await interaction.response.send_message("Select a slot to clear first.", ephemeral=True)
            return
        ok, msg = await self.hub.controller.clear_showcase(interaction, slot=slot)
        if ok:
            await interaction.response.send_message(embed=build_success_embed(title="Showcase Updated", description=msg), ephemeral=True)
        else:
            await interaction.response.send_message(embed=build_error_embed(title="Showcase Error", description=msg), ephemeral=True)


class BankBorrowButton(discord.ui.Button):
    def __init__(self, view: LuxuryHubView):
        super().__init__(label="Borrow", style=discord.ButtonStyle.success, row=1)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(BorrowModal(self.hub))


class BankRepayButton(discord.ui.Button):
    def __init__(self, view: LuxuryHubView):
        super().__init__(label="Repay", style=discord.ButtonStyle.primary, row=1)
        self.hub = view

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(RepayModal(self.hub))


async def build_shop_confirmation_embed(*, user: discord.abc.User, asset_key: str, balance: int) -> discord.Embed:
    return build_buy_confirmation_embed(user=user, asset_key=asset_key, balance=balance)
