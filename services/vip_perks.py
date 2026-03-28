from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import TypedDict


class VIPPerk(TypedDict):
    name: str
    value: str


VIPPerkCategory = list[VIPPerk]
VIPPerksMap = dict[str, VIPPerkCategory]


# Single source of truth for all user-facing VIP benefits.
# Keep this dictionary in sync with feature behavior across the project.
VIP_PERKS: VIPPerksMap = {
    "economy": [
        {
            "name": "Pickpocket Cooldown Boost",
            "value": "3m cooldown instead of 5m",
        },
        {
            "name": "Shop Mythical Upgrade Chance",
            "value": "3% mythical roll chance (vs 1% standard)",
        },
        {
            "name": "Daily Shop Reroll",
            "value": "1 free VIP reroll per shop day",
        },
    ],
    "jobs": [
        {
            "name": "Extra Job Slot",
            "value": "3 unlocked loadout slots instead of 2",
        },
        {
            "name": "VIP-Only Jobs",
            "value": "Access to Influencer, Streamer, Business CEO, and Space Miner",
        },
    ],
    "stamina": [
        {
            "name": "Faster Stamina Regen",
            "value": "30 stamina/hour regen (vs 10/hour standard)",
        },
    ],
    "business": [
        {
            "name": "Auto-Hire",
            "value": "Unlocks VIP Auto-Hire workflows in Business",
        },
        {
            "name": "Advanced VIP Rerolls",
            "value": "Bulk reroll setup and guided hiring panels",
        },
    ],
}


CATEGORY_TITLES: Mapping[str, str] = {
    "economy": "💰 Economy Perks",
    "jobs": "🛠️ Job Perks",
    "stamina": "⚡ Stamina Perks",
    "business": "🏢 Business Perks",
    "exclusive": "👑 Exclusive Perks",
    "quality_of_life": "✨ Quality of Life",
    "loot": "🎁 Loot Perks",
}


def get_vip_perks() -> VIPPerksMap:
    """Return a deep-copied VIP perks map safe for read-only rendering."""
    return deepcopy(VIP_PERKS)


def get_category_title(category_key: str) -> str:
    """Resolve a human-friendly category title for embed rendering."""
    fallback = category_key.replace("_", " ").title()
    return CATEGORY_TITLES.get(category_key, fallback)


def iter_vip_perk_lines(perks: Sequence[VIPPerk]) -> list[str]:
    """Convert perk objects into bullet-point display lines."""
    lines: list[str] = []
    for perk in perks:
        name = (perk.get("name") or "").strip()
        value = (perk.get("value") or "").strip()
        if not name and not value:
            continue
        if name and value:
            lines.append(f"• {name} — {value}")
        elif name:
            lines.append(f"• {name}")
        else:
            lines.append(f"• {value}")
    return lines
