from services.items_catalog import ITEMS


def lootbox_lines(lootboxes: dict[str, int]) -> str:
    return "\n".join(f"• {rarity.title()}: {qty}" for rarity, qty in lootboxes.items()) or "None"


def item_lines(items: dict[str, int]) -> str:
    return "\n".join(f"• {ITEMS[item_key].name} x{qty}" for item_key, qty in items.items()) or "None"
