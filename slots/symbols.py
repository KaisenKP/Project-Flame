from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Symbol:
    key: str
    emoji: str
    rarity: str = "common"


CHERRY = Symbol("cherry", "🍒")
LEMON = Symbol("lemon", "🍋")
GRAPE = Symbol("grape", "🍇")
BELL = Symbol("bell", "🔔")
CLOVER = Symbol("clover", "🍀", "uncommon")
DIAMOND = Symbol("diamond", "💎", "rare")
CROWN = Symbol("crown", "👑", "rare")
SEVEN = Symbol("seven", "7️⃣", "rare")
FIRE = Symbol("fire", "🔥", "rare")
SKULL = Symbol("skull", "💀", "common")
JEFF = Symbol("jeff", "🐙", "ultra")

ALL_SYMBOLS = {
    s.key: s
    for s in (CHERRY, LEMON, GRAPE, BELL, CLOVER, DIAMOND, CROWN, SEVEN, FIRE, SKULL, JEFF)
}


def as_emoji(symbol_key: str) -> str:
    return ALL_SYMBOLS.get(symbol_key, CHERRY).emoji
