# What this file is: Static role picker category, role, and component configuration.
# Last change: 2026-05-29 - Initial self-role definitions.

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


SelectionType = Literal["multi", "single"]


@dataclass(frozen=True, slots=True)
class RoleDefinition:
    key: str
    name: str
    emoji: str
    description: str
    role_id: int | None = None
    create_if_missing: bool = True


@dataclass(frozen=True, slots=True)
class RoleCategory:
    key: str
    label: str
    title: str
    description: str
    selection_type: SelectionType
    button_custom_id: str
    select_custom_id: str
    clear_custom_id: str
    button_emoji: str
    placeholder: str
    success_label: str
    embed_image_url: str = ""
    embed_thumbnail_url: str = ""
    banner_url: str = ""
    roles: tuple[RoleDefinition, ...] = ()


PANEL_TITLE = "Choose Your Roles"
PANEL_DESCRIPTION = (
    "Pick the roles that fit you.\n\n"
    "Use the buttons below to update your Platform, Age, Pings, and Fun Roles.\n\n"
    "Platform, Pings, and Fun Roles allow multiple choices.\n"
    "Age allows one choice.\n\n"
    "Age roles are optional and are not verification.\n\n"
    "You can come back and update your roles anytime."
)
PANEL_FOOTER = "Your choices update privately."
PANEL_IMAGE_URL = ""
PANEL_THUMBNAIL_URL = ""
SCHEMA_VERSION = 1


CATEGORIES: dict[str, RoleCategory] = {
    "platform": RoleCategory(
        key="platform",
        label="Platform",
        title="Choose Your Platforms",
        description="Pick every platform you play on. You can choose more than one.",
        selection_type="multi",
        button_custom_id="selfroles:open:platform",
        select_custom_id="selfroles:select:platform",
        clear_custom_id="selfroles:clear:platform",
        button_emoji="🖥️",
        placeholder="Choose your platforms...",
        success_label="Platform",
        roles=(
            RoleDefinition("pc", "PC", "🖥️", "I play on PC."),
            RoleDefinition("playstation", "PlayStation", "🎮", "I play on PlayStation."),
            RoleDefinition("xbox", "Xbox", "🟢", "I play on Xbox."),
            RoleDefinition("nintendo_switch", "Nintendo Switch", "🔴", "I play on Switch."),
            RoleDefinition("mobile", "Mobile", "📱", "I play on mobile."),
            RoleDefinition("crossplay", "Crossplay", "🔁", "I can play across multiple platforms."),
        ),
    ),
    "age": RoleCategory(
        key="age",
        label="Age",
        title="Choose Your Age Role",
        description="Pick one optional age role, or clear it. This is not verification.",
        selection_type="single",
        button_custom_id="selfroles:open:age",
        select_custom_id="selfroles:select:age",
        clear_custom_id="selfroles:clear:age",
        button_emoji="🔞",
        placeholder="Choose an optional age role...",
        success_label="Age",
        roles=(
            RoleDefinition("age_18", "18+", "🔞", "18 or older."),
            RoleDefinition("age_21", "21+", "🍷", "21 or older."),
            RoleDefinition("age_25", "25+", "✨", "25 or older."),
            RoleDefinition("age_30", "30+", "💼", "30 or older."),
            RoleDefinition("prefer_not_to_say", "Prefer Not To Say", "🤐", "I prefer to keep this private."),
        ),
    ),
    "pings": RoleCategory(
        key="pings",
        label="Pings",
        title="Choose Your Pings",
        description="Pick the notifications you actually want. You can change this anytime.",
        selection_type="multi",
        button_custom_id="selfroles:open:pings",
        select_custom_id="selfroles:select:pings",
        clear_custom_id="selfroles:clear:pings",
        button_emoji="🔔",
        placeholder="Choose your pings...",
        success_label="Ping",
        roles=(
            RoleDefinition("vc", "VC", "🔊", "Ping me for voice chat.", 1507862812855369959, False),
            RoleDefinition("chat_revive", "Chat Revive", "💬", "Ping me when chat needs reviving.", 1507863045563486470, False),
            RoleDefinition("looking_for_group", "Looking for Group", "👥", "Ping me when people need a group.", 1507863180120948766, False),
            RoleDefinition("looking_to_play", "Looking to Play", "🎮", "Ping me when people want to play.", 1507863339689054358, False),
            RoleDefinition("events", "Events", "📅", "Ping me for server events."),
            RoleDefinition("giveaways", "Giveaways", "🎁", "Ping me for giveaways."),
            RoleDefinition("announcements", "Announcements", "📢", "Ping me for important server updates."),
            RoleDefinition("game_nights", "Game Nights", "🌙", "Ping me for game night plans."),
        ),
    ),
    "fun": RoleCategory(
        key="fun",
        label="Fun Roles",
        title="Choose Your Fun Roles",
        description="Pick any personality roles that fit you.",
        selection_type="multi",
        button_custom_id="selfroles:open:fun",
        select_custom_id="selfroles:select:fun",
        clear_custom_id="selfroles:clear:fun",
        button_emoji="✨",
        placeholder="Choose your fun roles...",
        success_label="Fun",
        roles=(
            RoleDefinition("night_owl", "Night Owl", "🦉", "Usually online late."),
            RoleDefinition("meme_dealer", "Meme Dealer", "😂", "Brings the memes."),
            RoleDefinition("chill_vibes", "Chill Vibes", "🌙", "Here to relax."),
            RoleDefinition("sweaty_gamer", "Sweaty Gamer", "🔥", "Plays to win."),
            RoleDefinition("anime_fan", "Anime Fan", "🍜", "Likes anime."),
            RoleDefinition("achievement_hunter", "Achievement Hunter", "🏆", "Loves grinding goals."),
            RoleDefinition("lore_goblin", "Lore Goblin", "📚", "Reads every piece of lore."),
            RoleDefinition("certified_yapper", "Certified Yapper", "🗣️", "Always has something to say."),
            RoleDefinition("screenshot_goblin", "Screenshot Goblin", "📸", "Takes screenshots of everything."),
            RoleDefinition("side_quest_enjoyer", "Side Quest Enjoyer", "🧭", "Gets distracted by side quests."),
        ),
    ),
}

CATEGORY_ORDER: tuple[str, ...] = ("platform", "age", "pings", "fun")
