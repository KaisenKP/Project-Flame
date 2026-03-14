# services/activity_rules.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChatXpRules:
    xp_per_message: int = 12
    cooldown_seconds: int = 5
    min_chars: int = 0
    repeat_window_seconds: int = 60


@dataclass(frozen=True)
class VoiceXpRules:
    messages_equivalent_per_minute: int = 5
    min_session_seconds: int = 300
    ignore_self_deaf: bool = True
    ignore_afk_channel: bool = True


CHAT_XP = ChatXpRules()
VOICE_XP = VoiceXpRules()


def vc_xp_per_minute() -> int:
    return int(VOICE_XP.messages_equivalent_per_minute) * int(CHAT_XP.xp_per_message)
