from __future__ import annotations


def heat_penalty_multiplier(*, personal_heat: int) -> int:
    heat = max(0, int(personal_heat))
    if heat >= 160:
        return 7600
    if heat >= 120:
        return 8400
    if heat >= 80:
        return 9100
    if heat >= 40:
        return 9700
    return 10000


def can_enter(*, personal_heat: int, robbery_heat: int) -> tuple[bool, str | None]:
    combined = int(personal_heat) + int(robbery_heat)
    if combined >= 210:
        return False, "Heat is too high for this target. Cool down before launching another major score."
    return True, None
