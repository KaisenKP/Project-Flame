import random


def fmt_int(n: int) -> str:
    return f"{int(n):,}"


def bonus_bp(base: int, bp: int) -> int:
    return max((int(base) * (10_000 + int(bp))) // 10_000, 0)


def roll_bp(chance_bp: int) -> bool:
    chance = max(0, int(chance_bp))
    if chance <= 0:
        return False
    if chance >= 10_000:
        return True
    return random.randint(1, 10_000) <= chance
