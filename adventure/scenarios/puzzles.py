from adventure.models.adventure_state import StageTag, StageTemplate

PUZZLE_EVENTS = [
    StageTemplate(
        key="vault_gate",
        title="Sealed Vault Gate",
        beats=[
            "An iron door the size of a house blocks the passage.",
            "Mechanisms spin behind the wall as if the vault is waking up.",
        ],
        choices=["Force it open", "Solve the puzzle", "Fall back"],
        tag=StageTag.PUZZLE,
        party_only=True,
        min_adv_level=6,
    ),
    StageTemplate(
        key="puzzle_lock_1",
        title="Clockwork Lock 1",
        beats=[
            "The party spots clockwork lock marker 1 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Align the runes", "Jam the gears", "Try a brute-force pattern"],
        tag=StageTag.PUZZLE,
        min_adv_level=4,
    ),
    StageTemplate(
        key="puzzle_lock_2",
        title="Clockwork Lock 2",
        beats=[
            "The party spots clockwork lock marker 2 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Align the runes", "Jam the gears", "Try a brute-force pattern"],
        tag=StageTag.PUZZLE,
        min_adv_level=4,
    ),
    StageTemplate(
        key="puzzle_lock_3",
        title="Clockwork Lock 3",
        beats=[
            "The party spots clockwork lock marker 3 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Align the runes", "Jam the gears", "Try a brute-force pattern"],
        tag=StageTag.PUZZLE,
        min_adv_level=4,
    ),
    StageTemplate(
        key="puzzle_lock_4",
        title="Clockwork Lock 4",
        beats=[
            "The party spots clockwork lock marker 4 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Align the runes", "Jam the gears", "Try a brute-force pattern"],
        tag=StageTag.PUZZLE,
        min_adv_level=4,
    ),
    StageTemplate(
        key="puzzle_lock_5",
        title="Clockwork Lock 5",
        beats=[
            "The party spots clockwork lock marker 5 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Align the runes", "Jam the gears", "Try a brute-force pattern"],
        tag=StageTag.PUZZLE,
        min_adv_level=4,
    ),
    StageTemplate(
        key="puzzle_lock_6",
        title="Clockwork Lock 6",
        beats=[
            "The party spots clockwork lock marker 6 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Align the runes", "Jam the gears", "Try a brute-force pattern"],
        tag=StageTag.PUZZLE,
        min_adv_level=5,
    ),
    StageTemplate(
        key="puzzle_lock_7",
        title="Clockwork Lock 7",
        beats=[
            "The party spots clockwork lock marker 7 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Align the runes", "Jam the gears", "Try a brute-force pattern"],
        tag=StageTag.PUZZLE,
        min_adv_level=5,
    ),
    StageTemplate(
        key="puzzle_lock_8",
        title="Clockwork Lock 8",
        beats=[
            "The party spots clockwork lock marker 8 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Align the runes", "Jam the gears", "Try a brute-force pattern"],
        tag=StageTag.PUZZLE,
        min_adv_level=5,
    ),
    StageTemplate(
        key="puzzle_lock_9",
        title="Clockwork Lock 9",
        beats=[
            "The party spots clockwork lock marker 9 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Align the runes", "Jam the gears", "Try a brute-force pattern"],
        tag=StageTag.PUZZLE,
        min_adv_level=5,
    ),
]
