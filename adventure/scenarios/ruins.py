from adventure.models.adventure_state import StageTag, StageTemplate

RUINS_EVENTS = [
    StageTemplate(
        key="ruined_obelisk",
        title="Ruined Obelisk",
        beats=[
            "A black obelisk rises from cracked ruins.",
            "Symbols shift shape each time you blink.",
        ],
        choices=["Translate symbols", "Break a fragment", "Leave immediately"],
        tag=StageTag.MYSTIC,
        min_adv_level=10,
    ),
    StageTemplate(
        key="ancient_shrine",
        title="Ancient Shrine",
        beats=[
            "Moss-covered pillars circle a glowing shrine.",
            "The carvings pulse as if reacting to your footsteps.",
        ],
        choices=["Study the carvings", "Offer silver", "Touch the altar"],
        tag=StageTag.MYSTIC,
    ),
    StageTemplate(
        key="ruins_hall_1",
        title="Collapsed Archive 1",
        beats=[
            "The party spots collapsed archive marker 1 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=4,
    ),
    StageTemplate(
        key="ruins_hall_2",
        title="Collapsed Archive 2",
        beats=[
            "The party spots collapsed archive marker 2 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=4,
    ),
    StageTemplate(
        key="ruins_hall_3",
        title="Collapsed Archive 3",
        beats=[
            "The party spots collapsed archive marker 3 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=4,
    ),
    StageTemplate(
        key="ruins_hall_4",
        title="Collapsed Archive 4",
        beats=[
            "The party spots collapsed archive marker 4 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=4,
    ),
    StageTemplate(
        key="ruins_hall_5",
        title="Collapsed Archive 5",
        beats=[
            "The party spots collapsed archive marker 5 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=4,
    ),
    StageTemplate(
        key="ruins_hall_6",
        title="Collapsed Archive 6",
        beats=[
            "The party spots collapsed archive marker 6 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=5,
    ),
    StageTemplate(
        key="ruins_hall_7",
        title="Collapsed Archive 7",
        beats=[
            "The party spots collapsed archive marker 7 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=5,
    ),
    StageTemplate(
        key="ruins_hall_8",
        title="Collapsed Archive 8",
        beats=[
            "The party spots collapsed archive marker 8 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=5,
    ),
    StageTemplate(
        key="ruins_hall_9",
        title="Collapsed Archive 9",
        beats=[
            "The party spots collapsed archive marker 9 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=5,
    ),
    StageTemplate(
        key="ruins_hall_10",
        title="Collapsed Archive 10",
        beats=[
            "The party spots collapsed archive marker 10 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=5,
    ),
    StageTemplate(
        key="ruins_hall_11",
        title="Collapsed Archive 11",
        beats=[
            "The party spots collapsed archive marker 11 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=5,
    ),
    StageTemplate(
        key="ruins_hall_12",
        title="Collapsed Archive 12",
        beats=[
            "The party spots collapsed archive marker 12 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=6,
    ),
    StageTemplate(
        key="ruins_hall_13",
        title="Collapsed Archive 13",
        beats=[
            "The party spots collapsed archive marker 13 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Read the wall script", "Dust off a relic", "Back away slowly"],
        tag=StageTag.MYSTIC,
        min_adv_level=6,
    ),
]
