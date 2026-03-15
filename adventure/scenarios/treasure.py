from adventure.models.adventure_state import StageTag, StageTemplate

TREASURE_EVENTS = [
    StageTemplate(
        key="abandoned_campsite",
        title="Abandoned Campsite",
        beats=[
            "A dead campfire still smells like burnt stew.",
            "A dusty backpack sits in the dirt beside a half-buried knife.",
        ],
        choices=["Open the bag", "Check surroundings", "Ignore it"],
        tag=StageTag.TREASURE,
    ),
    StageTemplate(
        key="treasure_cache_1",
        title="Forgotten Cache 1",
        beats=[
            "The party spots forgotten cache marker 1 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Dig for the lockbox", "Watch for ambushers", "Move on quickly"],
        tag=StageTag.TREASURE,
        min_adv_level=2,
    ),
    StageTemplate(
        key="treasure_cache_2",
        title="Forgotten Cache 2",
        beats=[
            "The party spots forgotten cache marker 2 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Dig for the lockbox", "Watch for ambushers", "Move on quickly"],
        tag=StageTag.TREASURE,
        min_adv_level=2,
    ),
    StageTemplate(
        key="treasure_cache_3",
        title="Forgotten Cache 3",
        beats=[
            "The party spots forgotten cache marker 3 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Dig for the lockbox", "Watch for ambushers", "Move on quickly"],
        tag=StageTag.TREASURE,
        min_adv_level=2,
    ),
    StageTemplate(
        key="treasure_cache_4",
        title="Forgotten Cache 4",
        beats=[
            "The party spots forgotten cache marker 4 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Dig for the lockbox", "Watch for ambushers", "Move on quickly"],
        tag=StageTag.TREASURE,
        min_adv_level=2,
    ),
    StageTemplate(
        key="treasure_cache_5",
        title="Forgotten Cache 5",
        beats=[
            "The party spots forgotten cache marker 5 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Dig for the lockbox", "Watch for ambushers", "Move on quickly"],
        tag=StageTag.TREASURE,
        min_adv_level=2,
    ),
    StageTemplate(
        key="treasure_cache_6",
        title="Forgotten Cache 6",
        beats=[
            "The party spots forgotten cache marker 6 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Dig for the lockbox", "Watch for ambushers", "Move on quickly"],
        tag=StageTag.TREASURE,
        min_adv_level=3,
    ),
    StageTemplate(
        key="treasure_cache_7",
        title="Forgotten Cache 7",
        beats=[
            "The party spots forgotten cache marker 7 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Dig for the lockbox", "Watch for ambushers", "Move on quickly"],
        tag=StageTag.TREASURE,
        min_adv_level=3,
    ),
    StageTemplate(
        key="treasure_cache_8",
        title="Forgotten Cache 8",
        beats=[
            "The party spots forgotten cache marker 8 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Dig for the lockbox", "Watch for ambushers", "Move on quickly"],
        tag=StageTag.TREASURE,
        min_adv_level=3,
    ),
    StageTemplate(
        key="treasure_cache_9",
        title="Forgotten Cache 9",
        beats=[
            "The party spots forgotten cache marker 9 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Dig for the lockbox", "Watch for ambushers", "Move on quickly"],
        tag=StageTag.TREASURE,
        min_adv_level=3,
    ),
]
