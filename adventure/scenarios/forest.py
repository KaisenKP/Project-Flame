from adventure.models.adventure_state import StageTag, StageTemplate

FOREST_EVENTS = [
    StageTemplate(
        key="offended_bear",
        title="The Offended Bush",
        beats=[
            "A nearby bush trembles like it's trying to hold in a secret.",
            "Someone pokes it with a stick. Regret arrives instantly.",
            "The bush explodes open and a massive bear lunges forward, deeply offended.",
        ],
        choices=["Fight the bear", "Climb a tree", "Run away"],
        tag=StageTag.COMBAT,
    ),
    StageTemplate(
        key="forest_path_1",
        title="Whispering Trail 1",
        beats=[
            "The party spots whispering trail marker 1 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
    ),
    StageTemplate(
        key="forest_path_2",
        title="Whispering Trail 2",
        beats=[
            "The party spots whispering trail marker 2 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
    ),
    StageTemplate(
        key="forest_path_3",
        title="Whispering Trail 3",
        beats=[
            "The party spots whispering trail marker 3 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
    ),
    StageTemplate(
        key="forest_path_4",
        title="Whispering Trail 4",
        beats=[
            "The party spots whispering trail marker 4 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
    ),
    StageTemplate(
        key="forest_path_5",
        title="Whispering Trail 5",
        beats=[
            "The party spots whispering trail marker 5 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
    ),
    StageTemplate(
        key="forest_path_6",
        title="Whispering Trail 6",
        beats=[
            "The party spots whispering trail marker 6 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="forest_path_7",
        title="Whispering Trail 7",
        beats=[
            "The party spots whispering trail marker 7 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="forest_path_8",
        title="Whispering Trail 8",
        beats=[
            "The party spots whispering trail marker 8 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="forest_path_9",
        title="Whispering Trail 9",
        beats=[
            "The party spots whispering trail marker 9 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="forest_path_10",
        title="Whispering Trail 10",
        beats=[
            "The party spots whispering trail marker 10 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="forest_path_11",
        title="Whispering Trail 11",
        beats=[
            "The party spots whispering trail marker 11 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="forest_path_12",
        title="Whispering Trail 12",
        beats=[
            "The party spots whispering trail marker 12 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="forest_path_13",
        title="Whispering Trail 13",
        beats=[
            "The party spots whispering trail marker 13 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="forest_path_14",
        title="Whispering Trail 14",
        beats=[
            "The party spots whispering trail marker 14 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="forest_path_15",
        title="Whispering Trail 15",
        beats=[
            "The party spots whispering trail marker 15 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="forest_path_16",
        title="Whispering Trail 16",
        beats=[
            "The party spots whispering trail marker 16 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="forest_path_17",
        title="Whispering Trail 17",
        beats=[
            "The party spots whispering trail marker 17 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="forest_path_18",
        title="Whispering Trail 18",
        beats=[
            "The party spots whispering trail marker 18 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=4,
    ),
    StageTemplate(
        key="forest_path_19",
        title="Whispering Trail 19",
        beats=[
            "The party spots whispering trail marker 19 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Follow the footprints", "Set a cautious trap", "Circle around"],
        tag=StageTag.SOCIAL,
        min_adv_level=4,
    ),
]
