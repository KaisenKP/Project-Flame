from adventure.models.adventure_state import StageTag, StageTemplate

MERCHANT_EVENTS = [
    StageTemplate(
        key="broken_wagon",
        title="Broken Wagon",
        beats=[
            "A merchant wagon blocks the road with one wheel snapped clean off.",
            "One crate is humming. That seems concerning.",
        ],
        choices=["Inspect the cargo", "Set up an ambush", "Take a detour"],
        tag=StageTag.SOCIAL,
    ),
    StageTemplate(
        key="merchant_crossroads_1",
        title="Traveling Caravan 1",
        beats=[
            "The party spots traveling caravan marker 1 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Trade supplies", "Ask for rumors", "Escort the caravan"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="merchant_crossroads_2",
        title="Traveling Caravan 2",
        beats=[
            "The party spots traveling caravan marker 2 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Trade supplies", "Ask for rumors", "Escort the caravan"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="merchant_crossroads_3",
        title="Traveling Caravan 3",
        beats=[
            "The party spots traveling caravan marker 3 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Trade supplies", "Ask for rumors", "Escort the caravan"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="merchant_crossroads_4",
        title="Traveling Caravan 4",
        beats=[
            "The party spots traveling caravan marker 4 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Trade supplies", "Ask for rumors", "Escort the caravan"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="merchant_crossroads_5",
        title="Traveling Caravan 5",
        beats=[
            "The party spots traveling caravan marker 5 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Trade supplies", "Ask for rumors", "Escort the caravan"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="merchant_crossroads_6",
        title="Traveling Caravan 6",
        beats=[
            "The party spots traveling caravan marker 6 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Trade supplies", "Ask for rumors", "Escort the caravan"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="merchant_crossroads_7",
        title="Traveling Caravan 7",
        beats=[
            "The party spots traveling caravan marker 7 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Trade supplies", "Ask for rumors", "Escort the caravan"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="merchant_crossroads_8",
        title="Traveling Caravan 8",
        beats=[
            "The party spots traveling caravan marker 8 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Trade supplies", "Ask for rumors", "Escort the caravan"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="merchant_crossroads_9",
        title="Traveling Caravan 9",
        beats=[
            "The party spots traveling caravan marker 9 carved into old stone.",
            "A strange hush falls over the trail before everything gets louder.",
        ],
        choices=["Trade supplies", "Ask for rumors", "Escort the caravan"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
]
