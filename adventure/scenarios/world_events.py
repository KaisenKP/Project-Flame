from adventure.models.adventure_state import StageTag, StageTemplate

WORLD_EVENTS = [
    StageTemplate(
        key="supply_caravan",
        title="Stranded Supply Caravan",
        beats=[
            "A merchant caravan wheel snaps in a muddy ravine.",
            "Panicked quartermasters offer coin and favors for fast help.",
        ],
        choices=["Repair the axle", "Escort through danger", "Negotiate a toll"],
        tag=StageTag.SOCIAL,
        min_adv_level=4,
    ),
    StageTemplate(
        key="storm_shrine",
        title="Shrine of Static Skies",
        beats=[
            "Blue sparks dance between cracked shrine pillars.",
            "Every hair on your arms rises as thunder gathers overhead.",
        ],
        choices=["Channel the lightning", "Ground the shrine safely", "Loot and run"],
        tag=StageTag.MYSTIC,
        min_adv_level=7,
    ),
    StageTemplate(
        key="rift_aftershock",
        title="Planar Rift Aftershock",
        beats=[
            "A shimmering tear splits the air and throws shadows in reverse.",
            "Fragments of another world spill onto the trail for a few unstable moments.",
        ],
        choices=["Stabilize the rift", "Harvest volatile fragments", "Evacuate the area"],
        tag=StageTag.TRAP,
        min_adv_level=10,
    ),
    StageTemplate(
        key="duelist_challenge",
        title="Challenge of the Crimson Duelist",
        beats=[
            "A masked duelist blocks the road and plants a ceremonial banner.",
            '"Win with honor," they say, "and the path is yours."',
        ],
        choices=["Accept single combat", "Use tactical distractions", "Decline respectfully"],
        tag=StageTag.COMBAT,
        min_adv_level=8,
    ),
    StageTemplate(
        key="lost_apprentice",
        title="The Lost Apprentice",
        beats=[
            "A soaked apprentice mage stumbles out of the brush clutching a broken focus crystal.",
            "They promise secret routes if you help recover their research satchel.",
        ],
        choices=["Track the satchel", "Teach survival basics", "Send them to camp"],
        tag=StageTag.SOCIAL,
        min_adv_level=5,
    ),
    StageTemplate(
        key="titan_footprint",
        title="Footprint of a Sleeping Titan",
        beats=[
            "A crater-sized footprint steams in the earth, still fresh.",
            "Ancient runes pulse under the mud where the titan stepped.",
        ],
        choices=["Survey rune patterns", "Mine the crater edges", "Hide until it passes"],
        tag=StageTag.TREASURE,
        min_adv_level=9,
    ),
]
