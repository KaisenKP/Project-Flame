from adventure.models.adventure_state import StageTag, StageTemplate

EXPEDITION_EVENTS = [
    StageTemplate(
        key="expedition_sunken_archives",
        title="Sunken Archives",
        beats=[
            "A flooded library hums with old warding magic.",
            "Loose pages drift like fish and arrange into warning symbols.",
        ],
        choices=["Dive for sealed tomes", "Drain a chamber", "Map a safe route"],
        tag=StageTag.MYSTIC,
    ),
    StageTemplate(
        key="expedition_broken_ballista",
        title="Broken Ballista Line",
        beats=[
            "Rusting siege engines point toward a silent canyon.",
            "One cracked frame still creaks as if recently fired.",
        ],
        choices=["Repair and test one", "Scavenge spare parts", "Avoid the kill zone"],
        tag=StageTag.TREASURE,
    ),
    StageTemplate(
        key="expedition_crystal_fog",
        title="Crystal Fog",
        beats=[
            "Needle-thin crystals grow from the ground and ring in the wind.",
            "The fog bends moonlight into confusing doubles of the party.",
        ],
        choices=["Follow the echoes", "Shatter a path", "Mark stones with chalk"],
        tag=StageTag.PUZZLE,
        min_adv_level=2,
    ),
    StageTemplate(
        key="expedition_toll_bridge_ghost",
        title="Ghost Toll Bridge",
        beats=[
            "An invisible collector rattles a chain above a narrow bridge.",
            "Every step forward makes phantom coins clink louder.",
        ],
        choices=["Offer a trinket", "Negotiate passage", "Sprint through"],
        tag=StageTag.SOCIAL,
        min_adv_level=2,
    ),
    StageTemplate(
        key="expedition_glass_mantis",
        title="Glass Mantis Nest",
        beats=[
            "Hollow chimes reveal mantises with translucent blades.",
            "They mirror movements, waiting for the first mistake.",
        ],
        choices=["Strike first", "Distract with light", "Retreat quietly"],
        tag=StageTag.COMBAT,
        min_adv_level=2,
    ),
    StageTemplate(
        key="expedition_ember_well",
        title="Ember Well",
        beats=[
            "A stone well exhales warm sparks instead of water.",
            "Ancient runes suggest the fire grants brief resilience.",
        ],
        choices=["Bottle the embers", "Bless weapons", "Cover the well"],
        tag=StageTag.MYSTIC,
        min_adv_level=2,
    ),
    StageTemplate(
        key="expedition_howling_gearfield",
        title="Howling Gearfield",
        beats=[
            "Giant gears half-buried in moss rotate against no visible engine.",
            "Their timing opens and closes a safe corridor every few seconds.",
        ],
        choices=["Dash on rhythm", "Jam the largest gear", "Look for an override"],
        tag=StageTag.TRAP,
        min_adv_level=3,
    ),
    StageTemplate(
        key="expedition_raven_bargain",
        title="Raven Bargain",
        beats=[
            "A speaking raven offers secrets in exchange for shiny prizes.",
            "It already knows each hero's most expensive mistake.",
        ],
        choices=["Trade a gem", "Ask for directions", "Refuse the deal"],
        tag=StageTag.SOCIAL,
        min_adv_level=3,
    ),
    StageTemplate(
        key="expedition_storm_obelisk",
        title="Storm Obelisk",
        beats=[
            "Lightning circles a black obelisk without touching the ground.",
            "A carved socket matches the party's key fragment.",
        ],
        choices=["Insert the fragment", "Ground the charge", "Take shelter"],
        tag=StageTag.PUZZLE,
        min_adv_level=3,
    ),
    StageTemplate(
        key="expedition_golden_bramble",
        title="Golden Bramble",
        beats=[
            "A thorn maze glitters with coins trapped in the vines.",
            "The path shifts whenever someone reaches for treasure.",
        ],
        choices=["Cut through", "Use mirrors to navigate", "Ignore the coins"],
        tag=StageTag.TREASURE,
        min_adv_level=3,
    ),
    StageTemplate(
        key="expedition_party_banner_challenge",
        title="Banner Challenge",
        beats=[
            "A ceremonial gate scans for multiple adventurer crests.",
            "A booming voice demands a unified party oath.",
        ],
        choices=["Recite an oath", "Present achievements", "Force the gate"],
        tag=StageTag.SOCIAL,
        party_only=True,
        min_adv_level=2,
    ),
    StageTemplate(
        key="expedition_twin_minotaurs",
        title="Twin Minotaur Drill",
        beats=[
            "Two armored minotaurs run synchronized patrol loops.",
            "Their arena floor is marked with tactical lanes.",
        ],
        choices=["Split their formation", "Bait one away", "Hold a shield wall"],
        tag=StageTag.COMBAT,
        party_only=True,
        min_adv_level=3,
    ),
    StageTemplate(
        key="expedition_sapphire_vault",
        title="Sapphire Vault",
        beats=[
            "A sealed vault door reflects faces from years long gone.",
            "Three rotating rings require matching constellations.",
        ],
        choices=["Solve the ring puzzle", "Use brute force", "Search for side entrance"],
        tag=StageTag.PUZZLE,
        min_adv_level=4,
    ),
    StageTemplate(
        key="expedition_basilisk_tracks",
        title="Basilisk Tracks",
        beats=[
            "Stone statues of wildlife line a muddy ravine.",
            "Fresh claw marks confirm the basilisk is nearby.",
        ],
        choices=["Set mirrored shields", "Hunt it", "Move under cover"],
        tag=StageTag.COMBAT,
        min_adv_level=4,
    ),
    StageTemplate(
        key="expedition_moonclock_courtyard",
        title="Moonclock Courtyard",
        beats=[
            "A giant clockface in the ground moves with moonlight only.",
            "Standing on wrong numbers triggers spectral arrows.",
        ],
        choices=["Track the pattern", "Leap between pillars", "Wait for clouds"],
        tag=StageTag.TRAP,
        min_adv_level=4,
    ),
    StageTemplate(
        key="expedition_drifting_market",
        title="Drifting Market",
        beats=[
            "Floating stalls drift in circles around a central lantern.",
            "Vendors trade only in stories, not coin.",
        ],
        choices=["Tell a heroic tale", "Trade a rumor", "Observe and leave"],
        tag=StageTag.SOCIAL,
        min_adv_level=4,
    ),
    StageTemplate(
        key="expedition_sandwurm_ambush",
        title="Sandwurm Ambush",
        beats=[
            "The ground ripples beneath ancient caravan markers.",
            "A sandwurm breaches with armored scales and hooked jaws.",
        ],
        choices=["Climb the rocks", "Target weak scales", "Use bait"],
        tag=StageTag.COMBAT,
        min_adv_level=5,
    ),
    StageTemplate(
        key="expedition_oathfire_shrine",
        title="Oathfire Shrine",
        beats=[
            "Blue flames ignite when each party member speaks their intent.",
            "The shrine rewards honesty and punishes hesitation.",
        ],
        choices=["Speak true vows", "Game the ritual", "Extinguish the flames"],
        tag=StageTag.MYSTIC,
        party_only=True,
        min_adv_level=5,
    ),
    StageTemplate(
        key="expedition_iron_choir",
        title="Iron Choir",
        beats=[
            "Animated helmets hang in rows and sing in perfect harmony.",
            "Their final note resonates with a locked adamant door.",
        ],
        choices=["Match their pitch", "Silence one helm", "Break the lock"],
        tag=StageTag.PUZZLE,
        min_adv_level=5,
    ),
    StageTemplate(
        key="expedition_aurora_colossus",
        title="Aurora Colossus",
        beats=[
            "A colossal sentinel wakes as ribbons of aurora cross the sky.",
            "Its chest core cycles through colors tied to different attacks.",
        ],
        choices=["Exploit color shifts", "Protect the healer", "Aim for the core"],
        tag=StageTag.BOSS,
        party_only=True,
        min_adv_level=6,
    ),
]


def _build_expedition_frontier_events() -> list[StageTemplate]:
    zone_names = [
        "Shatterfield",
        "Cinderpass",
        "Verdant Sink",
        "Frostvault",
        "Thunder Steppe",
        "Obsidian Reach",
        "Hollow Delta",
        "Ivory Scar",
        "Nightglass Basin",
        "Ashen Shelf",
    ]
    complications = [
        "arc traps",
        "bandit scouts",
        "runic storms",
        "mirror phantoms",
        "collapsed tunnels",
        "rogue constructs",
        "toxic blooms",
        "spectral sentries",
        "howling geysers",
        "gravity rifts",
    ]
    stakes = [
        "a missing survey team",
        "an emperor's tribute crate",
        "a cursed waystone",
        "a sealed relic chest",
        "a living map shard",
        "an oath-bound messenger",
        "an unstable mana battery",
        "a fallen sky skiff",
        "a stolen guild banner",
        "a dormant titan key",
    ]
    choice_sets = [
        ["Secure the perimeter", "Send a scout ahead", "Advance through the center"],
        ["Take the high ground", "Disarm hazards", "Lure threats away"],
        ["Decode the markings", "Use a decoy", "Push through quickly"],
        ["Fortify and rest", "Track the enemy trail", "Bypass with stealth"],
        ["Parley with locals", "Prepare an ambush", "Call for regroup"],
    ]
    tags = [
        StageTag.COMBAT,
        StageTag.PUZZLE,
        StageTag.TRAP,
        StageTag.SOCIAL,
        StageTag.MYSTIC,
        StageTag.TREASURE,
        StageTag.COMBAT,
        StageTag.PUZZLE,
        StageTag.SOCIAL,
        StageTag.TRAP,
    ]

    events: list[StageTemplate] = []
    for i in range(1, 101):
        zone = zone_names[(i - 1) % len(zone_names)]
        hazard = complications[(i * 3) % len(complications)]
        stake = stakes[(i * 7) % len(stakes)]
        choice_set = choice_sets[(i + 1) % len(choice_sets)]
        tag = tags[(i - 1) % len(tags)]
        min_adv_level = min(8, 1 + (i // 14))
        party_only = i % 6 == 0 or (tag == StageTag.BOSS)

        events.append(
            StageTemplate(
                key=f"expedition_frontier_{i}",
                title=f"Frontier Dispatch {i}: {zone}",
                beats=[
                    f"Scout reports from {zone} confirm {hazard} blocking the route.",
                    f"The objective is recovering {stake} before rival crews arrive.",
                ],
                choices=choice_set,
                tag=tag,
                party_only=party_only,
                min_adv_level=min_adv_level,
            )
        )

    # Insert high-tier boss checks at clear milestones.
    for boss_idx in (20, 40, 60, 80, 100):
        events[boss_idx - 1] = StageTemplate(
            key=f"expedition_frontier_{boss_idx}",
            title=f"Frontier Apex {boss_idx}",
            beats=[
                "A regional warlord emerges with a champion retinue and siege sigils.",
                "Defeating this force secures a strategic corridor for future expeditions.",
            ],
            choices=["Break their formation", "Target the warlord", "Hold the line together"],
            tag=StageTag.BOSS,
            party_only=True,
            min_adv_level=min(9, 3 + (boss_idx // 20)),
        )

    return events


EXPEDITION_FRONTIER_EVENTS = _build_expedition_frontier_events()
