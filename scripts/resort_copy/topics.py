"""Deterministic research categories. Application code, not the model, decides what must be researched.

Query templates use {name}, {region}, and {domain}. A template that needs {domain} is skipped until
the official domain is verified. Templates are grouped by round: round 0 is the focused first try,
later rounds are different wordings so a follow-up never repeats an earlier query.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Topic:
    key: str
    label: str
    group: str
    rounds: tuple[tuple[str, ...], ...]
    keywords: str
    # A candidate sentence must also match this (prices need a number or the word ticket).
    must: str | None = None
    # "bool": presence of an amenity. "text": a stated value. "list": several named values.
    kind: str = "text"
    allow_regional: bool = False
    counts: bool = True
    legacy_topic: str = "other"
    legacy_field: str | None = None
    # Two verified values that capture different groups from this pattern are a conflict.
    conflict_pattern: str | None = None


def _topic(key: str, label: str, group: str, rounds: list[list[str]], keywords: str, **kw) -> Topic:
    return Topic(key, label, group, tuple(tuple(r) for r in rounds), keywords, **kw)


_PASS_WORDS = r"\b(?:epic|ikon|indy|mountain collective|powder alliance|peak pass|rcr rockies|ski ?pass)\b|\b[a-z][\w'’-]+ pass\b"

TOPICS: tuple[Topic, ...] = (
    _topic(
        "location", "Official website and location", "basics",
        [
            ['"{name}" "{region}" official resort'],
            ['site:{domain} directions location', '"{name}" resort {region} address'],
        ],
        r"located|address|miles (?:from|south|north|east|west)|minutes from|\bnear\b|off (?:route|us|interstate)|\bva-?\d+|"
        r"shenandoah|\bin the\b[^.]{0,40}(?:mountains?|valley|county)",
        allow_regional=True, legacy_topic="setting", counts=False,
    ),
    _topic(
        "ownership", "Ownership and operator", "basics",
        [
            ['"{name}" "{region}" ski resort owned operated by'],
            ['site:{domain} about us history ownership', '"{name}" resort owner management company'],
        ],
        r"owned by|operated by|managed by|family[- ]owned|privately owned|owner|ownership|parent company|"
        r"acquired|purchased|\bpart of\b",
        legacy_topic="other", legacy_field="owner",
    ),
    _topic(
        "public_access", "Public ticket access and membership requirements", "access",
        [
            ['site:{domain} lift tickets open to the public membership'],
            ['"{name}" ski area public or private members only', 'site:{domain} faq tickets guests'],
        ],
        r"open to the public|members? only|membership (?:is )?required|private club|public (?:can|may)|"
        r"no membership|non-members?|anyone can|guests? (?:are )?welcome|day tickets? (?:are )?(?:available|sold)",
        legacy_topic="public_or_private",
    ),
    _topic(
        "multi_mountain_pass", "Multi-mountain pass affiliation", "access",
        [
            ['"{name}" multi mountain pass affiliation', 'site:{domain} season passes partner passes'],
            ['"{name}" Indy OR Ikon OR Epic OR "Mountain Collective" pass', 'site:{domain} partner pass benefits'],
        ],
        _PASS_WORDS,
        kind="list", legacy_topic="pass", legacy_field="pass_affiliation",
    ),
    _topic(
        "lift_tickets", "Lift tickets", "tickets",
        [
            ['site:{domain} lift tickets season passes', '"{name}" lift tickets prices {region}'],
            ['site:{domain} lift ticket rates pricing', '"{name}" day ticket price'],
        ],
        r"lift tickets?|day tickets?|all[- ]day|half[- ]day|twilight|night tickets?|ticket (?:price|rates?)",
        must=r"\$\s?\d|ticket", legacy_topic="price", legacy_field="lift_ticket",
    ),
    _topic(
        "season_passes", "Season passes", "tickets",
        [
            ['site:{domain} season passes'],
            ['"{name}" season pass price {region}', 'site:{domain} pass holders benefits'],
        ],
        r"season pass(?:es)?|pass ?holders?|unlimited (?:skiing|access)",
        must=r"\$\s?\d|season pass|passholder|pass holder", legacy_topic="price", legacy_field="season_pass",
    ),
    _topic(
        "terrain_stats", "Ski terrain and trail statistics", "terrain",
        [
            ['site:{domain} skiing snowboarding mountain statistics', '"{name}" trails vertical drop acres'],
            ['site:{domain} trail map trails slopes', '"{name}" skiing terrain trails {region}'],
        ],
        r"\b\d+\s+(?:ski\s+)?(?:trails?|slopes?|runs?|pistes?)\b|vertical (?:drop|rise|feet)|\bacres?\b|"
        r"skiable|longest (?:run|trail)|summit elevation|\bbase elevation",
        must=r"\d", legacy_topic="other", legacy_field="terrain",
        conflict_pattern=r"\b(\d+)\s+(?:ski\s+)?(?:trails?|slopes?|runs?|pistes?)\b",
    ),
    _topic(
        "lifts", "Lift types and lift statistics", "terrain",
        [
            ['site:{domain} lifts chairlifts mountain statistics', '"{name}" ski lifts chairlifts quad high-speed'],
            ['site:{domain} lift status chairlift', '"{name}" number of lifts chairlift gondola'],
        ],
        r"chair ?lifts?|\blifts?\b|gondola|high[- ]speed|\bquad\b|\btriple\b|\bdouble\b|magic carpet|surface lift|"
        r"conveyor|t-bar|rope tow",
        must=r"lift|gondola|carpet|tow", legacy_topic="other",
    ),
    _topic(
        "snowmaking", "Snowmaking", "terrain",
        [['site:{domain} snowmaking'], ['"{name}" snowmaking {region} ski']],
        r"snow ?making|snow guns?|snow cannons?|man-made snow|snowmakers?",
        kind="bool", legacy_topic="other", legacy_field="snowmaking",
    ),
    _topic(
        "night_skiing", "Night skiing", "terrain",
        [['site:{domain} night skiing hours'], ['"{name}" night skiing {region}']],
        r"night ski(?:ing)?|night riding|night sessions?|under the lights|lit (?:trails|slopes)|until \d{1,2}(?::\d\d)?\s?(?:p\.?m|pm)",
        kind="bool", legacy_topic="scene", legacy_field="night_skiing",
    ),
    _topic(
        "terrain_parks", "Terrain parks", "terrain",
        [['site:{domain} terrain parks'], ['"{name}" terrain park {region} snowboard']],
        r"terrain parks?|snow parks?|freestyle (?:park|terrain)|halfpipe|half-pipe",
        kind="bool", legacy_topic="scene", legacy_field="terrain_park",
    ),
    _topic(
        "lessons", "Lessons and ski school", "services",
        [['site:{domain} ski school lessons'], ['"{name}" ski school lessons {region}', 'site:{domain} learn to ski snowboard']],
        r"ski school|snowboard school|lessons?|learn to (?:ski|snowboard)|ski instruction|instructors?",
        kind="bool", legacy_topic="other", legacy_field="ski_school",
    ),
    _topic(
        "rentals", "Equipment rentals", "services",
        [['site:{domain} equipment rentals ski snowboard'], ['"{name}" ski rentals snowboard rental shop {region}']],
        r"(?:ski|snowboard|equipment|gear) rentals?|rental (?:shop|center|equipment|department)|rent (?:skis|a snowboard|equipment)",
        kind="bool", legacy_topic="other",
    ),
    _topic(
        "lodging", "Lodging", "lodging",
        [['site:{domain} lodging accommodations'], ['"{name}" resort lodging where to stay {region}']],
        r"lodging|accommodations?|where to stay|places to stay|overnight|resort (?:rooms?|stay)",
        legacy_topic="lodging",
    ),
    _topic(
        "lodging_hotel", "Lodging: hotel rooms", "lodging",
        [['site:{domain} hotel rooms suites inn'], ['"{name}" resort hotel rooms {region}']],
        r"\bhotel\b|\binn\b|hotel rooms?|guest rooms?|\bsuites?\b|resort rooms?|lodge rooms?",
        kind="bool", legacy_topic="lodging",
    ),
    _topic(
        "lodging_condo", "Lodging: condo rentals", "lodging",
        [['site:{domain} condo rentals'], ['"{name}" condo rentals {region}']],
        r"\bcondos?\b|condominiums?",
        kind="bool", legacy_topic="lodging",
    ),
    _topic(
        "lodging_townhouse", "Lodging: townhouse rentals", "lodging",
        [['site:{domain} townhouse townhome rentals'], ['"{name}" townhouse townhome rentals {region}']],
        r"town ?houses?|town ?homes?",
        kind="bool", legacy_topic="lodging",
    ),
    _topic(
        "lodging_house", "Lodging: detached-home rentals", "lodging",
        [
            ['"{name}" vacation house rentals', 'site:{domain} vacation homes houses chalets'],
            ['"{name}" {region} single family home rental sleeps bedrooms', 'site:{domain} vacation rentals homes'],
        ],
        r"\b(?:vacation|rental|private|single[- ]family|detached|mountain|luxury)\s+(?:homes?|houses?)\b|"
        r"\b(?:houses?|homes?|cabins?|chalets?)\s+(?:for rent|rentals?|available)\b|"
        r"\b\d+\s?-?\s?(?:bedroom|br)\s+(?:homes?|houses?|cabins?|chalets?)\b|\bchalets?\b",
        kind="bool", legacy_topic="lodging",
    ),
    _topic(
        "lodging_timeshare", "Lodging: timeshare and vacation ownership", "lodging",
        [['site:{domain} vacation ownership timeshare'], ['"{name}" vacation ownership timeshare {region}']],
        r"timeshares?|vacation ownership|fractional ownership|interval ownership|owners? club|vacation club",
        kind="bool", legacy_topic="lodging",
    ),
    _topic(
        "winter_tubing", "Winter activities: snow tubing", "winter_other",
        [['site:{domain} snow tubing'], ['"{name}" snow tubing {region}']],
        r"snow tubing|tubing|tube park|tubing park",
        kind="bool", legacy_topic="scene", legacy_field="tubing",
    ),
    _topic(
        "winter_skating", "Winter activities: ice skating", "winter_other",
        [['site:{domain} ice skating'], ['"{name}" ice skating rink {region}']],
        r"ice skating|ice rink|skating rink|outdoor rink",
        kind="bool", legacy_topic="scene",
    ),
    _topic(
        "winter_other", "Other non-ski winter activities", "winter_other",
        [['site:{domain} winter activities'], ['"{name}" winter activities besides skiing {region}']],
        r"winter activities|snowshoe|snowshoeing|cross[- ]country|nordic|fat ?bik|sleigh|snowmobil|snow ?park",
        kind="list", legacy_topic="scene",
    ),
    _topic(
        "waterpark", "Indoor waterpark", "year_round",
        [['site:{domain} indoor waterpark'], ['"{name}" indoor water park {region}']],
        r"indoor water ?park|water ?park|aquapark|indoor pool",
        kind="bool", legacy_topic="scene",
    ),
    _topic(
        "year_round", "Major year-round amenities", "year_round",
        [
            ['site:{domain} things to do activities'],
            ['"{name}" resort year round activities golf spa zip line', 'site:{domain} summer activities'],
        ],
        r"\bgolf\b|\bspa\b|zip ?lines?|mountain bik(?:e|ing)|bike park|hiking|adventure (?:park|center)|"
        r"aerial|disc golf|ropes course|rock climbing|horseback|water ?park|restaurants?|dining",
        kind="list", legacy_topic="scene",
    ),
    _topic(
        "parking_access", "Parking and practical access", "access",
        [
            ['site:{domain} parking directions'],
            ['"{name}" resort parking free shuttle directions {region}', 'site:{domain} getting here'],
        ],
        r"parking|shuttle|directions|driving|interstate|\bexit \d+|airport|miles from|minutes from",
        must=r"parking|shuttle|miles|minutes|exit|airport|directions", legacy_topic="access", legacy_field="parking",
    ),
    _topic(
        "opening_year", "Opening year and background", "background",
        [['"{name}" ski resort opened history {region}'], ['site:{domain} history about']],
        r"\b(?:opened|founded|established|began|built)\b[^.]{0,60}\b(?:18|19|20)\d{2}\b|\b(?:18|19|20)\d{2}\b[^.]{0,40}\b(?:opened|founded|established)\b",
        counts=False, legacy_topic="year_started", legacy_field="year_started",
        conflict_pattern=r"\b((?:18|19|20)\d{2})\b",
    ),
)

TOPIC_BY_KEY: dict[str, Topic] = {t.key: t for t in TOPICS}

PASS_TOPIC = "multi_mountain_pass"

# Attractions a resort's own navigation may reveal. Every hit becomes a fact with the nav label as evidence.
ATTRACTION_NAV = (
    "golf", "spa", "zip line", "zipline", "mountain biking", "bike park", "water park", "waterpark",
    "adventure park", "adventure center", "ropes course", "aerial adventure", "disc golf", "horseback",
    "climbing", "go-karts", "go karts", "mini golf", "arcade", "tennis", "fishing", "hiking",
)


def queries_for(topic: Topic, round_index: int, *, name: str, region: str, domain: str | None) -> list[str]:
    """Round-specific queries with placeholders filled. Domain-only templates are skipped without a domain."""
    if round_index >= len(topic.rounds):
        return []
    out: list[str] = []
    for template in topic.rounds[round_index]:
        if "{domain}" in template and not domain:
            continue
        if not region:
            template = template.replace(' "{region}"', "").replace(" {region}", "")
        query = template.format(name=name, region=region, domain=domain or "")
        out.append(re.sub(r"\s+", " ", query).strip())
    return out


def max_rounds() -> int:
    return max(len(t.rounds) for t in TOPICS)
