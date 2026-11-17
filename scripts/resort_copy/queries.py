"""Web-search query builders for each research pass."""
from __future__ import annotations

from typing import Any

from . import bedrock

from .config import (
    _cell,
    _tier_key,
)


_CONTINENT_REGION = frozenset(
    {
        "africa",
        "asia",
        "europe",
        "north-america",
        "oceania",
        "south-america",
        "antarctica",
    }
)


def _place_from_region(region: str | None) -> tuple[str | None, str | None]:
    """country, state from a Geofabrik path when the parquet cells are blank."""
    parts = [part for part in (region or "").split("/") if part and part not in _CONTINENT_REGION]
    if not parts:
        return None, None
    country = parts[0].replace("-", " ").title()
    state = parts[-1].replace("-", " ").title() if len(parts) >= 2 else None
    if state and state.lower() == country.lower():
        state = None
    return country, state


def _known_state_country(row: dict[str, Any]) -> tuple[str | None, str | None]:
    state = _cell(row, "state")
    country = _cell(row, "country")
    region_country, region_state = _place_from_region(_cell(row, "region"))
    return state or region_state, country or region_country


def _place_query(name: str, state: str | None, country: str | None) -> str:
    loc = " ".join(part for part in (state or "", country or "") if part).strip()
    return f"{name} {loc}".strip()


def _wide_area(tier: str | None) -> bool:
    return _tier_key(tier or "") == "large"


def _long_plate(tier: str | None) -> bool:
    """Medium and large pages need more than the every-resort record list."""
    return _tier_key(tier or "") in {"medium", "large"}


def dossier_queries(name: str, state: str | None, country: str | None, tier: str | None = None) -> list[str]:
    """Anchors, then the searches that turn up why someone would go."""
    place = _place_query(name, state, country)
    quoted = _search_name(name, state)
    queries = [
        f"{quoted} ski opened OR founded OR established",
        f"{quoted} ski pass",
        f"{quoted} ski club OR nonprofit OR \"not-for-profit\" OR private OR municipal",
        f"{place} ski overlooks OR lake OR valley OR mountain",
        f"5 facts about {quoted} ski",
        f"{quoted} ski resort review",
        f"{quoted} ski famous OR racer OR champion OR skier",
        f"{quoted} \"ski-in\" OR \"ski out\" OR lodging OR \"where to stay\"",
        f"{quoted} apres OR nightlife OR family OR festival OR events",
    ]
    queries.extend(reader_queries(name, state))
    queries.extend(record_queries(name, state))
    if _wide_area(tier):
        queries.extend(large_area_queries(name, state))
    if _long_plate(tier):
        queries.extend(depth_queries(name, state))
        queries.append(f"{quoted} site:wikipedia.org ski")
    return queries


def _search_name(name: str, state: str | None) -> str:
    """Quote the resort, then add the state so a shared name stays in the right place."""
    quoted = f'"{name}"'
    if state:
        return f"{quoted} {state}"
    return quoted


def reader_queries(name: str, state: str | None = None) -> list[str]:
    """The four questions an atlas reader asks of a small hill."""
    quoted = _search_name(name, state)
    return [
        f"{quoted} ski \"minutes from\" OR \"km from\" OR \"drive from\" OR airport",
        f"{quoted} ski \"day pass\" OR \"lift ticket\" OR \"day ticket\" OR admission",
        f"{quoted} ski membership cost OR \"season pass\" price OR \"day pass\"",
        f"{quoted} ski beginners OR locals OR members OR \"learned to ski\"",
    ]


def record_queries(name: str, state: str | None = None) -> list[str]:
    """Comparable fields for every hill: price, operator, season, and what is offered."""
    quoted = _search_name(name, state)
    return [
        f"{quoted} ski \"weekend\" ticket OR \"night ticket\" OR \"night pass\" price",
        f"{quoted} ski parking free OR paid OR reservation OR \"day trip\"",
        f"{quoted} ski \"operated by\" OR \"owned by\" OR \"Vail Resorts\" OR Alterra OR Boyne OR Powdr OR \"Compagnie des Alpes\"",
        f"{quoted} ski school OR childcare OR \"kids club\"",
        f"{quoted} ski \"opens in\" OR \"closes in\" OR \"season runs\" OR glacier OR snowmaking",
        f"{quoted} ski \"night skiing\" OR \"terrain park\" OR nordic OR tubing",
        f"{quoted} ski official website OR instagram OR facebook",
        f"{quoted} ski beginner OR intermediate OR powder OR expert terrain",
        f"{quoted} ski \"ski-in ski-out\" OR \"purpose-built\" OR \"old town\"",
        # added explicit queries for lifts, night skiing, and snow tubing
        f"{quoted} ski lifts OR \"chair lifts\" OR \"surface lifts\"",
        f"{quoted} ski \"night skiing\"",
        f"{quoted} ski \"snow tubing\""


    ]


def large_area_queries(name: str, state: str | None = None) -> list[str]:
    """Searches for a linked ski domain. Small hills do not need them."""
    quoted = _search_name(name, state)
    return [
        f"{quoted} ski villages OR sectors OR \"linked to\" OR domain",
        f"{quoted} ski gondola OR \"ski bus\" OR connects OR \"ski safari\"",
        f"{quoted} ski \"World Cup\" OR Olympics OR downhill OR race",
        f"{quoted} ski \"known for\" OR \"famous run\" OR signature OR couloir",
        f"{quoted} ski village \"purpose-built\" OR \"car-free\" OR pedestrian OR \"old town\"",
        f"{quoted} ski \"where to stay\" OR \"best base\" OR \"which village\"",
        f"{quoted} ski hut OR refuge OR \"mountain restaurant\"",
        f"{quoted} ski airport OR \"train station\" OR transfer",
        f"{quoted} ski pass villages OR \"area pass\" OR \"valley pass\"",
        f"{quoted} ski crowded OR quiet OR busiest OR \"avoid the\"",
    ]


def depth_queries(name: str, state: str | None = None) -> list[str]:
    """Page-filling searches that the every-resort record list does not already cover."""
    quoted = _search_name(name, state)
    return [
        f"{quoted} ski \"linked with\" OR neighbour OR \"ski to\" OR border",
        f"{quoted} ski history OR \"named after\" OR \"founded by\" OR film",
        f"{quoted} ski lodge OR \"grand hotel\" OR \"stay at\"",
        f"{quoted} ski lunch OR \"self-service\" OR picnic OR \"mountain lunch\"",
    ]


def followup_queries(
    name: str,
    state: str | None,
    country: str | None,
    missing: list[str],
    *,
    need_color: bool,
    tier: str | None = None,
) -> list[str]:
    place = _place_query(name, state, country)
    quoted = _search_name(name, state)
    queries: list[str] = []
    if "year_started" in missing:
        queries.append(f"{quoted} ski opened OR founded OR established {place}")
    if "pass" in missing:
        queries.append(f"{quoted} ski pass membership")
    if "public_or_private" in missing:
        queries.append(f"{quoted} \"not-for-profit\" OR \"ski club\" OR private OR municipal")
    if "setting" in missing:
        queries.append(f"{place} ski overlooks OR lake OR valley OR view")
    if need_color:
        queries.extend(
            [
                f"5 facts about {quoted} ski resort",
                f"{quoted} ski resort review",
                f"{quoted} ski club OR nonprofit",
                f"{quoted} ski-in ski-out lodging",
                f"{quoted} nightlife OR apres OR \"family mountain\" OR festival",
                f"who skied at {quoted}",
            ]
        )
        queries.extend(reader_queries(name, state))
        queries.extend(record_queries(name, state))
        if _wide_area(tier):
            queries.extend(large_area_queries(name, state))
        if _long_plate(tier):
            queries.extend(depth_queries(name, state))
    return queries


_TOPIC_QUERY = {
    "year_started": '{q} ski opened OR founded OR established',
    "history": '{q} ski history founded opened',
    "pass": '{q} ski "ski pass" OR Epic OR Ikon OR "season pass"',
    "passes": '{q} ski "ski pass" OR Epic OR Ikon OR "season pass"',
    "public_or_private": '{q} ski club OR municipal OR private OR "not-for-profit"',
    "who": '{q} ski beginners OR families OR members',
    "setting": '{q} ski valley OR overlooks OR mountain',
    "access": '{q} ski airport OR "km from" OR "drive from"',
    "price": '{q} ski "lift ticket" OR "day pass" price',
    "prices": '{q} ski "lift ticket" OR "day pass" price',
    "lodging": '{q} ski lodging OR hotel OR "where to stay"',
    "terrain": '{q} ski vertical OR trails OR terrain',
    "lifts": '{q} ski chairlift OR gondola OR lifts',
    "season": '{q} ski season opens OR closes OR snowmaking',
    "school": '{q} ski school OR childcare',
    "scene": '{q} ski restaurant OR village OR festival',
    "food": '{q} ski restaurant OR dining',
    "night": '{q} ski "night skiing"',
    "people": '{q} ski racer OR champion OR olympic',
    "rentals": '{q} ski rentals OR "rental shop" OR "snowboard rental"',
    "ski_instructors": '{q} "ski school" OR "ski instructor" OR "ski lessons"',
    "snowtubing": '{q} "snow tubing" OR "tube park"',
}


def topic_queries(
    name: str,
    state: str | None,
    country: str | None,
    topics: list[str],
    *,
    tier: str | None = None,
) -> list[str]:
    """One search per thin topic. Medium and large also ask for the Wikipedia page."""
    quoted = _search_name(name, state)
    out: list[str] = []
    seen: set[str] = set()
    for topic in topics:
        template = _TOPIC_QUERY.get(topic)
        if not template:
            continue
        query = template.format(q=quoted)
        if query not in seen:
            seen.add(query)
            out.append(query)
    if _long_plate(tier):
        wiki = f"{quoted} site:wikipedia.org"
        if wiki not in seen:
            out.append(wiki)
    if country and "year_started" in topics:
        out.append(f"{quoted} {country} ski resort founded")
    return out


def add_web_snippets(snippets: list[dict[str, Any]], queries: list[str]) -> list[dict[str, Any]]:
    if not queries or bedrock.DDGS is None:
        return snippets
    cap = min(90, max(12, len(queries) * 2))
    more = bedrock.fetch_duckduckgo_snippets(queries, max_per_query=2, max_total=cap)
    bedrock._append_unique_snippets(snippets, more)
    bedrock._renumber_snippet_ids(snippets)
    return snippets
