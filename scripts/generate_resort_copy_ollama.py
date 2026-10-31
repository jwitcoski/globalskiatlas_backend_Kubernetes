#!/usr/bin/env python3
"""
Short atlas resort blurbs on a local Ollama 8B (the 3070).

Five passes, one after another, on one loaded model. A 3070 (8 GB) cannot
hold five 8B models at once, and swapping models fifty times would not finish
in a night. Each pass is a different instruction on the same model.

  1. Search — the long pass. Anchors are opening year, which pass (if any),
     private or public, and the setting. The town is already in the map data.
     On top of the anchors, a small resort needs 5 cited reasons to visit
     and a larger one needs 8, 10, or 15.
  2. Outline — a few beats, in order. Not a page outline.
  3. Writer — plain paragraphs from the outline and the notes.
  4. Critique — unsupported claims, cliches, and fixes. No new facts.
  5. Editor — applies the critique and lands inside the plate's character,
     line, and paragraph ranges. A short draft gets one more pass that adds
     only material from the notes.

The ranges come from the 6 pt print columns (see layout_instruction in
generate_resort_copy_bedrock).

Defaults: llama3.1:8b at http://192.168.68.59:11434.

Usage:
  python scripts/generate_resort_copy_ollama.py --list-models
  python scripts/generate_resort_copy_ollama.py --smoke
  python scripts/generate_resort_copy_ollama.py --demo-query "Massanutten" --demo-state Virginia --tier small
  python scripts/generate_resort_copy_ollama.py -i output/combined/ski_areas_analyzed.parquet --batch 50 --dry-batch
  python scripts/generate_resort_copy_ollama.py -i output/combined/ski_areas_analyzed.parquet --batch 50
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import generate_resort_copy_bedrock as bedrock  # noqa: E402

TIER_CHOICES: tuple[str, ...] = (*bedrock.PLATE_TIERS, *bedrock._TIER_ALIASES)

# Extra visit facts on top of the anchors. The town is not one of them.
TIER_FACT_MIN: dict[str, int] = {
    "small": 6,
    "medium": 12,
    "large": 15,
    "mega": 15,
}

TIER_SNIPPETS: dict[str, int] = {
    "small": 16,
    "medium": 26,
    "large": 30,
    "mega": 30,
}

# Enough beats for the anchors plus a few visit facts. The writer still gets every note.
TIER_BEATS: dict[str, int] = {
    "small": 8,
    "medium": 16,
    "large": 24,
    "mega": 24,
}

# The atlas already knows the town from OpenStreetMap. Do not spend a fact on it.
REQUIRED_TOPICS: tuple[str, ...] = (
    "year_started",
    "pass",
    "public_or_private",
    "setting",
)

COLOR_TOPICS: tuple[str, ...] = (
    "famous_people",
    "lodging",
    "scene",
    "review",
    "access",
    "price",
    "who",
    "other",
)

DEFAULT_HOST = "http://192.168.68.59:11434"
KEY_POINTS_PATH = Path("output/resort_key_points/key_points.jsonl")

# OSM does not have these. The search pass keeps each one only when a snippet says it.
_KEY_POINT_FIELDS = (
    "website",
    "owner",
    "operator_group",
    "pass_affiliation",
    "operator_status",
    "year_started",
    "lift_ticket",
    "weekend_ticket",
    "night_ticket",
    "season_pass",
    "drive",
    "parking",
    "visit_type",
    "ski_school",
    "childcare",
    "season_open",
    "season_close",
    "snowmaking",
    "glacier",
    "night_skiing",
    "terrain_park",
    "nordic",
    "tubing",
    "village_type",
    "sectors",
    "terrain",
    "famous_for",
    "on_hill_lodge",
)
_YES_NO_FIELDS = (
    "ski_school",
    "childcare",
    "snowmaking",
    "glacier",
    "night_skiing",
    "terrain_park",
    "nordic",
    "tubing",
)
_SOCIAL_HOSTS = {
    "instagram.com": "instagram",
    "facebook.com": "facebook",
    "twitter.com": "twitter",
    "x.com": "x",
    "youtube.com": "youtube",
    "tiktok.com": "tiktok",
}
_NOT_RESORT_SITE = (
    "wikipedia.org",
    "wikidata.org",
    "tripadvisor.",
    "booking.com",
    "expedia.",
    "snow-forecast.com",
    "j2ski.com",
    "onthesnow.",
    "skiresort.com",
    "snow-online.com",
    "skicentral.com",
    "yelp.com",
    "google.",
    "grokipedia.com",
    "youtube.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "tiktok.com",
    "x.com",
)
DEFAULT_MODEL = "llama3.1:8b"
DEFAULT_NUM_CTX = 4096

_PREAMBLE_RE = re.compile(
    r"^(?:"
    r"here(?:'s| is) (?:the |a |your )?(?:paragraph|copy|blurb|draft|prose)[:\s]*|"
    r"atlas (?:paragraph|copy)[:\s]*|"
    r"sure[,!]?\s*(?:here(?:'s| is)[^:\n]*:?\s*)?"
    r")",
    re.IGNORECASE,
)


def ollama_base_url(raw: str | None) -> str:
    host = (raw or os.environ.get("OLLAMA_HOST") or DEFAULT_HOST).strip().rstrip("/")
    if "://" not in host:
        host = "http://" + host
    return host


def _tier_key(tier: str) -> str:
    return bedrock.canonical_tier(tier)


def _num_predict(tier: str) -> int:
    """About four characters per token, plus room for the model to finish a sentence."""
    return max(700, min(4096, bedrock.copy_goal_for_tier(tier).characters[1] // 3))


def ollama_chat(
    *,
    base_url: str,
    model: str,
    system_text: str,
    user_text: str,
    temperature: float,
    num_predict: int,
    num_ctx: int,
    timeout: float,
) -> str:
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ],
        "stream": False,
        "keep_alive": "30m",
        "options": {
            "temperature": temperature,
            "num_predict": num_predict,
            "num_ctx": num_ctx,
        },
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base_url + "/api/chat",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw_body = resp.read().decode("utf-8", errors="replace")
                payload = json.loads(raw_body)
            message = payload.get("message") or {}
            text = message.get("content") or ""
            if not text.strip():
                err = payload.get("error") or "empty reply"
                raise RuntimeError(f"Ollama returned no text: {err}")
            return text.strip()
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"Ollama {e.code} from {base_url}: {detail}") from e
        except (urllib.error.URLError, json.JSONDecodeError, TimeoutError, ConnectionError, RuntimeError) as e:
            last_error = e
            if attempt == 2:
                break
            print(
                f"    ollama connection failed ({e}); retry {attempt + 2}/3",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(8 * (attempt + 1))
    raise RuntimeError(
        f"Cannot reach Ollama at {base_url} ({last_error}). "
        "The box closed the connection or returned an empty reply."
    ) from last_error


def list_models(base_url: str, timeout: float = 8.0) -> list[str]:
    req = urllib.request.Request(base_url + "/api/tags", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.URLError as e:
        raise SystemExit(f"Cannot reach Ollama at {base_url} ({e.reason}).") from e
    return [str(m.get("name")) for m in payload.get("models") or [] if m.get("name")]


def _parse_json_loose(raw: str) -> dict[str, Any]:
    s = bedrock._strip_json_fence(raw)
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        start = s.find("{")
        end = s.rfind("}")
        if start >= 0 and end > start:
            return json.loads(s[start : end + 1])
        raise


def _trim_snippets(snippets: list[dict[str, Any]], *, limit: int, chars: int) -> list[dict[str, Any]]:
    """Keep Wikipedia first, then the rest, short enough for an 8B context."""
    wiki = [s for s in snippets if s.get("source") == "wikipedia"]
    rest = [s for s in snippets if s.get("source") != "wikipedia"]
    chosen = (wiki + rest)[:limit]
    out: list[dict[str, Any]] = []
    for s in chosen:
        text = str(s.get("text") or "")
        if len(text) > chars:
            text = text[:chars].rsplit(" ", 1)[0] + "…"
        item = dict(s)
        item["text"] = text
        out.append(item)
    return out


def _bullets_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("bullets")
    if not isinstance(raw, list):
        return []
    bullets: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        ids = item.get("snippet_ids")
        if not isinstance(ids, list):
            ids = []
        bullets.append(
            {
                "topic": str(item.get("topic") or "other"),
                "text": text,
                "snippet_ids": [str(x).strip() for x in ids if str(x).strip()],
            }
        )
    return bullets


def _keep_grounded(
    bullets: list[dict[str, Any]], snippets: list[dict[str, Any]], *, resort_name: str
) -> list[dict[str, Any]]:
    known = {str(s.get("id")) for s in snippets}
    by_id = bedrock._snippet_text_by_id(snippets)
    kept: list[dict[str, Any]] = []
    for b in bullets:
        ids = [i for i in b["snippet_ids"] if i in known]
        if b["snippet_ids"] and not ids:
            continue
        if not bedrock._pass_network_fact_snippet_grounded(b["text"], ids, by_id):
            continue
        cited = " ".join(by_id.get(i, "") for i in ids)
        names_a_pass = bool(bedrock._pass_brand_grounding_needles(b["text"])) or b.get("topic") == "pass"
        if names_a_pass and not _mentions_resort(cited, resort_name):
            continue
        kept.append({"topic": b.get("topic") or "other", "text": b["text"], "snippet_ids": ids})
    return kept


def _chat_json(
    *,
    base_url: str,
    model: str,
    system_text: str,
    user_text: str,
    temperature: float,
    num_predict: int,
    num_ctx: int,
    timeout: float,
) -> dict[str, Any]:
    raw = ollama_chat(
        base_url=base_url,
        model=model,
        system_text=system_text,
        user_text=user_text,
        temperature=temperature,
        num_predict=num_predict,
        num_ctx=num_ctx,
        timeout=timeout,
    )
    try:
        return _parse_json_loose(raw)
    except json.JSONDecodeError:
        raw = ollama_chat(
            base_url=base_url,
            model=model,
            system_text=system_text + " Reply was not JSON. Return only the JSON object.",
            user_text=user_text,
            temperature=0.0,
            num_predict=num_predict,
            num_ctx=num_ctx,
            timeout=timeout,
        )
        return _parse_json_loose(raw)


def fact_min_for(tier: str) -> int:
    return TIER_FACT_MIN.get(_tier_key(tier), TIER_FACT_MIN["medium"])


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
    return _tier_key(tier or "") in {"large", "mega"}


def _long_plate(tier: str | None) -> bool:
    """Medium and up have enough page that a thin search runs out of facts."""
    return _tier_key(tier or "") in {"medium", "large", "mega"}


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
        queries.extend(mega_queries(name, state))
    if _long_plate(tier):
        queries.extend(depth_queries(name, state))
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
    ]


def mega_queries(name: str, state: str | None = None) -> list[str]:
    """Ten searches for a linked ski domain. Small hills do not need them."""
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
            queries.extend(mega_queries(name, state))
        if _long_plate(tier):
            queries.extend(depth_queries(name, state))
    return queries


def add_web_snippets(snippets: list[dict[str, Any]], queries: list[str]) -> list[dict[str, Any]]:
    if not queries or bedrock.DDGS is None:
        return snippets
    cap = min(90, max(12, len(queries) * 2))
    more = bedrock.fetch_duckduckgo_snippets(queries, max_per_query=2, max_total=cap)
    bedrock._append_unique_snippets(snippets, more)
    bedrock._renumber_snippet_ids(snippets)
    return snippets


_GENERIC_NAME_WORDS = frozenset(
    {
        "ski",
        "area",
        "resort",
        "mountain",
        "mountains",
        "club",
        "the",
        "and",
        "of",
        "de",
        "la",
        "le",
    }
)

_DOSSIER_WORDS: tuple[str, ...] = (
    "open",
    "opened",
    "founded",
    "established",
    "pass",
    "private",
    "public",
    "municipal",
    "club",
    "nonprofit",
    "profit",
    "overlook",
    "lake",
    "valley",
    "famous",
    "racer",
    "champion",
    "lodging",
    "hotel",
    "apres",
    "nightlife",
    "family",
    "festival",
    "event",
    "village",
    "gondola",
    "world cup",
    "olympic",
    "glacier",
    "hut",
    "refuge",
    "airport",
    "train",
    "car-free",
    "crowded",
    "operated",
    "owned",
    "snowmaking",
    "school",
    "lesson",
    "powder",
    "piste",
    "parking",
    "nordic",
    "night",
    "history",
    "lunch",
    "weekend",
    "ticket",
    "childcare",
    "tubing",
    "instagram",
    "facebook",
    "alterra",
    "boyne",
    "powdr",
    "beginner",
    "expert",
)


def _name_needles(name: str) -> list[str]:
    """Phrases a page must contain before it can support a fact about this resort."""
    raw = re.sub(r"\s+", " ", (name or "").lower()).strip()
    needles: list[str] = []
    if len(raw) >= 4:
        needles.append(raw)
    for token in re.findall(r"[a-z0-9]+", raw):
        if token in _GENERIC_NAME_WORDS or len(token) < 4:
            continue
        if token not in needles:
            needles.append(token)
    return needles or ([raw] if raw else [])


def _mentions_resort(blob: str, name: str) -> bool:
    text = blob.lower()
    return any(needle in text for needle in _name_needles(name))


# A snippet that says ", California" is not evidence for a Pennsylvania hill.
_OTHER_ADMIN = (
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
    "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
    "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
    "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming",
    "ontario", "quebec", "british columbia", "alberta", "manitoba", "saskatchewan",
    "nova scotia", "new brunswick",
)


def _names_another_state(blob: str, state: str | None) -> bool:
    if not state:
        return False
    text = blob.lower()
    ours = state.lower().strip()
    if ours and ours in text:
        return False
    for other in _OTHER_ADMIN:
        if other == ours:
            continue
        if re.search(rf"(?:,|\bin\b|\bof\b)\s+{re.escape(other)}\b", text):
            return True
    return False


def _page_is_this_resort(blob: str, name: str, state: str | None) -> bool:
    """'Bear' is not enough. The full name, two name words, or the name plus the state."""
    if _names_another_state(blob, state):
        return False
    text = blob.lower()
    full = re.sub(r"\s+", " ", (name or "").lower()).strip()
    if len(full) >= 4 and full in text:
        return True
    tokens = [needle for needle in _name_needles(name) if " " not in needle]
    if any(len(token) >= 8 and token in text for token in tokens):
        return True
    state_name = (state or "").lower().strip()
    if state_name and state_name in text and any(token in text for token in tokens):
        return True
    if len(tokens) >= 2 and sum(token in text for token in tokens) >= 2:
        return True
    return False


def _drop_pages_about_other_places(
    snippets: list[dict[str, Any]], name: str, state: str | None = None
) -> list[dict[str, Any]]:
    """A national Epic-vs-Ikon roundup is not evidence this hill is on either pass."""
    kept: list[dict[str, Any]] = []
    for snippet in snippets:
        blob = f"{snippet.get('title') or ''} {snippet.get('text') or ''}"
        if _page_is_this_resort(blob, name, state):
            kept.append(snippet)
    if kept:
        bedrock._renumber_snippet_ids(kept)
    return kept


# One reserved slot each, so a late About page is not pushed out by the first ten hits.
_TOPIC_BUCKETS: tuple[tuple[str, str], ...] = (
    ("year", r"\b(opened|founded|established|since)\b|\b(18|19|20)\d{2}\b"),
    ("status", r"not-for-profit|not for profit|nonprofit|non-profit|ski club|municipal"),
    ("pass", r"\b(epic|ikon|indy|collective|magic)\b|\b[\w'’-]+\s+pass\b"),
    ("setting", r"overlook|\blake\b|\bbay\b|\bvalley\b|\bview"),
    ("review", r"\breview\b|learned to ski|worth the"),
    ("lodging", r"ski-in|ski out|slopeside|\blodge\b|where to stay"),
    ("scene", r"night ski|apr[eè]s|festival|snowshoe|terrain park|\bfamily\b"),
    ("story", r"founded by|named after|bring winter|history"),
    ("access", r"minutes from|km from|kilometres|kilometers|drive from|airport|southwest of"),
    ("price", r"day pass|lift ticket|day ticket|admission|\$|cad\s?\d|usd\s?\d|€\s?\d"),
    ("who", r"beginner|locals|members|learned to ski|ski club"),
    ("operator", r"operated by|owned by|vail resorts|alterra|boyne|powdr|compagnie des alpes"),
    ("season", r"opens in|closes in|season runs|snowmaking|glacier"),
    ("amenities", r"night skiing|terrain park|nordic|tubing|ski school|childcare|kids club"),
    ("parking", r"free parking|paid parking|parking fee|day trip"),
)

_DIRECTORY_PAGE = re.compile(
    r"booking\.com|skicentral|find the perfect|vacation rental|hotels\.com|expedia\.com",
    re.IGNORECASE,
)
_READER_NOTE = re.compile(
    r"day pass|lift ticket|day ticket|\$|cad\s?\d|minutes from|km from|kilomet|"
    r"drive from|beginner|members|learned to ski|not-for-profit|ski club",
    re.IGNORECASE,
)
_STAT_RECAP = re.compile(
    r"\b(skiable acres|acres|acreage|vertical|elevation of|elevation|chairlift|number of lifts|trail count)\b",
    re.IGNORECASE,
)


def _snippet_blob(snippet: dict[str, Any]) -> str:
    return f"{snippet.get('title') or ''} {snippet.get('text') or ''}".lower()


def _snippet_score(snippet: dict[str, Any], name: str) -> int:
    blob = _snippet_blob(snippet)
    url = str(snippet.get("url") or "").lower()
    score = sum(1 for word in _DOSSIER_WORDS if word in blob)
    if _DIRECTORY_PAGE.search(url) or "find the perfect" in blob:
        score -= 8
    if any(needle in url for needle in _name_needles(name)):
        score += 5
    title = str(snippet.get("title") or "").lower()
    if snippet.get("source") == "wikipedia" and "ski" in title:
        score += 2
    if _STAT_RECAP.search(blob):
        score -= 3
    return score


def _trim_snippet(snippet: dict[str, Any], chars: int) -> dict[str, Any]:
    text = str(snippet.get("text") or "")
    if len(text) > chars:
        text = text[:chars].rsplit(" ", 1)[0] + "…"
    item = dict(snippet)
    item["text"] = text
    return item


def _select_snippets(
    snippets: list[dict[str, Any]], *, limit: int, chars: int, name: str, state: str | None = None
) -> list[dict[str, Any]]:
    """Keep one strong page per fact, then fill. Fetch order must not decide."""
    snippets = _drop_pages_about_other_places(snippets, name, state)
    ranked = sorted(snippets, key=lambda snippet: _snippet_score(snippet, name), reverse=True)
    chosen: list[dict[str, Any]] = []
    seen: set[str] = set()

    def take(snippet: dict[str, Any]) -> None:
        key = str(snippet.get("url") or id(snippet))
        if key in seen:
            return
        seen.add(key)
        chosen.append(_trim_snippet(snippet, chars))

    for _bucket, pattern in _TOPIC_BUCKETS:
        if len(chosen) >= limit:
            break
        best: dict[str, Any] | None = None
        best_score = 0
        for snippet in ranked:
            if str(snippet.get("url") or id(snippet)) in seen:
                continue
            if not re.search(pattern, _snippet_blob(snippet)):
                continue
            score = _snippet_score(snippet, name)
            if score > best_score:
                best = snippet
                best_score = score
        if best is not None:
            take(best)
    for snippet in ranked:
        if len(chosen) >= limit:
            break
        if _snippet_score(snippet, name) <= 0:
            continue
        take(snippet)
    return chosen[:limit]


def _note_from_item(topic: str, item: dict[str, Any]) -> dict[str, Any] | None:
    text = str(item.get("text") or "").strip()
    if not text or text.lower().startswith("not found"):
        return None
    ids = item.get("snippet_ids")
    if not isinstance(ids, list):
        ids = []
    return {
        "topic": topic,
        "text": text,
        "snippet_ids": [str(x).strip() for x in ids if str(x).strip()],
    }


def _is_place_restatement(text: str, name: str, state: str | None, country: str | None) -> bool:
    """True when a note only repeats the town the map already has."""
    if re.search(
        r"overlook|lake|bay|valley|ridge|glacier|forest|coast|river|cape|island|view|summit|base",
        text,
        re.IGNORECASE,
    ):
        return False
    blob = text.lower()
    for part in (name, state or "", country or ""):
        for token in re.findall(r"[a-z0-9]+", part.lower()):
            if len(token) >= 3:
                blob = blob.replace(token, " ")
    for filler in (
        "located",
        "situated",
        "based",
        "found",
        "town",
        "village",
        "community",
        "place",
        "ski",
        "resort",
        "area",
        "mountain",
        "canada",
        "in",
        "the",
        "of",
        "a",
        "an",
        "and",
        "is",
        "at",
    ):
        blob = re.sub(rf"\b{filler}\b", " ", blob)
    return len(re.findall(r"[a-z0-9]+", blob)) < 3


def _notes_from_payload(
    payload: dict[str, Any],
    fact_min: int,
    *,
    name: str,
    state: str | None,
    country: str | None,
) -> list[dict[str, Any]]:
    notes: list[dict[str, Any]] = []
    for topic in REQUIRED_TOPICS:
        item = payload.get(topic)
        if isinstance(item, dict):
            note = _note_from_item(topic, item)
            if note is not None:
                notes.append(note)
    allowed = set(COLOR_TOPICS)
    for bullet in _bullets_from_payload(payload):
        topic = str(bullet.get("topic") or "other").strip().lower()
        bullet["topic"] = topic if topic in allowed else "other"
        notes.append(bullet)
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for note in notes:
        key = note["text"].lower()
        if key in seen or key.startswith("not found"):
            continue
        if note.get("topic") not in REQUIRED_TOPICS and _is_place_restatement(
            note["text"], name, state, country
        ):
            continue
        if _DIRECTORY_PAGE.search(note["text"]) or "find the perfect" in key:
            continue
        if note.get("topic") not in REQUIRED_TOPICS and _STAT_RECAP.search(note["text"]):
            continue
        seen.add(key)
        unique.append(note)
    tagged = _tag_required_topics(unique)
    anchors = [n for n in tagged if n.get("topic") in REQUIRED_TOPICS]
    color = [n for n in tagged if n.get("topic") not in REQUIRED_TOPICS]
    reader = [n for n in color if n.get("topic") in {"access", "price", "who"} or _READER_NOTE.search(n["text"])]
    rest = [n for n in color if n not in reader]
    return anchors + reader + rest[: max(0, fact_min - len(reader))]


def _tag_required_topics(notes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    have = {n["topic"] for n in notes if n.get("topic") in REQUIRED_TOPICS}
    rules: tuple[tuple[str, str], ...] = (
        ("year_started", r"\b(1[89]\d{2}|20\d{2})\b"),
        ("pass", r"\bpass\b"),
        (
            "public_or_private",
            r"\b(private|public|municipal|town-owned|ski club|nonprofit|non-profit|not-for-profit)\b",
        ),
        ("setting", r"overlook|lake|bay|valley|ridge|glacier|forest|coast|river|view|summit"),
    )
    for note in notes:
        if note.get("topic") in REQUIRED_TOPICS:
            continue
        text = note["text"].lower()
        for topic, pattern in rules:
            if topic in have:
                continue
            if topic == "year_started" and not re.search(
                r"open|found|establish|built|since|began", text
            ):
                continue
            if topic == "pass" and not re.search(
                r"\b(?!season\b|day\b|lift\b|ski\b|the\b|a\b|an\b)[a-z0-9'’-]{3,}\s+pass\b",
                text,
            ):
                continue
            if re.search(pattern, text):
                note["topic"] = topic
                have.add(topic)
                break
    return notes


def _missing_topics(notes: list[dict[str, Any]]) -> list[str]:
    have = {n.get("topic") for n in notes}
    return [topic for topic in REQUIRED_TOPICS if topic not in have]


def _color_notes(notes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [n for n in notes if n.get("topic") not in REQUIRED_TOPICS]


def search_notes(
    snippets_block: str,
    *,
    resort_label: str,
    name: str,
    state: str | None,
    country: str | None,
    fact_min: int,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Search pass: four anchors, then cited reasons a visitor would care."""
    known = ", ".join(part for part in (name, state, country) if part)
    system = (
        "You are the search pass for a ski atlas. SNIPPETS were already fetched. "
        "Use only SNIPPETS. Output one JSON object and nothing else. Schema: "
        '{"year_started":{"text":"when it opened or was founded","snippet_ids":["D1"]},'
        '"pass":{"text":"the named pass product that includes THIS resort","snippet_ids":["D2"]},'
        '"public_or_private":{"text":"private, public, municipal, or not-for-profit club","snippet_ids":["D3"]},'
        '"setting":{"text":"what it overlooks or the landform, not the town name","snippet_ids":["W2"]},'
        '"key_points":{"website":{"text":"official website url","snippet_ids":["D1"]},'
        '"owner":{"text":"who owns or operates it","snippet_ids":["D2"]},'
        '"operator_group":{"text":"Vail Resorts, Alterra, Boyne, Powdr, Compagnie des Alpes, or independent","snippet_ids":["D3"]},'
        '"pass_products":[{"text":"one pass this resort is on","snippet_ids":["D4"]}],'
        '"lift_ticket":{"text":"adult day price with currency","snippet_ids":["D5"]},'
        '"weekend_ticket":{"text":"weekend day price with currency","snippet_ids":["D6"]},'
        '"night_ticket":{"text":"night ticket price with currency","snippet_ids":["D7"]},'
        '"season_pass":{"text":"season pass or membership price with currency","snippet_ids":["D8"]},'
        '"drive":{"text":"minutes or km to a named city or airport","snippet_ids":["D9"]},'
        '"parking":{"text":"free, paid, or reservation","snippet_ids":["D10"]},'
        '"visit_type":{"text":"day trip or stay over","snippet_ids":["D11"]},'
        '"audience":[{"text":"club, beginners, families, destination, or experts","snippet_ids":["D12"]}],'
        '"ski_school":{"text":"yes or no","snippet_ids":["D13"]},'
        '"childcare":{"text":"yes or no","snippet_ids":["D14"]},'
        '"season_open":{"text":"month it opens","snippet_ids":["D15"]},'
        '"season_close":{"text":"month it closes","snippet_ids":["D16"]},'
        '"snowmaking":{"text":"yes or no","snippet_ids":["D17"]},'
        '"glacier":{"text":"yes or no","snippet_ids":["D18"]},'
        '"night_skiing":{"text":"yes or no","snippet_ids":["D19"]},'
        '"terrain_park":{"text":"yes or no","snippet_ids":["D20"]},'
        '"nordic":{"text":"yes or no","snippet_ids":["D21"]},'
        '"tubing":{"text":"yes or no","snippet_ids":["D22"]},'
        '"village_type":{"text":"none, ski-in ski-out, purpose-built, or old town","snippet_ids":["D23"]},'
        '"sectors":{"text":"village or sector count, and gondola or ski bus if they are linked","snippet_ids":["D24"]},'
        '"terrain":{"text":"beginner hill, intermediate, powder, park, or expert","snippet_ids":["D25"]},'
        '"famous_for":{"text":"skier or race name and year","snippet_ids":["D26"]},'
        '"on_hill_lodge":{"text":"named lodge on the hill","snippet_ids":["D27"]},'
        '"social":[{"text":"https://instagram.com/example","snippet_ids":["D28"]}]},'
        '"bullets":[{"topic":"famous_people","text":"one concrete fact, 25 words or fewer","snippet_ids":["W1"]}]}. '
        f"The map already knows the place ({known}). Do not write a bullet that only says where it is. "
        "Fill the four named fields from SNIPPETS. "
        f"Then add at least {fact_min} bullets that would make someone want to visit or read on: "
        "a not-for-profit or club detail, a famous skier, ski-in ski-out or nearby lodging, "
        "whether the scene is nightlife or a few seasonal family events, "
        "and one thing a reviewer says about who it suits or what the skiing is like. "
        "A review bullet is one concrete observation. No star rating and no trail or lift count. "
        "Also add bullets for who skis there, how a visitor gets there, who the hill is for, "
        "and the day-pass or membership price. A price must be a number copied from SNIPPETS. "
        "Getting there means a drive, a distance, or the nearest city, not a repeat of the town name. "
        + (
            "This is a large linked area. Also look for which villages or sectors are in it, "
            "how a skier moves between them, a race or event it is known for, one named run or terrain, "
            "whether the base is an old town or purpose-built, which village to stay in, "
            "a mountain hut, how people arrive, the local area pass, and which side is quiet or crowded. "
            if fact_min >= 10
            else ""
        )
        + (
            "Also look for who operates it, the season or snowmaking, a ski school, "
            "a neighbouring area you can ski into, who the terrain suits, parking or a day trip, "
            "a terrain park or night skiing, the name origin or a film, a named lodge, and where lunch is. "
            if fact_min >= 8
            else ""
        )
        + "bullet topic must be famous_people, lodging, scene, review, access, price, who, or other. "
        "Pass products are numerous. Name one only when a snippet says THIS resort is on it. "
        "Ignore a snippet about a different state or a different resort that shares part of the name. "
        "A page that compares Epic, Ikon, Indy, or other passes in general is not evidence. "
        "If a named field is not in SNIPPETS, set its text to \"Not found in search results.\" and snippet_ids to []. "
        "Do not invent a year, a pass, a person, a hotel, a website, or a price. "
        "A ticket field is a day, weekend, night, or season price with the currency copied from SNIPPETS. "
        "A per-night lodging rate is not a ticket. "
        "yes or no fields are yes only when a snippet says the resort has that feature, "
        "and no only when a snippet says it does not. "
        "pass_products lists only passes a snippet includes THIS resort on. "
        "Do not recite vertical, lift counts, trail counts, or acreage."
    )
    payload = _chat_json(
        base_url=base_url,
        model=model,
        system_text=system,
        user_text=f"Resort: {resort_label}\n\nSNIPPETS:\n{snippets_block}\n",
        temperature=0.1,
        num_predict=max(1800, fact_min * 160),
        num_ctx=num_ctx,
        timeout=timeout,
    )
    notes = _notes_from_payload(payload, fact_min, name=name, state=state, country=country)
    return notes, payload.get("key_points") if isinstance(payload.get("key_points"), dict) else {}


def outline_beats(
    notes: list[dict[str, Any]],
    *,
    resort_label: str,
    beat_cap: int,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> list[str]:
    system = (
        "You are the outline pass. Order the NOTES into beats the writer will turn into paragraphs. "
        "Output one JSON object and nothing else. "
        '{"beats":["short beat, 15 words or fewer"]}. '
        f"Use up to {beat_cap} beats. One beat is one paragraph. "
        + (
            "This blurb is a single paragraph, so few beats. "
            if beat_cap <= 6
            else "This is half a page or more. Use a separate beat for each paragraph. Do not collapse the page into one beat. "
        )
        + "Use only NOTES. Do not add facts. "
        "The page title is already the resort name, so do not spend a beat on the name or the state. "
        "One beat is the setting, not the town name. "
        "When NOTES support them, give separate beats to who skis there, how a visitor gets there, who the hill is for, and what a day costs. "
        "If notes name linked villages, a race, or which base to stay in, give those their own beats. "
        "This is not an essay outline. No headings like Historically or Overall."
    )
    payload = _chat_json(
        base_url=base_url,
        model=model,
        system_text=system,
        user_text=f"Resort: {resort_label}\n\nNOTES:\n{json.dumps(notes, ensure_ascii=False)}\n",
        temperature=0.2,
        num_predict=max(400, beat_cap * 40),
        num_ctx=num_ctx,
        timeout=timeout,
    )
    raw = payload.get("beats")
    if not isinstance(raw, list):
        return [n["text"] for n in notes[:beat_cap]]
    beats = [str(b).strip() for b in raw if str(b).strip()]
    return (beats or [n["text"] for n in notes])[:beat_cap]


def _clean_prose(text: str) -> str:
    s = text.strip()
    s = re.sub(r"^```(?:\w+)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    s = _PREAMBLE_RE.sub("", s).strip()
    s = re.sub(r"^#+\s+.*\n+", "", s).strip()
    return bedrock.strip_banned_travel_cliches(s)


def _reader_fixes(draft: str, notes: list[dict[str, Any]], name: str = "") -> list[str]:
    """Tell the editor to put back a reader fact the draft dropped."""
    blob = " ".join(str(note.get("text") or "") for note in notes)
    draft_l = draft.lower()
    fixes: list[str] = []
    opened = draft_l.lstrip()
    if name and opened.startswith(name.lower()):
        fixes.append("The page title is the resort name. Rewrite the opening so it does not start with that name.")
    if re.search(r"\b(18|19|20)\d{2}\b", blob) and not re.search(r"\b(18|19|20)\d{2}\b", draft):
        fixes.append("Put the opening year from NOTES back into the paragraph.")
    if re.search(r"day pass|lift ticket|\$|cad\s?\d|€\s?\d", blob, re.IGNORECASE) and not re.search(
        r"\d|\$|cad|€|£|day pass|ticket", draft, re.IGNORECASE
    ):
        fixes.append("Include the day-pass or ticket price from NOTES. Use the number, not a cheapest ranking.")
    if re.search(r"minutes|km from|kilomet|drive from|airport|southwest", blob, re.IGNORECASE) and not re.search(
        r"minutes|\bkm\b|kilomet|drive|airport|southwest", draft_l
    ):
        fixes.append("Include how a visitor gets there, using the distance or city in NOTES.")
    if re.search(r"not-for-profit|nonprofit|ski club|members|beginners|locals|families", blob, re.IGNORECASE) and not re.search(
        r"not-for-profit|nonprofit|club|member|beginner|local|famil", draft_l
    ):
        fixes.append("Say who skis there and who the hill is for, using NOTES.")
    return fixes


def _fit_to_layout(text: str, tier: str) -> str:
    """Deterministic cut if the model will not cut itself. Keeps paragraph breaks."""
    goal = bedrock.copy_goal_for_tier(tier)
    paras = [re.split(r"(?<=[.!?])\s+", p) for p in bedrock._paragraphs(text)]

    def joined() -> str:
        return "\n\n".join(" ".join(s) for s in paras if s)

    while len(paras) > goal.paragraphs[1]:
        paras[-2].extend(paras.pop())
    while bedrock._copy_over_max(bedrock.measure_copy(joined(), tier), goal):
        if len(paras[-1]) > 1:
            paras[-1].pop()
        elif len(paras) > 1:
            paras.pop()
        else:
            break
    return joined()


def write_draft(
    notes: list[dict[str, Any]],
    beats: list[str],
    *,
    resort_label: str,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> str:
    key = _tier_key(tier)
    length_rule = bedrock.layout_instruction(key) + " "
    system = (
        "You are the writer. Follow OUTLINE order. Use only NOTES. "
        "When notes include the opening year, a pass, private versus public, or the setting, use them. "
        "The town is already on the map, so do not write a sentence that only says where it is. "
        "Prefer notes about who skied there, the club, where to stay, whether the scene is family events or nightlife, and one reviewer observation. "
        "When NOTES include them, the paragraph must say who skis there, how to get there, who the hill is for, and what a day or a membership costs. "
        "Keep the opening year. Quote a price as a number from NOTES, not as a cheapest-in-the-country ranking. "
        "Cut a nearby golf club, banquet hall, or event-space detail before you cut the drive, the price, the year, or who it is for. "
        + length_rule
        + "Do not add years, prices, pass names, or places that are not in NOTES. "
        + "Do not recite vertical, trails, lifts, or acreage. "
        + "The resort name, the lifts, the trails, and the vertical drop are already printed on the page from the database. "
        + "Do not open with the resort name, the state, or the country. Do not repeat those database facts. "
        + "Start with who the hill is for, how a person gets there, what a day costs, or what the place is like. "
        + "One blank line between paragraphs. No title, no bullets, no markdown. "
        + bedrock._BANNED_TRAVEL_CLICHES_RULE
        + " "
        + bedrock._ATLAS_PROSE_LEADIN_RULE
    )
    user = (
        f"Resort: {resort_label}\n\nOUTLINE:\n{json.dumps(beats, ensure_ascii=False)}\n\n"
        f"NOTES:\n{json.dumps(notes, ensure_ascii=False)}\n"
    )
    raw = ollama_chat(
        base_url=base_url,
        model=model,
        system_text=system,
        user_text=user,
        temperature=0.4,
        num_predict=_num_predict(key),
        num_ctx=num_ctx,
        timeout=timeout,
    )
    return _clean_prose(raw)


def critique_draft(
    draft: str,
    notes: list[dict[str, Any]],
    *,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> dict[str, Any]:
    goal = bedrock.copy_goal_for_tier(tier)
    measured = bedrock.measure_copy(draft, tier)
    over = bedrock._copy_over_max(measured, goal)
    if over:
        length_fix = "One fix must say which sentences or paragraph to cut to get under every maximum. "
    elif bedrock._copy_under_min(measured, goal):
        length_fix = "Do not ask for a cut. One fix may name an unused NOTES item to add. "
    else:
        length_fix = "The length is in range. Do not ask for a cut or an addition. "
    system = (
        "You are the critic and fact checker. Compare DRAFT to NOTES. "
        "Output one JSON object and nothing else. Schema: "
        '{"unsupported":["claim in the draft that NOTES do not support"],'
        '"cliches":["banned or empty phrase"],'
        '"fixes":["one imperative fix"]}. '
        f"{bedrock._measured_brief(measured, goal)} "
        f"{length_fix}"
        "fixes must not introduce a new fact. "
        "Flag nestled, hidden gem, world-class, and essay openers such as Overall or Historically. "
        "If the draft is fine, return empty arrays."
    )
    user = f"NOTES:\n{json.dumps(notes, ensure_ascii=False)}\n\nDRAFT:\n{draft}\n"
    try:
        payload = _chat_json(
            base_url=base_url,
            model=model,
            system_text=system,
            user_text=user,
            temperature=0.1,
            num_predict=500,
            num_ctx=num_ctx,
            timeout=timeout,
        )
    except json.JSONDecodeError:
        return {"unsupported": [], "cliches": [], "fixes": []}
    out: dict[str, Any] = {}
    for key in ("unsupported", "cliches", "fixes"):
        raw = payload.get(key)
        if isinstance(raw, list):
            out[key] = [str(x).strip() for x in raw if str(x).strip()]
        else:
            out[key] = []
    draft_lower = draft.lower()
    if not over:
        out["fixes"] = [fix for fix in out["fixes"] if not re.search(r"\bcuts?\b", fix, re.IGNORECASE)]
    out["cliches"] = [phrase for phrase in out["cliches"] if phrase.lower() in draft_lower]
    out["unsupported"] = [
        claim
        for claim in out["unsupported"]
        if claim.lower() not in {"claim in the draft that notes do not support"}
    ]
    return out


def edit_draft(
    draft: str,
    notes: list[dict[str, Any]],
    critique: dict[str, Any],
    *,
    resort_label: str,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> str:
    goal = bedrock.copy_goal_for_tier(tier)
    system = (
        "You are the editor. Apply CRITIQUE fixes. "
        f"{bedrock._measured_brief(bedrock.measure_copy(draft, tier), goal)} "
        f"{bedrock.layout_instruction(tier)} "
        "If the draft is already inside the range, do not shorten it. "
        "Do not add any fact that is not already in NOTES or the draft. "
        "Delete unsupported claims and banned phrases. "
        "The page title is already the resort name, and the fact block already lists lifts, trails, and vertical drop. "
        "If the draft opens with the resort name, the state, or the country, rewrite the opening. Do not repeat those mountain facts. "
        "Plain paragraphs only, one blank line between paragraphs. No title, no markdown. "
        + bedrock._BANNED_TRAVEL_CLICHES_RULE
    )
    user = (
        f"Resort: {resort_label}\n\nNOTES:\n{json.dumps(notes, ensure_ascii=False)}\n\n"
        f"CRITIQUE:\n{json.dumps(critique, ensure_ascii=False)}\n\nDRAFT:\n{draft}\n"
    )
    raw = ollama_chat(
        base_url=base_url,
        model=model,
        system_text=system,
        user_text=user,
        temperature=0.2,
        num_predict=_num_predict(tier),
        num_ctx=num_ctx,
        timeout=timeout,
    )
    return _fit_to_layout(_clean_prose(raw), tier)


def extend_draft(
    draft: str,
    notes: list[dict[str, Any]],
    *,
    resort_label: str,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> str:
    """One pass that adds NOTES material when the edit lands under a minimum."""
    goal = bedrock.copy_goal_for_tier(tier)
    if not bedrock._copy_under_min(bedrock.measure_copy(draft, tier), goal):
        return draft
    system = (
        "You are the editor. The draft is too short for its print column. "
        f"{bedrock._measured_brief(bedrock.measure_copy(draft, tier), goal)} "
        f"{bedrock.layout_instruction(tier)} "
        "Keep every existing sentence. Add sentences and paragraphs that use NOTES items the draft does not use yet. "
        "Do not repeat an idea already in the draft, and do not add any fact that is not in NOTES. "
        "Do not recite vertical, trails, lifts, or acreage. "
        "Plain paragraphs only, one blank line between paragraphs. No title, no markdown. "
        + bedrock._BANNED_TRAVEL_CLICHES_RULE
        + " "
        + bedrock._ATLAS_PROSE_LEADIN_RULE
    )
    user = (
        f"Resort: {resort_label}\n\nNOTES:\n{json.dumps(notes, ensure_ascii=False)}\n\n"
        f"DRAFT:\n{draft}\n"
    )
    raw = ollama_chat(
        base_url=base_url,
        model=model,
        system_text=system,
        user_text=user,
        temperature=0.3,
        num_predict=_num_predict(tier),
        num_ctx=num_ctx,
        timeout=timeout,
    )
    longer = _fit_to_layout(_clean_prose(raw), tier)
    if bedrock._char_count(longer) <= bedrock._char_count(draft):
        return draft
    return longer


def _cited_urls(ids: list[str], snippets: list[dict[str, Any]]) -> list[str]:
    urls: list[str] = []
    for sid in ids:
        for snippet in snippets:
            if str(snippet.get("id")) == sid and snippet.get("url"):
                url = str(snippet["url"])
                if url not in urls:
                    urls.append(url)
    return urls


def _host(url: str) -> str:
    match = re.search(r"https?://([^/]+)", url.lower())
    return match.group(1).removeprefix("www.") if match else ""


def _point_value(text: str, ids: list[str], snippets: list[dict[str, Any]]) -> dict[str, Any] | None:
    cleaned = text.strip()
    if not cleaned or cleaned.lower().startswith("not found"):
        return None
    return {"value": cleaned, "snippet_ids": ids, "sources": _cited_urls(ids, snippets)}


def _social_links(snippets: list[dict[str, Any]], name: str = "") -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    seen: set[str] = set()
    tokens = [needle for needle in _name_needles(name) if " " not in needle and len(needle) >= 4]
    for snippet in snippets:
        url = str(snippet.get("url") or "")
        low_url = url.lower()
        if "/watch" in low_url or "/shorts" in low_url:
            continue
        host = _host(url)
        if tokens and not any(token in low_url for token in tokens):
            continue
        for domain, network in _SOCIAL_HOSTS.items():
            if host == domain or host.endswith("." + domain):
                if url not in seen:
                    seen.add(url)
                    found.append({"network": network, "url": url, "snippet_id": str(snippet.get("id") or "")})
    return found


def _host_matches_resort(host: str, url: str, name: str) -> bool:
    """A short token such as 'bear' is not enough to claim another resort's site."""
    words = [
        word
        for word in re.findall(r"[a-z0-9]+", (name or "").lower())
        if word not in _GENERIC_NAME_WORDS and len(word) >= 3
    ]
    joined = "".join(words[:2])
    if len(joined) >= 6 and joined in host:
        return True
    return any(len(word) >= 6 and word in host for word in words)


def _official_website(snippets: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    for snippet in snippets:
        url = str(snippet.get("url") or "")
        host = _host(url)
        if not host or any(blocked in host for blocked in _NOT_RESORT_SITE):
            continue
        if not _host_matches_resort(host, url, name):
            continue
        path = url.split(host, 1)[-1]
        score = 2 if path in ("", "/", "/index.html") else 1
        if best is None or score > best["score"]:
            best = {
                "score": score,
                "value": url.split("?")[0],
                "snippet_ids": [str(snippet.get("id") or "")],
                "sources": [url],
            }
    if best is None:
        return None
    best.pop("score")
    return best


_MONEY = re.compile(
    r"(?:(?:CAD|USD|EUR|GBP|CHF|SEK|NOK|AUD|NZD)\s?\$?\s?\d[\d,]*(?:\.\d{2})?"
    r"|[$€£]\s?\d[\d,]*(?:\.\d{2})?(?:\s?(?:CAD|USD|EUR|GBP|CHF))?)",
    re.IGNORECASE,
)
_YES_PATTERNS = {
    "ski_school": r"ski school|ski lessons",
    "childcare": r"childcare|kids club|day care|daycare",
    "snowmaking": r"snowmaking|snow making|snow guns",
    "glacier": r"\bglacier\b",
    "night_skiing": r"night ski",
    "terrain_park": r"terrain park",
    "nordic": r"\bnordic\b|cross-country|cross country",
    "tubing": r"\btubing\b",
}
_CHAINS = (
    ("Vail Resorts", r"vail resorts"),
    ("Alterra", r"\balterra\b"),
    ("Boyne", r"\bboyne\b"),
    ("Powdr", r"\bpowdr\b"),
    ("Compagnie des Alpes", r"compagnie des alpes"),
)
_PASS_NAMES = (
    ("Epic", r"\bepic pass\b"),
    ("Ikon", r"\bikon pass\b"),
    ("Indy", r"\bindy pass\b"),
    ("Mountain Collective", r"mountain collective"),
    ("Power Pass", r"\bpower pass\b"),
)
_AUDIENCE = (
    ("club", r"ski club|members only|not-for-profit"),
    ("beginners", r"beginner|learned to ski|never-ever|first time"),
    ("families", r"\bfamil(?:y|ies)\b"),
    ("destination", r"destination resort|fly-?in|international visitors"),
    ("experts", r"\bexperts?\b|advanced terrain|off piste|off-piste"),
)
_MONTHS = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)


def _snippet_text(snippet: dict[str, Any]) -> str:
    return f"{snippet.get('title') or ''} {snippet.get('text') or ''}"


def _as_yes_no(text: str) -> str | None:
    cleaned = text.strip().lower()
    if cleaned in {"yes", "true", "y"}:
        return "yes"
    if cleaned in {"no", "false", "n"}:
        return "no"
    if cleaned.startswith("not found"):
        return None
    if re.search(r"\b(no|not|without)\b", cleaned):
        return "no"
    if re.search(r"\b(yes|has|offers|open)\b", cleaned):
        return "yes"
    return None


def _price_in(text: str, label: str) -> str | None:
    for match in re.finditer(label, text, flags=re.IGNORECASE):
        window = text[max(0, match.start() - 50) : match.end() + 50]
        if re.search(r"per night|a night|/night", window, flags=re.IGNORECASE):
            continue
        money = _MONEY.search(window)
        if money:
            return re.sub(r"\s+", " ", money.group(0)).strip()
    return None


def _from_snippet(snippet: dict[str, Any], value: str) -> dict[str, Any]:
    sid = str(snippet.get("id") or "")
    url = str(snippet.get("url") or "")
    return {
        "value": value,
        "snippet_ids": [sid] if sid else [],
        "sources": [url] if url else [],
    }


def _harvest_key_points(snippets: list[dict[str, Any]]) -> dict[str, Any]:
    """Pull comparable fields straight from snippet text. Missing stays missing."""
    found: dict[str, Any] = {"pass_products": [], "audience": []}
    for snippet in snippets:
        text = _snippet_text(snippet)
        low = text.lower()
        for field, pattern in _YES_PATTERNS.items():
            if field not in found and re.search(pattern, low):
                if not re.search(r"\bno\b.{0,24}(?:" + pattern + ")", low):
                    found[field] = _from_snippet(snippet, "yes")
        for label, pattern in _CHAINS:
            if "operator_group" in found:
                break
            if re.search(pattern, low) and re.search(r"operated by|owned by|part of", low):
                found["operator_group"] = _from_snippet(snippet, label)
        if "operator_group" not in found and re.search(r"independently (owned|operated)", low):
            found["operator_group"] = _from_snippet(snippet, "independent")
        if "owner" not in found:
            owner = re.search(r"(?:owned|operated) by ([A-Z][^.]{2,80})", text)
            if owner:
                found["owner"] = _from_snippet(snippet, re.sub(r"\s+", " ", owner.group(1)).strip(" ."))
        if "operator_status" not in found and re.search(r"not-for-profit|nonprofit|non-profit", low):
            found["operator_status"] = _from_snippet(snippet, "not-for-profit")
        elif "operator_status" not in found and re.search(r"\bski club\b", low):
            found["operator_status"] = _from_snippet(snippet, "club")
        for label, pattern in _PASS_NAMES:
            if any(label.lower() in item["value"].lower() or item["value"].lower() in label.lower() for item in found["pass_products"]):
                continue
            if not re.search(pattern, low):
                continue
            if re.search(r"not on|is not on|isn't on", low):
                continue
            found["pass_products"].append(_from_snippet(snippet, label))
        for field, label in (
            ("lift_ticket", r"day pass|day ticket|lift ticket|adult (?:day )?ticket"),
            ("weekend_ticket", r"weekend (?:day )?(?:pass|ticket|rate)"),
            ("night_ticket", r"night (?:pass|ticket|rate)"),
            ("season_pass", r"season pass|membership (?:fee|cost|price)"),
        ):
            if field in found:
                continue
            price = _price_in(text, label)
            if price:
                found[field] = _from_snippet(snippet, price)
        if "drive" not in found:
            drive = re.search(
                r"\d+\s*(?:minute|min|km|kilometre|kilometer|mile)s?\s+"
                r"(?:from|north of|south of|east of|west of|outside|"
                r"southwest of|northeast of|northwest of|southeast of)\s+"
                r"[A-Za-z][\w'-]+(?:\s+[A-Za-z][\w'-]+){0,2}",
                text,
                flags=re.IGNORECASE,
            )
            if drive:
                found["drive"] = _from_snippet(snippet, re.sub(r"\s+", " ", drive.group(0)).strip())
        if "parking" not in found:
            if re.search(r"free parking", low):
                found["parking"] = _from_snippet(snippet, "free")
            elif re.search(r"paid parking|parking fee|pay to park", low):
                found["parking"] = _from_snippet(snippet, "paid")
            elif re.search(r"parking reservation|reserve(?:d| a)? parking", low):
                found["parking"] = _from_snippet(snippet, "reservation")
        if "visit_type" not in found:
            day = re.search(r"day trip|day-trip", low)
            stay = re.search(r"stay over|overnight", low)
            if day and not stay:
                found["visit_type"] = _from_snippet(snippet, "day trip")
            elif stay and not day:
                found["visit_type"] = _from_snippet(snippet, "stay over")
        for label, pattern in _AUDIENCE:
            if any(item["value"] == label for item in found["audience"]):
                continue
            if re.search(pattern, low):
                found["audience"].append(_from_snippet(snippet, label))
        if "season_open" not in found:
            opened = re.search(
                r"(?:opens|opening|open from|from)\s+(?:in\s+)?(" + "|".join(_MONTHS) + r")",
                low,
            )
            if opened:
                found["season_open"] = _from_snippet(snippet, opened.group(1))
        if "season_close" not in found:
            closed = re.search(
                r"(?:closes|closing|until|through)\s+(?:in\s+)?(" + "|".join(_MONTHS) + r")",
                low,
            )
            if closed and (found.get("season_open") or {}).get("value") != closed.group(1):
                found["season_close"] = _from_snippet(snippet, closed.group(1))
        if "village_type" not in found:
            if re.search(r"ski-in|ski out|ski-out", low):
                found["village_type"] = _from_snippet(snippet, "ski-in ski-out")
            elif re.search(r"purpose-built|purpose built|car-free", low):
                found["village_type"] = _from_snippet(snippet, "purpose-built")
            elif re.search(r"old town", low):
                found["village_type"] = _from_snippet(snippet, "old town")
        if "sectors" not in found:
            sectors = re.search(r"\b(\d+)\s+(?:villages|sectors|areas)\b", low)
            if sectors:
                link = ""
                if re.search(r"gondola", low):
                    link = ", linked by gondola"
                elif re.search(r"ski bus", low):
                    link = ", linked by ski bus"
                found["sectors"] = _from_snippet(snippet, f"{sectors.group(1)}{link}")
        if "terrain" not in found:
            for label, pattern in (
                ("expert", r"\bexperts?\b|off piste|off-piste"),
                ("powder", r"\bpowder\b"),
                ("intermediate", r"\bintermediate\b"),
                ("beginner hill", r"\bbeginners?\b"),
            ):
                if re.search(pattern, low):
                    found["terrain"] = _from_snippet(snippet, label)
                    break
    return found


def assemble_key_points(
    notes: list[dict[str, Any]],
    raw_points: dict[str, Any],
    snippets: list[dict[str, Any]],
    name: str,
) -> dict[str, Any]:
    """Facts OSM does not store. Each value keeps the snippet it came from."""
    points: dict[str, Any] = {field: None for field in _KEY_POINT_FIELDS}
    points["social"] = _social_links(snippets, name)
    points["pass_products"] = []
    points["audience"] = []

    def cited_blob(ids: list[str]) -> str:
        return " ".join(
            _snippet_text(snippet)
            for snippet in snippets
            if str(snippet.get("id")) in ids
        ).lower()

    def take(field: str, item: Any) -> None:
        if points.get(field) or not isinstance(item, dict):
            return
        ids = item.get("snippet_ids")
        if not isinstance(ids, list):
            ids = []
        ids = [str(x).strip() for x in ids if str(x).strip()]
        text = str(item.get("text") or "")
        if field in {"lift_ticket", "weekend_ticket", "night_ticket", "season_pass"} and re.search(
            r"per night|a night|/night", text, re.IGNORECASE
        ):
            return
        if field in _YES_NO_FIELDS:
            yn = _as_yes_no(text)
            if yn is None:
                return
            text = yn
        if field == "website":
            host = _host(text)
            if not host or not _host_matches_resort(host, text, name):
                return
        if field == "operator_group":
            known = {label.lower() for label, _pattern in _CHAINS}
            if text.lower() not in known and text.lower() != "independent":
                return
            if text.lower() == "independent" and not re.search(
                r"independently (owned|operated)", cited_blob(ids), re.IGNORECASE
            ):
                return
        if field == "drive" and not re.search(
            r"\d+\s*(?:minute|min|km|kilometre|kilometer|mile)s?\s+"
            r"(?:from|north of|south of|east of|west of|outside)\b",
            text,
            re.IGNORECASE,
        ):
            return
        if field == "sectors" and not re.search(r"\b\d+\s+(?:villages|sectors|areas)\b", text, re.IGNORECASE):
            return
        if field == "terrain":
            mapped = next(
                (
                    label
                    for label, pattern in (
                        ("expert", r"\bexperts?\b|off piste|off-piste"),
                        ("powder", r"\bpowder\b"),
                        ("intermediate", r"\bintermediate\b"),
                        ("beginner hill", r"\bbeginners?\b"),
                    )
                    if re.search(pattern, text, re.IGNORECASE)
                ),
                None,
            )
            if mapped is None:
                return
            text = mapped
        if field == "visit_type":
            low_visit = text.lower()
            if "day trip" in low_visit and "stay" in low_visit:
                return
            if "day trip" in low_visit or "day-trip" in low_visit:
                text = "day trip"
            elif "stay over" in low_visit or "overnight" in low_visit:
                text = "stay over"
            else:
                return
        value = _point_value(text, ids, snippets)
        if value is None:
            return
        if ids and not value["sources"]:
            return
        blob = cited_blob(ids)
        if field == "pass_affiliation" and blob and not _mentions_resort(blob, name):
            return
        if field in _YES_NO_FIELDS and blob and not re.search(_YES_PATTERNS[field], blob):
            return
        if ids and blob:
            tokens = re.findall(r"[a-z0-9]{4,}|\d+", text.lower())
            if tokens and not any(token in blob for token in tokens):
                return
        points[field] = value

    def take_list(key: str, item: Any) -> None:
        if not isinstance(item, dict):
            return
        ids = [str(x).strip() for x in (item.get("snippet_ids") or []) if str(x).strip()]
        raw = str(item.get("text") or "")
        if key == "audience":
            labels = [label for label, pattern in _AUDIENCE if re.search(pattern, raw, re.IGNORECASE)]
        elif key == "pass_products":
            labels = [raw.strip()] if raw.strip() else []
        else:
            labels = [raw.strip()] if raw.strip() else []
        for label in labels:
            if key == "pass_products" and any(
                label.lower() in existing.get("value", "").lower()
                or existing.get("value", "").lower() in label.lower()
                for existing in points[key]
            ):
                continue
            value = _point_value(label, ids, snippets)
            if value is None:
                continue
            if any(existing.get("value") == value["value"] for existing in points[key]):
                continue
            points[key].append(value)

    for field in _KEY_POINT_FIELDS:
        take(field, raw_points.get(field))
    for item in raw_points.get("pass_products") or []:
        take_list("pass_products", item)
    for item in raw_points.get("audience") or []:
        take_list("audience", item)
    for item in raw_points.get("social") or []:
        if isinstance(item, dict):
            url = str(item.get("text") or "")
            host = _host(url)
            for domain, network in _SOCIAL_HOSTS.items():
                if host == domain or host.endswith("." + domain):
                    if not any(link["url"] == url for link in points["social"]):
                        points["social"].append(
                            {
                                "network": network,
                                "url": url,
                                "snippet_id": ",".join(str(x) for x in item.get("snippet_ids") or []),
                            }
                        )

    for note in notes:
        topic = note.get("topic")
        if topic == "year_started":
            take("year_started", note)
        elif topic == "pass":
            take("pass_affiliation", note)
            take_list("pass_products", note)
        elif topic == "public_or_private":
            take("operator_status", note)
            if re.search(r"owned by|operated by|ski club|not-for-profit|nonprofit", note.get("text") or "", re.I):
                take("owner", note)
        elif topic == "famous_people":
            take("famous_for", note)
        elif topic == "access":
            take("drive", note)
        elif topic == "lodging" and re.search(r"ski-in|ski out|slopeside|on the hill", note.get("text") or "", re.I):
            take("on_hill_lodge", note)
        elif topic in {"price", "other"}:
            text = note.get("text") or ""
            if re.search(r"season pass|membership", text, re.I):
                take("season_pass", note)
            elif re.search(r"weekend", text, re.I):
                take("weekend_ticket", note)
            elif re.search(r"night", text, re.I):
                take("night_ticket", note)
            elif re.search(r"day pass|lift ticket|day ticket", text, re.I):
                take("lift_ticket", note)
        elif topic == "other" and re.search(
            r"owned by|operated by|ski club|not-for-profit|nonprofit", note.get("text") or "", re.I
        ):
            take("owner", note)
        elif topic == "who":
            take_list("audience", note)
    if points["website"] is None:
        points["website"] = _official_website(snippets, name)
    harvested = _harvest_key_points(snippets)
    for field, value in harvested.items():
        if field in {"pass_products", "audience"}:
            for item in value:
                if any(
                    str(item.get("value") or "").lower() in str(existing.get("value") or "").lower()
                    or str(existing.get("value") or "").lower() in str(item.get("value") or "").lower()
                    for existing in points[field]
                ):
                    continue
                points[field].append(item)
        elif points.get(field) is None:
            points[field] = value
    if points["pass_affiliation"] is None and points["pass_products"]:
        points["pass_affiliation"] = points["pass_products"][0]
    return points


def upsert_key_points(record: dict[str, Any], path: Path = KEY_POINTS_PATH) -> None:
    """One row per resort. A later run replaces the earlier row."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            same_id = record.get("winterSportsId") and item.get("winterSportsId") == record.get("winterSportsId")
            same_name = not record.get("winterSportsId") and item.get("resort") == record.get("resort")
            if same_id or same_name:
                continue
            rows.append(item)
    rows.append(record)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _extract_notes(
    snippets: list[dict[str, Any]],
    *,
    resort_label: str,
    name: str,
    state: str | None,
    country: str | None,
    fact_min: int,
    snippet_limit: int,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    trimmed = _select_snippets(snippets, limit=snippet_limit, chars=650, name=name, state=state)
    block = bedrock.format_snippets_for_prompt(trimmed)
    if not block:
        return trimmed, [], {}
    notes, raw_points = search_notes(
        block,
        resort_label=resort_label,
        name=name,
        state=state,
        country=country,
        fact_min=fact_min,
        base_url=base_url,
        model=model,
        num_ctx=num_ctx,
        timeout=timeout,
    )
    notes = _keep_grounded(notes, trimmed, resort_name=name)
    points = assemble_key_points(notes, raw_points, trimmed, name)
    return trimmed, notes, points


def run_passes(
    *,
    resort_label: str,
    snippets: list[dict[str, Any]],
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
    snippet_limit: int,
    name: str,
    state: str | None,
    country: str | None,
    use_ddg: bool,
    search_model: str | None = None,
    writer_model: str | None = None,
    critic_model: str | None = None,
    search_only: bool = False,
) -> dict[str, Any]:
    search_model = search_model or model
    writer_model = writer_model or model
    critic_model = critic_model or model
    print(
        f"models search={search_model} writer={writer_model} critic={critic_model}",
        file=sys.stderr,
        flush=True,
    )
    fact_min = fact_min_for(tier)
    limit = max(snippet_limit, TIER_SNIPPETS.get(_tier_key(tier), 10))
    if not bedrock.format_snippets_for_prompt(snippets):
        raise RuntimeError("No research snippets.")
    trimmed, notes, key_points = _extract_notes(
        snippets,
        resort_label=resort_label,
        name=name,
        state=state,
        country=country,
        fact_min=fact_min,
        snippet_limit=limit,
        base_url=base_url,
        model=search_model,
        num_ctx=num_ctx,
        timeout=timeout,
    )
    gaps = _missing_topics(notes)
    need_color = len(_color_notes(notes)) < fact_min
    if use_ddg and (need_color or gaps):
        add_web_snippets(
            snippets,
            followup_queries(name, state, country, gaps, need_color=need_color, tier=tier),
        )
        trimmed, notes_again, points_again = _extract_notes(
            snippets,
            resort_label=resort_label,
            name=name,
            state=state,
            country=country,
            fact_min=fact_min,
            snippet_limit=limit,
            base_url=base_url,
            model=search_model,
            num_ctx=num_ctx,
            timeout=timeout,
        )
        again_score = (
            len(REQUIRED_TOPICS) - len(_missing_topics(notes_again)),
            len(_color_notes(notes_again)),
        )
        kept_score = (
            len(REQUIRED_TOPICS) - len(gaps),
            len(_color_notes(notes)),
        )
        if again_score >= kept_score:
            notes = notes_again
            key_points = points_again
            gaps = _missing_topics(notes)
    if not notes:
        raise RuntimeError("Search pass returned no grounded notes.")
    if search_only:
        return {
            "resort_label": resort_label,
            "tier": tier,
            "model": search_model,
            "host": base_url,
            "search_fact_min": fact_min,
            "search_fact_count": len(notes),
            "search_color_count": len(_color_notes(notes)),
            "search_gaps": gaps,
            "notes": notes,
            "key_points": key_points,
            "snippets": [
                {"id": s.get("id"), "source": s.get("source"), "title": s.get("title"), "url": s.get("url")}
                for s in trimmed
            ],
        }
    goal = bedrock.copy_goal_for_tier(tier)
    beat_cap = TIER_BEATS.get(_tier_key(tier), 5)
    page_ctx = num_ctx
    if _tier_key(tier) == "large":
        page_ctx = max(num_ctx, 12288)
    elif _tier_key(tier) == "medium":
        page_ctx = max(num_ctx, 8192)
    beats = outline_beats(
        notes,
        resort_label=resort_label,
        beat_cap=beat_cap,
        base_url=base_url,
        model=search_model,
        num_ctx=page_ctx,
        timeout=timeout,
    )
    draft = write_draft(
        notes,
        beats,
        resort_label=resort_label,
        tier=tier,
        base_url=base_url,
        model=writer_model,
        num_ctx=page_ctx,
        timeout=timeout,
    )
    critique = critique_draft(
        draft,
        notes,
        tier=tier,
        base_url=base_url,
        model=critic_model,
        num_ctx=page_ctx,
        timeout=timeout,
    )
    critique["fixes"] = list(critique.get("fixes") or []) + _reader_fixes(draft, notes, name)
    final = edit_draft(
        draft,
        notes,
        critique,
        resort_label=resort_label,
        tier=tier,
        base_url=base_url,
        model=writer_model,
        num_ctx=page_ctx,
        timeout=timeout,
    )
    final = extend_draft(
        final,
        notes,
        resort_label=resort_label,
        tier=tier,
        base_url=base_url,
        model=writer_model,
        num_ctx=page_ctx,
        timeout=timeout,
    )
    measured = bedrock.measure_copy(final, tier)
    return {
        "resort_label": resort_label,
        "tier": tier,
        "layout_goal": {
            "paragraphs": list(goal.paragraphs),
            "lines": list(goal.lines),
            "characters": list(goal.characters),
        },
        "model": model,
        "models": {
            "search": search_model,
            "outline": search_model,
            "writer": writer_model,
            "critic": critic_model,
            "editor": writer_model,
        },
        "host": base_url,
        "search_fact_min": fact_min,
        "search_fact_count": len(notes),
        "search_color_count": len(_color_notes(notes)),
        "search_gaps": gaps,
        "notes": notes,
        "key_points": key_points,
        "outline": beats,
        "draft": draft,
        "critique": critique,
        "final": final,
        "final_word_count": measured["words"],
        "final_char_count": measured["characters"],
        "final_line_count": measured["lines"],
        "final_paragraph_count": measured["paragraphs"],
        "snippets": [
            {"id": s.get("id"), "source": s.get("source"), "title": s.get("title"), "url": s.get("url")}
            for s in trimmed
        ],
    }


def key_point_record(result: dict[str, Any], *, winter_sports_id: str | None, page_id: str | None) -> dict[str, Any]:
    return {
        "winterSportsId": winter_sports_id,
        "pageId": page_id,
        "resort": result.get("resort_label"),
        "tier": result.get("tier"),
        "key_points": result.get("key_points") or {},
    }


def _resort_name(row: dict[str, Any]) -> str | None:
    """Skip bare OpenStreetMap ids that were stored in the name column."""
    for key in ("english_name", "name"):
        text = _cell(row, key)
        if text and not text.isdigit():
            return text
    return None


def _cell(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, float) and value != value:
            continue
        text = str(value).strip()
        if text and text.lower() != "nan":
            return text
    return None


def _round_robin(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    """Spread a sample across countries instead of taking the first rows of one nation."""
    if n <= 0 or not rows:
        return []
    by_country: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_country.setdefault(_cell(row, "country") or "?", []).append(row)
    for group in by_country.values():
        group.sort(key=lambda row: (_resort_name(row) or "").lower())
    countries = sorted(by_country)
    picked: list[dict[str, Any]] = []
    index = 0
    while len(picked) < n and any(by_country.values()):
        group = by_country[countries[index % len(countries)]]
        index += 1
        if group:
            picked.append(group.pop(0))
        if index > n * len(countries) + 5:
            break
    return picked


# 50-resort mix: mostly the short plates, a few longer ones. Not fifty pages.
_TEST_QUOTA: dict[str, int] = {"small": 18, "medium": 16, "large": 10, "mega": 6}


def pick_test_rows(rows: list[dict[str, Any]], n: int = 50) -> list[dict[str, Any]]:
    usable = [
        row
        for row in rows
        if not bedrock._is_not_downhill_row(row) and _resort_name(row)
    ]
    buckets: dict[str, list[dict[str, Any]]] = {tier: [] for tier in _TEST_QUOTA}
    for row in usable:
        tier = bedrock.layout_tier_for_parquet_row(row)
        buckets.setdefault(tier, []).append(row)
    picked: list[dict[str, Any]] = []
    seen: set[str] = set()

    def row_id(row: dict[str, Any]) -> str:
        return _cell(row, "winter_sports_id") or _cell(row, "name") or ""

    scale = n / 50 if n != 50 else 1
    for tier, quota in _TEST_QUOTA.items():
        take = max(1, round(quota * scale)) if n >= 4 else n
        for row in _round_robin(buckets.get(tier, []), take):
            key = row_id(row)
            if not key or key in seen:
                continue
            seen.add(key)
            picked.append(row)
            if len(picked) >= n:
                return picked
    if len(picked) < n:
        rest = [row for row in usable if row_id(row) not in seen]
        picked.extend(_round_robin(rest, n - len(picked)))
    return picked[:n]


def _in_state(row: dict[str, Any], wanted: str) -> bool:
    needle = wanted.strip().lower()
    state, _country = _known_state_country(row)
    if (state or "").lower() == needle:
        return True
    region = (_cell(row, "region") or "").lower().replace("_", "-")
    return needle.replace(" ", "-") in region


def _print_key_points(points: dict[str, Any]) -> None:
    for key, value in points.items():
        if not value:
            continue
        if isinstance(value, list):
            shown = ", ".join(
                str(item.get("value") or item.get("url") or item) for item in value if isinstance(item, dict)
            )
            if shown:
                print(f"    {key}: {shown}", file=sys.stderr)
        elif isinstance(value, dict) and value.get("value"):
            print(f"    {key}: {value['value']}", file=sys.stderr)


def unload_model(base_url: str, model: str) -> None:
    body = json.dumps({"model": model, "keep_alive": 0, "prompt": ""}).encode("utf-8")
    request = urllib.request.Request(
        base_url.rstrip("/") + "/api/generate",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            pass
    except Exception as exc:
        print(f"unload {model}: {exc}", file=sys.stderr)


def _finished_ids(path: Path) -> set[str]:
    done: set[str] = set()
    if not path.is_file():
        return done
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("ok") and item.get("winterSportsId"):
            done.add(str(item["winterSportsId"]))
    return done


def _resolve_inputs(args: argparse.Namespace) -> tuple[str, str | None, str | None, dict[str, Any] | None, str]:
    row: dict[str, Any] | None = None
    if args.input and args.name:
        row = bedrock.load_resort_row(args.input, args.name)
        label = bedrock.row_to_label(row)
        name = str(row.get("english_name") or row.get("name") or args.name)
        state, country = _known_state_country(row)
        tier = bedrock.resolve_layout_tier(args.tier, row)
        return label, state, country, row, tier
    if args.demo_massanutten:
        return "Massanutten, Virginia, United States", "Virginia", "United States", None, args.tier
    if args.demo_query:
        parts = [args.demo_query]
        if args.demo_state:
            parts.append(args.demo_state)
        if args.demo_country:
            parts.append(args.demo_country)
        tier = "small" if args.tier == "auto" else args.tier
        return ", ".join(parts), args.demo_state, args.demo_country, None, tier
    raise SystemExit("Pass --demo-query, --demo-massanutten, or -i with --name.")


def main() -> int:
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=None, help=f"Ollama base URL (default {DEFAULT_HOST} or OLLAMA_HOST)")
    ap.add_argument("--model", default=DEFAULT_MODEL, help=f"Ollama model tag (default {DEFAULT_MODEL})")
    ap.add_argument("--search-model", default=None, help="Tag for search and outline. Defaults to --model.")
    ap.add_argument("--writer-model", default=None, help="Tag for the draft and the edit. Defaults to --model.")
    ap.add_argument("--critic-model", default=None, help="Tag for the fact check. Defaults to --model.")
    ap.add_argument("--num-ctx", type=int, default=DEFAULT_NUM_CTX, help="Context length; 4096 fits an 8B model on 8 GB")
    ap.add_argument("--timeout", type=float, default=180.0, help="Seconds to wait for each Ollama call")
    ap.add_argument("--snippet-limit", type=int, default=8, help="How many research snippets to send the fact agent")
    ap.add_argument("--list-models", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="One short completion, then exit")
    ap.add_argument(
        "--tier",
        default="auto",
        choices=list(TIER_CHOICES) + ["auto"],
        help="Word ceiling. auto follows the parquet size bucket. A demo with no row uses small.",
    )
    ap.add_argument("--batch", type=int, default=None, metavar="N", help="Run N resorts (50 for the overnight test)")
    ap.add_argument("--state", type=str, default=None, help="Only resorts in this state, in parquet order")
    ap.add_argument("--search-only", action="store_true", help="Search and key points only. Skip outline, draft, and edit.")
    ap.add_argument("--dry-batch", action="store_true", help="Print the batch list and exit. Does not call Ollama.")
    ap.add_argument(
        "--out-jsonl",
        type=Path,
        default=None,
        help="Batch output, one JSON object per line (default output/ollama_resort_copy/test50.jsonl)",
    )
    ap.add_argument("--delay-seconds", type=float, default=1.0, help="Pause between resorts in a batch")
    ap.add_argument("-i", "--input", type=Path, default=None)
    ap.add_argument("--name", type=str, default=None)
    ap.add_argument("--demo-massanutten", action="store_true")
    ap.add_argument("--demo-query", type=str, default=None)
    ap.add_argument("--demo-state", type=str, default=None)
    ap.add_argument("--demo-country", type=str, default=None)
    ap.add_argument("--research-only", action="store_true")
    ap.add_argument("--no-ddg", action="store_true")
    ap.add_argument("--brave-api-key", type=str, default=None)
    ap.add_argument("--out-json", type=Path, default=None)
    args = ap.parse_args()

    if args.dry_batch or args.batch or args.state:
        if not args.input:
            raise SystemExit("--batch needs -i/--input parquet")
        count = args.batch if args.batch else 50
        rows = bedrock.load_parquet_resorts_for_batch(args.input, skip_not_downhill=True)
        if args.state:
            chosen = [row for row in rows if _resort_name(row) and _in_state(row, args.state)]
            if args.batch:
                chosen = chosen[: args.batch]
        else:
            chosen = pick_test_rows(rows, count)
        if args.dry_batch:
            for i, row in enumerate(chosen, start=1):
                tier = bedrock.layout_tier_for_parquet_row(row)
                name = _resort_name(row)
                place = ", ".join(p for p in (_cell(row, "state"), _cell(row, "country")) if p)
                print(f"{i:02d}\t{tier}\t{name}\t{place}")
            return 0
        out_path = args.out_jsonl or (
            Path("output/ollama_resort_copy/pennsylvania_search.jsonl")
            if args.search_only and args.state
            else Path("output/ollama_resort_copy/test50.jsonl")
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        done = _finished_ids(out_path)
        base = ollama_base_url(args.host)
        brave_key = (args.brave_api_key or os.environ.get("BRAVE_API_KEY") or "").strip() or None
        ok = 0
        failed = 0
        started = time.time()
        for index, row in enumerate(chosen, start=1):
            wid = _cell(row, "winter_sports_id") or ""
            if wid and wid in done:
                print(f"[{index}/{len(chosen)}] skip {wid}", file=sys.stderr)
                continue
            name = _resort_name(row) or "Unknown"
            state, country = _known_state_country(row)
            label = ", ".join(p for p in (name, state, country) if p)
            tier = bedrock.resolve_layout_tier(args.tier, row)
            t0 = time.time()
            print(f"[{index}/{len(chosen)}] {label} ({tier})", file=sys.stderr, flush=True)
            try:
                snippets = bedrock.gather_research_snippets(
                    name=name,
                    state=state,
                    country=country,
                    brave_api_key=brave_key,
                    use_ddg=not args.no_ddg,
                    parquet_row=row,
                )
                if not args.no_ddg:
                    add_web_snippets(snippets, dossier_queries(name, state, country, tier))
                result = run_passes(
                    resort_label=label,
                    snippets=snippets,
                    tier=tier,
                    base_url=base,
                    model=args.model,
                    num_ctx=args.num_ctx,
                    timeout=args.timeout,
                    snippet_limit=args.snippet_limit,
                    name=name,
                    state=state,
                    country=country,
                    use_ddg=not args.no_ddg,
                    search_model=args.search_model,
                    writer_model=args.writer_model,
                    critic_model=args.critic_model,
                    search_only=args.search_only,
                )
                result["ok"] = True
                result["winterSportsId"] = wid
                result["pageId"] = bedrock.wiki_page_id_from_row(row)
                if not args.search_only:
                    result["contentMarkdown"] = bedrock.wiki_content_markdown_from_row(row, result["final"])
                ok += 1
                print(
                    f"    {result.get('search_color_count', 0)} visit facts "
                    f"plus anchors ({result.get('search_fact_count', 0)} notes)"
                    + (
                        f", {result['final_char_count']} characters"
                        f" / {result['final_line_count']} lines"
                        f" / {result['final_paragraph_count']} paragraphs"
                        if not args.search_only
                        else ""
                    )
                    + f" in {time.time() - t0:.0f}s",
                    file=sys.stderr,
                    flush=True,
                )
                if args.search_only:
                    _print_key_points(result.get("key_points") or {})
            except Exception as exc:
                result = {
                    "ok": False,
                    "winterSportsId": wid,
                    "resort_label": label,
                    "tier": tier,
                    "error": str(exc),
                }
                failed += 1
                print(f"    failed: {exc}", file=sys.stderr, flush=True)
            with out_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(result, ensure_ascii=False) + "\n")
            if result.get("ok") and result.get("key_points"):
                upsert_key_points(
                    key_point_record(
                        result,
                        winter_sports_id=wid or None,
                        page_id=result.get("pageId"),
                    )
                )
            if args.delay_seconds > 0 and index < len(chosen):
                time.sleep(args.delay_seconds)
        print(
            f"done {ok} ok, {failed} failed, {time.time() - started:.0f}s -> {out_path}",
            file=sys.stderr,
        )
        loaded = [args.search_model or args.model, args.writer_model, args.critic_model]
        seen: set[str] = set()
        for tag in loaded:
            if tag and tag not in seen:
                seen.add(tag)
                unload_model(base, tag)
        return 0 if failed == 0 else 1

    base = ollama_base_url(args.host)
    if args.list_models:
        for name in list_models(base):
            print(name)
        return 0
    if args.smoke:
        text = ollama_chat(
            base_url=base,
            model=args.model,
            system_text="Reply with exactly three words.",
            user_text="Say hello.",
            temperature=0.0,
            num_predict=16,
            num_ctx=args.num_ctx,
            timeout=args.timeout,
        )
        print(text)
        return 0

    label, state, country, row, tier = _resolve_inputs(args)
    if tier == "auto":
        raise SystemExit("--tier auto needs -i and --name.")
    brave_key = (args.brave_api_key or os.environ.get("BRAVE_API_KEY") or "").strip() or None
    name = label.split(",")[0].strip()
    snippets = bedrock.gather_research_snippets(
        name=name,
        state=state,
        country=country,
        brave_api_key=brave_key,
        use_ddg=not args.no_ddg,
        parquet_row=row,
    )
    if not args.no_ddg:
        add_web_snippets(snippets, dossier_queries(name, state, country, tier))
    print(f"research: {len(snippets)} snippets for {label}", file=sys.stderr)
    if args.research_only:
        print(bedrock.format_snippets_for_prompt(snippets))
        return 0

    result = run_passes(
        resort_label=label,
        snippets=snippets,
        tier=tier,
        base_url=base,
        model=args.model,
        num_ctx=args.num_ctx,
        timeout=args.timeout,
        snippet_limit=args.snippet_limit,
        name=name,
        state=state,
        country=country,
        use_ddg=not args.no_ddg,
        search_model=args.search_model,
        writer_model=args.writer_model,
        critic_model=args.critic_model,
    )
    if row is not None:
        result["winterSportsId"] = _cell(row, "winter_sports_id")
        result["pageId"] = bedrock.wiki_page_id_from_row(row)
        result["contentMarkdown"] = bedrock.wiki_content_markdown_from_row(row, result["final"])
    if result.get("key_points"):
        upsert_key_points(
            key_point_record(
                result,
                winter_sports_id=result.get("winterSportsId"),
                page_id=result.get("pageId"),
            )
        )
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.out_json:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(text + "\n", encoding="utf-8")
        print(f"wrote {args.out_json}", file=sys.stderr)
    print(result["final"])
    goal_out = result["layout_goal"]
    print(
        f"\n[{result['final_char_count']} characters ({goal_out['characters'][0]}-{goal_out['characters'][1]}), "
        f"{result['final_line_count']} lines ({goal_out['lines'][0]}-{goal_out['lines'][1]}), "
        f"{result['final_paragraph_count']} paragraphs ({goal_out['paragraphs'][0]}-{goal_out['paragraphs'][1]}), "
        f"{args.model}]",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
