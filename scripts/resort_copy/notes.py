"""Search pass: turn snippets and pages into cited visit-fact notes."""
from __future__ import annotations

import json
import re
import sys
import time
from typing import Any, Callable

from .config import (
    COLOR_TOPICS,
    PAGE_FACTS_PER_PAGE,
    REQUIRED_TOPICS,
)
from .ollama_client import (
    _chat_json,
)
from .snippets import (
    _DIRECTORY_PAGE,
    _READER_NOTE,
    _STAT_RECAP,
    _bullets_from_payload,
)


def _salvage_bullets(raw: str) -> list[dict[str, Any]]:
    """Keep every complete bullet from a reply that was cut off mid-JSON."""
    found: list[dict[str, Any]] = []
    pattern = re.compile(
        r'\{\s*"topic"\s*:\s*"([^"]+)"\s*,\s*"text"\s*:\s*"((?:\\.|[^"\\])*)"\s*,\s*"snippet_ids"\s*:\s*(\[[^\]]*\])\s*\}'
    )
    for match in pattern.finditer(raw or ""):
        try:
            text = json.loads('"' + match.group(2) + '"')
            ids = json.loads(match.group(3))
        except json.JSONDecodeError:
            continue
        if isinstance(text, str) and text.strip():
            found.append({"topic": match.group(1), "text": text.strip(), "snippet_ids": ids if isinstance(ids, list) else []})
    return found
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


def _content_words(text: str) -> set[str]:
    return {w for w in re.findall(r"[a-z0-9]+", text.lower()) if len(w) >= 4 or w.isdigit()}


def _near_duplicate(text: str, kept: list[set[str]]) -> bool:
    """The same fact copied by three websites. Most of the words already appear in one kept note."""
    words = _content_words(text)
    if not words:
        return False
    return any(len(words & other) >= 0.7 * min(len(words), len(other)) for other in kept if other)


def _notes_from_payload(
    payload: dict[str, Any],
    fact_min: int,
    *,
    name: str,
    state: str | None,
    country: str | None,
    page_facts: list[dict[str, Any]] | None = None,
    cap: int | None = None,
) -> list[dict[str, Any]]:
    notes: list[dict[str, Any]] = []
    for topic in REQUIRED_TOPICS:
        item = payload.get(topic)
        if isinstance(item, dict):
            note = _note_from_item(topic, item)
            if note is not None:
                notes.append(note)
    allowed = set(COLOR_TOPICS)
    # Facts read from a whole page are more specific than the bullets from the blurb call.
    for bullet in [*(page_facts or []), *_bullets_from_payload(payload)]:
        topic = str(bullet.get("topic") or "other").strip().lower()
        bullet["topic"] = topic if topic in allowed else "other"
        notes.append(bullet)
    seen: set[str] = set()
    seen_words: list[set[str]] = []
    unique: list[dict[str, Any]] = []
    for note in notes:
        key = note["text"].lower()
        if key in seen or key.startswith("not found"):
            continue
        if note.get("topic") not in REQUIRED_TOPICS and _near_duplicate(note["text"], seen_words):
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
        seen_words.append(_content_words(note["text"]))
        unique.append(note)
    tagged = _tag_required_topics(unique)
    anchors = [n for n in tagged if n.get("topic") in REQUIRED_TOPICS]
    color = [n for n in tagged if n.get("topic") not in REQUIRED_TOPICS]
    reader = [n for n in color if n.get("topic") in {"access", "price", "who"} or _READER_NOTE.search(n["text"])]
    rest = [n for n in color if n not in reader]
    return anchors + reader + rest[: max(0, max(cap or 0, fact_min) - len(reader))]


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
    page_facts: list[dict[str, Any]] | None = None,
    cap: int | None = None,
    on_part: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Search pass: four anchors, then cited reasons a visitor would care."""
    known = ", ".join(part for part in (name, state, country) if part)
    intro = (
        "You are the search pass for a ski atlas. SNIPPETS were already fetched. "
        "Use only SNIPPETS. Output one JSON object and nothing else. Schema: "
    )
    anchors_schema = (
        '{"year_started":{"text":"when it opened or was founded","snippet_ids":["D1"]},'
        '"pass":{"text":"the named pass product that includes THIS resort","snippet_ids":["D2"]},'
        '"public_or_private":{"text":"private, public, municipal, or not-for-profit club","snippet_ids":["D3"]},'
        '"setting":{"text":"what it overlooks or the landform, not the town name","snippet_ids":["W2"]}}. '
    )
    points_schema = (
        '{"key_points":{"website":{"text":"official website url","snippet_ids":["D1"]},'
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
        '"lifts":{"text":"number of lifts or yes/no if lift info present","snippet_ids":["D23"]},'
        '"snow_tubing":{"text":"yes or no","snippet_ids":["D24"]},'
        '"village_type":{"text":"none, ski-in ski-out, purpose-built, or old town","snippet_ids":["D25"]},'
        '"sectors":{"text":"village or sector count, and gondola or ski bus if they are linked","snippet_ids":["D26"]},'
        '"terrain":{"text":"beginner hill, intermediate, powder, park, or expert","snippet_ids":["D27"]},'
        '"famous_for":{"text":"skier or race name and year","snippet_ids":["D28"]},'
        '"on_hill_lodge":{"text":"named lodge on the hill","snippet_ids":["D29"]},'
        '"social":[{"text":"https://instagram.com/example","snippet_ids":["D30"]}]}}. '
    )
    bullets_schema = (
        '{"bullets":[{"topic":"famous_people","text":"one concrete fact, 25 words or fewer","snippet_ids":["W1"]}]}. '
    )
    bullet_rules = (
        f"The map already knows the place ({known}). Do not write a bullet that only says where it is. "
        f"Add at least {fact_min} bullets that would make someone want to visit or read on: "
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
    )
    shared_rules = (
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
    # Three short replies instead of one long one: each call stays well inside the timeout.
    user_text = f"Resort: {resort_label}\n\nSNIPPETS:\n{snippets_block}\n"
    # An 8B model times out on 28 key-point fields at once, so ask for a few at a time.
    inner = points_schema[len('{"key_points":{') : points_schema.rindex("}}")]
    fields = re.split(r'(?<=[}\]]),(?="[a-z_]+":[\[{])', inner)
    point_calls = tuple(
        (
            f"key points {i // 5 + 1}",
            '{"key_points":{' + ",".join(fields[i : i + 5]) + "}}. ",
            shared_rules,
            450,
        )
        for i in range(0, len(fields), 5)
    )
    calls = (
        ("anchors", anchors_schema, "Fill the four named fields from SNIPPETS. " + shared_rules, 500),
        *point_calls,
        ("bullets", bullets_schema, bullet_rules + shared_rules, max(900, fact_min * 120)),
    )
    payload: dict[str, Any] = {}
    for label, schema, rules, budget in calls:
        started = time.time()
        try:
            part = _chat_json(
                base_url=base_url,
                model=model,
                system_text=intro + schema + rules,
                user_text=user_text,
                temperature=0.1,
                num_predict=budget,
                num_ctx=num_ctx,
                timeout=timeout,
            )
        except (RuntimeError, json.JSONDecodeError) as exc:
            raw = getattr(exc, "raw", "")
            salvaged = _salvage_bullets(raw) if label == "bullets" else []
            if salvaged:
                payload["bullets"] = salvaged
                print(f"  search {label}: kept {len(salvaged)} bullets from a cut-off reply", file=sys.stderr, flush=True)
            else:
                print(f"  search {label} failed: {exc}", file=sys.stderr, flush=True)
            continue
        print(f"  search {label}: {time.time() - started:.0f}s", file=sys.stderr, flush=True)
        merged = {**payload.get("key_points", {}), **(part.get("key_points") or {})} if "key_points" in part else None
        payload.update(part)
        if merged is not None:
            payload["key_points"] = merged
        if on_part is not None:
            on_part(label, dict(payload))
    notes = _notes_from_payload(
        payload,
        fact_min,
        name=name,
        state=state,
        country=country,
        page_facts=page_facts,
        cap=cap,
    )
    return notes, payload.get("key_points") if isinstance(payload.get("key_points"), dict) else {}


_NOT_STATED = re.compile(
    r"\bnot (?:stated|specified|listed|given|provided|available|found)\b|\bno (?:information|mention)\b|"
    r"\bmentioned\b|\bnothing useful\b|\bthe page\b|\bthis page\b",
    re.IGNORECASE,
)


_PAGE_FACT_TOPICS = {
    "history": "other",
    "people": "famous_people",
    "access": "access",
    "lodging": "lodging",
    "food": "scene",
    "scene": "scene",
    "events": "scene",
    "review": "review",
    "price": "price",
    "who": "who",
    "other": "other",
}


def read_page_facts(
    page: dict[str, Any],
    *,
    resort_label: str,
    name: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> list[dict[str, Any]]:
    """One page, one short call. A small model finds the specific facts a long prompt buries.

    num_ctx should match the search call: Ollama reloads the model whenever it changes.
    """
    text = str(page.get("text") or "").strip()
    if len(text) < 200:
        return []
    system = (
        f"You read one web page for a ski atlas entry about {name}. Output one JSON object and nothing else: "
        '{"facts":[{"topic":"history","text":"one sentence, 30 words or fewer"}]}. '
        f"List up to {PAGE_FACTS_PER_PAGE} facts about {name} that would make someone want to read on or visit. "
        "topic is history, people, events, access, lodging, food, scene, review, price, who, or other. "
        "One fact per item: never list several years or events in one item. "
        "Leave a topic out when PAGE does not cover it. Never write that something is not mentioned. "
        "Good facts: when and how it started, a first or a record, who owns it and why, a famous skier or team, "
        "a tradition or event, the road it is on and how far to a named town, where to eat or stay, "
        "what the skiing feels like and who it suits, and a day ticket price. "
        "Copy names, years, distances, and prices exactly from PAGE. "
        "Skip vertical, lift counts, trail counts, acreage, snowfall, and opening hours. "
        "Skip anything about a different ski area, and skip facts only about the town. "
        'If PAGE says nothing useful about this ski area, return {"facts":[]}.'
    )
    try:
        payload = _chat_json(
            base_url=base_url,
            model=model,
            system_text=system,
            user_text=f"Ski area: {resort_label}\nPAGE ({page.get('title') or page.get('url')}):\n{text}\n",
            temperature=0.1,
            num_predict=700,
            num_ctx=num_ctx,
            timeout=timeout,
            json_format=True,
        )
    except (RuntimeError, json.JSONDecodeError) as exc:
        print(f"    page facts failed for {page.get('url')}: {exc}", file=sys.stderr, flush=True)
        return []
    facts: list[dict[str, Any]] = []
    for item in payload.get("facts") or []:
        if not isinstance(item, dict):
            continue
        fact = str(item.get("text") or "").strip()
        if len(fact) < 20 or _STAT_RECAP.search(fact) or _NOT_STATED.search(fact):
            continue
        topic = _PAGE_FACT_TOPICS.get(str(item.get("topic") or "other").strip().lower(), "other")
        facts.append({"topic": topic, "text": fact})
    return facts[:PAGE_FACTS_PER_PAGE]
