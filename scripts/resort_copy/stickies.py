"""Sticky-question search: answer from the page pile, fetch more only on a miss.

Each sticky is a reader question. The model writes a longer grounded note when the
pile has the answer. A miss triggers a web search; new pages join the pile for later stickies.
"""
from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable

from .config import (
    PAGE_FACT_WINDOW_CHARS,
    SEARCH_SYSTEM_TOKENS,
    _tier_key,
)
from .notes import split_page_windows
from .ollama_client import _chat_json
from .queries import topic_queries, add_web_snippets
from .snippets import _select_snippets
from .web_fetch import fetch_resort_pages


CheckpointCallback = Callable[[str, dict[str, Any]], None]


@dataclass(frozen=True)
class Sticky:
    id: str
    topic: str
    ask: str
    search_topics: tuple[str, ...]


# Order matters: early stickies grow the pile for later ones.
_STICKIES: tuple[Sticky, ...] = (
    Sticky(
        "setting",
        "setting",
        "What mountain range, peaks, or landscape setting is {name} in? "
        "If the page only names a range, add a vivid grounded detail from the same page.",
        ("setting",),
    ),
    Sticky(
        "year_started",
        "year_started",
        "When did {name} open or first operate as a ski area? Include the year and how it started if the page says.",
        ("year_started", "history"),
    ),
    Sticky(
        "public_or_private",
        "public_or_private",
        "Is {name} public, private, a club, municipal, or not-for-profit? Only if a page says so.",
        ("public_or_private",),
    ),
    Sticky(
        "ownership",
        "other",
        "Who owns or operates {name}? Name the company or operator only if a page says so.",
        ("history",),
    ),
    Sticky(
        "pass",
        "pass",
        "Which season pass or multi-resort pass includes {name}? "
        "Name a pass only when a page says THIS resort is on it.",
        ("pass", "passes"),
    ),
    Sticky(
        "access",
        "access",
        "How do people get to {name}? Road, nearest named town, drive time, or airport if stated.",
        ("access",),
    ),
    Sticky(
        "price",
        "price",
        "What is a day lift ticket or adult ticket price for {name}, with currency, if stated?",
        ("price", "prices"),
    ),
    Sticky(
        "ski_school",
        "other",
        "Does {name} have a ski school or lessons? Where on the mountain, and who they are for, if stated.",
        ("school", "ski_instructors"),
    ),
    Sticky(
        "rentals",
        "other",
        "Does {name} offer ski or snowboard rentals? Where, if stated.",
        ("rentals",),
    ),
    Sticky(
        "lodging",
        "lodging",
        "What lodging is at or near {name}? Hotels, village, ski-in ski-out, pools, or nightly rates if stated.",
        ("lodging",),
    ),
    Sticky(
        "who",
        "who",
        "Who is {name} best for — beginners, families, intermediates, experts — according to the pages?",
        ("who", "terrain"),
    ),
    Sticky(
        "scene",
        "scene",
        "What is the on-mountain or village scene at {name}? Terrain character, parks, nightlife, or a signature run.",
        ("scene", "terrain"),
    ),
    Sticky(
        "season",
        "other",
        "When does {name} usually open and close, or what is notable about its season or snowmaking?",
        ("season",),
    ),
    Sticky(
        "famous_people",
        "famous_people",
        "Which famous skier, race, or notable person is tied to {name} if a page says?",
        ("people",),
    ),
    Sticky(
        "night_tubing",
        "scene",
        "Does {name} offer night skiing or snow tubing? Only if a page says yes or no clearly.",
        ("night", "snowtubing"),
    ),
    Sticky(
        "food",
        "scene",
        "Where do people eat at {name}? A named lodge, village restaurants, or mountain lunch if stated.",
        ("food", "scene"),
    ),
    Sticky(
        "review",
        "review",
        "What do visitors or guides praise or warn about at {name}, if a page says?",
        ("scene",),
    ),
    Sticky(
        "history_color",
        "other",
        "One grounded history or name-origin story about {name} that is not only the opening year.",
        ("history",),
    ),
    Sticky(
        "linked_area",
        "other",
        "Is {name} linked to another ski area or peak with shared tickets? Name it only if stated.",
        ("terrain",),
    ),
    Sticky(
        "signature",
        "scene",
        "A signature trail, peak, event, or tradition at {name} that would make someone want to visit.",
        ("scene", "people"),
    ),
)


_TIER_STICKY_COUNT = {"small": 10, "medium": 16, "large": 22}


def stickies_for_tier(tier: str) -> list[Sticky]:
    return list(_STICKIES[: _TIER_STICKY_COUNT.get(_tier_key(tier), 16)])


def _page_text(snippet: dict[str, Any]) -> str:
    return str(snippet.get("body") or snippet.get("text") or "").strip()


def _score_page(snippet: dict[str, Any], ask: str) -> int:
    blob = f"{snippet.get('title') or ''} {_page_text(snippet)}".lower()
    score = 0
    if snippet.get("fetched") or snippet.get("body"):
        score += 5
    src = str(snippet.get("source") or "")
    if src == "wikipedia":
        score += 4
    if src == "stay22":
        score += 2
    for word in re.findall(r"[a-z]{4,}", ask.lower()):
        if word in blob:
            score += 1
    return score


def _pile_windows(
    snippets: list[dict[str, Any]],
    *,
    ask: str,
    max_chars: int,
) -> tuple[str, list[str]]:
    """Best windows from the indexed pile for one sticky, sized for the 8B context."""
    ranked = sorted(snippets, key=lambda s: _score_page(s, ask), reverse=True)
    chunks: list[tuple[str, str]] = []
    for snippet in ranked:
        text = _page_text(snippet)
        if len(text) < 120:
            continue
        sid = str(snippet.get("id") or "")
        for window in split_page_windows(text, size=PAGE_FACT_WINDOW_CHARS, limit=4):
            chunks.append((sid, window))
        if len(chunks) >= 16:
            break
    picked: list[str] = []
    used_ids: list[str] = []
    used = 0
    for sid, window in chunks:
        block = f"[{sid}]\n{window}"
        if used and used + 2 + len(block) > max_chars:
            continue
        picked.append(block)
        if sid and sid not in used_ids:
            used_ids.append(sid)
        used += len(block) + 2
        if used >= max_chars:
            break
    return "\n\n".join(picked), used_ids


def _answer_sticky(
    sticky: Sticky,
    *,
    pile_block: str,
    resort_label: str,
    name: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
) -> dict[str, Any] | None:
    system = (
        f"You answer one sticky question for a ski atlas entry about {name}. "
        "Use only INDEXED_PAGES. Output one JSON object and nothing else: "
        '{"found":true,"text":"two to four sentences","snippet_ids":["W1"]}. '
        "When found is true, text is a longer sticky note: first answer the question, "
        "then keep a vivid grounded detail from the same pages (range age, where lessons meet, "
        "a named lodge, a price with currency, ski-in ski-out, a pool — only if stated). "
        "Copy names, years, distances, and prices exactly. "
        "Do not invent. Do not recite vertical, lift counts, trail counts, or acreage. "
        "If INDEXED_PAGES do not answer the question, return {\"found\":false,\"text\":\"\",\"snippet_ids\":[]}."
    )
    user = (
        f"Resort: {resort_label}\n"
        f"STICKY ({sticky.id}): {sticky.ask.format(name=name)}\n\n"
        f"INDEXED_PAGES:\n{pile_block}\n"
    )
    try:
        payload = _chat_json(
            base_url=base_url,
            model=model,
            system_text=system,
            user_text=user,
            temperature=0.1,
            num_predict=450,
            num_ctx=num_ctx,
            timeout=timeout,
            json_format=True,
        )
    except (RuntimeError, json.JSONDecodeError, TimeoutError) as exc:
        print(f"    sticky {sticky.id} failed: {exc}", file=sys.stderr, flush=True)
        return None
    if not payload.get("found"):
        return None
    text = str(payload.get("text") or "").strip()
    if len(text) < 40:
        return None
    ids = payload.get("snippet_ids")
    if not isinstance(ids, list):
        ids = []
    return {
        "topic": sticky.topic,
        "text": text,
        "snippet_ids": [str(x).strip() for x in ids if str(x).strip()],
        "sticky": sticky.id,
    }


def _grow_pile_for_sticky(
    sticky: Sticky,
    snippets: list[dict[str, Any]],
    *,
    name: str,
    state: str | None,
    country: str | None,
    tier: str,
    page_limit: int,
) -> int:
    """Search and fetch more pages when a sticky misses. Returns newly fetched count."""
    before = len(snippets)
    before_fetched = sum(1 for s in snippets if s.get("fetched"))
    queries = topic_queries(name, state, country, list(sticky.search_topics), tier=tier)
    add_web_snippets(snippets, queries)
    added_urls = len(snippets) - before
    fetched = fetch_resort_pages(snippets, name=name, state=state, limit=page_limit)
    print(
        f"    sticky {sticky.id}: searched, +{added_urls} urls, fetched {fetched} "
        f"(pile pages {before_fetched} -> {sum(1 for s in snippets if s.get('fetched'))})",
        file=sys.stderr,
        flush=True,
    )
    return fetched


def run_sticky_search(
    snippets: list[dict[str, Any]],
    *,
    resort_label: str,
    name: str,
    state: str | None,
    country: str | None,
    tier: str,
    base_url: str,
    model: str,
    num_ctx: int,
    timeout: float,
    use_ddg: bool,
    page_limit: int,
    char_target: int,
    checkpoint: CheckpointCallback | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], int]:
    """Walk stickies against a growing page pile. Returns trimmed, notes, key_points, dropped."""
    stickies = stickies_for_tier(tier)
    # Seed the pile: read the best pages we already have URLs for.
    fetch_resort_pages(snippets, name=name, state=state, limit=page_limit)
    notes: list[dict[str, Any]] = []
    reply_tokens = 500
    budget = int((num_ctx - reply_tokens - SEARCH_SYSTEM_TOKENS) * 3.2)
    budget = max(3500, min(budget, 12000))

    for index, sticky in enumerate(stickies, start=1):
        ask = sticky.ask.format(name=name)
        pile_block, _ids = _pile_windows(snippets, ask=ask, max_chars=budget)
        print(
            f"  sticky {index}/{len(stickies)} {sticky.id}: "
            f"{sum(1 for s in snippets if s.get('fetched'))} indexed pages, "
            f"{len(pile_block)} chars in prompt",
            file=sys.stderr,
            flush=True,
        )
        note = None
        if pile_block:
            started = time.time()
            note = _answer_sticky(
                sticky,
                pile_block=pile_block,
                resort_label=resort_label,
                name=name,
                base_url=base_url,
                model=model,
                num_ctx=num_ctx,
                timeout=timeout,
            )
            print(
                f"    sticky {sticky.id}: "
                f"{'hit' if note else 'miss'} in {round(time.time() - started)}s",
                file=sys.stderr,
                flush=True,
            )
        if note is None and use_ddg:
            _grow_pile_for_sticky(
                sticky,
                snippets,
                name=name,
                state=state,
                country=country,
                tier=tier,
                page_limit=page_limit,
            )
            pile_block, _ids = _pile_windows(snippets, ask=ask, max_chars=budget)
            if pile_block:
                started = time.time()
                note = _answer_sticky(
                    sticky,
                    pile_block=pile_block,
                    resort_label=resort_label,
                    name=name,
                    base_url=base_url,
                    model=model,
                    num_ctx=num_ctx,
                    timeout=timeout,
                )
                print(
                    f"    sticky {sticky.id} after fetch: "
                    f"{'hit' if note else 'miss'} in {round(time.time() - started)}s",
                    file=sys.stderr,
                    flush=True,
                )
        if note is not None:
            notes.append(note)
        if checkpoint is not None:
            checkpoint(
                f"sticky_{sticky.id}",
                {
                    "notes": notes,
                    "progress": {
                        "sticky_id": sticky.id,
                        "sticky_index": index,
                        "indexed_pages": sum(1 for s in snippets if s.get("fetched")),
                    },
                },
            )
        if sum(len(n["text"]) for n in notes) >= char_target:
            print(
                f"  sticky search reached note character target "
                f"({sum(len(n['text']) for n in notes)}/{char_target})",
                file=sys.stderr,
                flush=True,
            )
            break

    trimmed, dropped = _select_snippets(
        snippets, limit=max(8, page_limit), chars=650, name=name, state=state, country=country
    )
    return trimmed, notes, {}, dropped
