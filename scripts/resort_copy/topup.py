"""Top-up pass: when the sticky notes miss the fact target, mine the fetched pages
for more facts and search the gap topics before giving up on the resort.

The sticky pass yields at most one note per question (10 / 16 / 20 by tier), so on
its own it can never reach the 22-note write floor for medium and large plates. This
pass reads the pages already in the pile in fact windows, then searches thin topics
for new pages, until the pile reaches the tier's fact minimum or the call budget runs out.
"""
from __future__ import annotations

import sys
import time
from typing import Any, Callable

from .config import (
    PAGE_FACT_WINDOW_CHARS,
    REQUIRED_TOPICS,
    _tier_key,
    search_retries_for,
    write_char_floor_for,
    write_floor_for,
)
from .notes import (
    _content_words,
    _near_duplicate,
    read_page_facts,
    split_page_windows,
)
from .validators import note_char_target, note_chars, sanitize_notes
from .queries import add_web_snippets, topic_queries
from .snippets import _drop_pages_about_other_places, _keep_grounded
from .web_fetch import fetch_resort_pages

CheckpointCallback = Callable[[str, dict[str, Any]], None]


# Pages read per top-up round, windows read per page.
# Always 24 windows (plus extra) until the pantry target. A 12-window prefer-stop
# used to fire as soon as the write floor was met — Killington stopped at 7458/16200.
TOPUP_PAGES_PER_ROUND = 4
TOPUP_WINDOWS_PER_PAGE = 3
TOPUP_MAX_WINDOWS = 24
TOPUP_MAX_WINDOWS_IF_SHORT = 24
# After large stickies + top-up, keep mining this many more windows if the plate is still short.
LARGE_EXTRA_WINDOWS = 24
# Large extra pass also stops once this many new notes land.
LARGE_EXTRA_NOTES = 20

_SKIP_MINE_HOSTS = ("stay22.com", "booking.com", "tripadvisor.", "expedia.")
_FACT_HOSTS = (
    "ikonpass.com",
    "epicpass.com",
    "indyskipass.com",
    "mountaincollective.com",
    "nord.pass",
)
_FACT_URL_BITS = (
    "lift-ticket",
    "lift_ticket",
    "lift-tickets",
    "day-pass",
    "/tickets",
    "/rates",
    "season-pass",
    "ikon-pass",
    "epic-pass",
)


def dedupe_notes(notes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop exact and near-duplicate note texts. Anchor topics only lose exact repeats.

    Several stickies can lift the same sentence from the same page. Three notes that all
    say "a ski resort in the northeastern United States" are one fact, not three.
    """
    seen: set[str] = set()
    seen_words: list[set[str]] = []
    unique: list[dict[str, Any]] = []
    for note in notes:
        text = str(note.get("text") or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        anchor = note.get("topic") in REQUIRED_TOPICS
        if not anchor and _near_duplicate(text, seen_words):
            continue
        seen.add(key)
        seen_words.append(_content_words(text))
        unique.append(note)
    return unique


def _page_rank(page: dict[str, Any]) -> tuple[int, int]:
    url = str(page.get("url") or "").lower()
    src = str(page.get("source") or "")
    if any(host in url for host in _FACT_HOSTS) or any(bit in url for bit in _FACT_URL_BITS):
        bonus = 4
    elif src == "wikipedia":
        bonus = 2
    elif page.get("fetched"):
        bonus = 1
    else:
        bonus = 0
    return (bonus, len(str(page.get("body") or "")))


def _worth_mining(page: dict[str, Any]) -> bool:
    url = str(page.get("url") or "").lower()
    return not any(host in url for host in _SKIP_MINE_HOSTS)


def _unmined_pages(snippets: list[dict[str, Any]], mined: set[str]) -> list[dict[str, Any]]:
    pages = [
        s
        for s in snippets
        if str(s.get("id") or "") not in mined
        and _worth_mining(s)
        and len(str(s.get("body") or s.get("text") or "")) >= 200
    ]
    return sorted(pages, key=_page_rank, reverse=True)


def _trim_page(page: dict[str, Any]) -> dict[str, Any]:
    """Cap the body so one page costs at most TOPUP_WINDOWS_PER_PAGE fact calls."""
    body = str(page.get("body") or page.get("text") or "")
    windows = split_page_windows(body, size=PAGE_FACT_WINDOW_CHARS, limit=TOPUP_WINDOWS_PER_PAGE)
    return {**page, "body": "\n".join(windows)}


def top_up_notes(
    notes: list[dict[str, Any]],
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
    page_limit: int,
    use_ddg: bool,
    gap_topics: Callable[[list[dict[str, Any]]], list[str]],
    checkpoint: CheckpointCallback | None = None,
    mined: set[str] | None = None,
    extra_windows: int = 0,
    extra_note_goal: int = 0,
) -> tuple[list[dict[str, Any]], int, list[dict[str, str]], int, set[str]]:
    """Grow the note pile until both the note-count floor and the plate characters are met.

    Stops when note text reaches 2× the plate *and* the note-count floor is met, or
    at 24 (+ extra) windows. Empty searches stop. Pass/price URLs are mined before
    Wikipedia so the facts table is not starved by wiki windows.

    Returns (notes, search_attempts, validation_errors, windows_read, mined).
    """
    note_floor = write_floor_for(tier)
    char_floor = write_char_floor_for(tier)
    char_target = note_char_target(tier)
    rounds = search_retries_for(tier) + 1
    mined = set(mined or ())
    attempts = 0
    windows_read = 0
    errors: list[dict[str, str]] = []
    notes = dedupe_notes(notes)
    start_count = len(notes)

    def pantry() -> int:
        return note_chars(notes)

    def enough_to_write() -> bool:
        return len(notes) >= note_floor and pantry() >= char_floor

    def window_budget() -> int:
        return TOPUP_MAX_WINDOWS_IF_SHORT + extra_windows

    def done() -> bool:
        if pantry() >= char_target and len(notes) >= note_floor:
            return True
        if extra_note_goal and (len(notes) - start_count) >= extra_note_goal and enough_to_write():
            return True
        return windows_read >= window_budget()

    if pantry() >= char_target and len(notes) >= note_floor:
        return notes, attempts, errors, windows_read, mined
    print(
        f"top-up: {len(notes)} notes, {pantry()} chars; need {note_floor} notes and "
        f"{char_floor} chars to write, {char_target} chars to stop ({_tier_key(tier)})",
        file=sys.stderr,
        flush=True,
    )
    for round_index in range(rounds):
        added = 0
        fetched = 0
        if done():
            break
        fact_gaps = [g for g in gap_topics(notes) if g in ("pass", "passes", "price", "prices")]
        if round_index == 0 and fact_gaps:
            queries = topic_queries(name, state, country, fact_gaps, tier=tier)
            before = len(snippets)
            add_web_snippets(snippets, queries, use_ddg=use_ddg)
            fetched = fetch_resort_pages(snippets, name=name, state=state, limit=page_limit)
            added = len(snippets) - before
            attempts += 1
            print(
                f"  top-up search {attempts}: gaps={fact_gaps[:6]} +{added} urls, "
                f"fetched {fetched}",
                file=sys.stderr,
                flush=True,
            )
        elif round_index > 0:
            gaps = gap_topics(notes)
            queries = topic_queries(name, state, country, gaps, tier=tier)
            before = len(snippets)
            add_web_snippets(snippets, queries, use_ddg=use_ddg)
            fetched = fetch_resort_pages(snippets, name=name, state=state, limit=page_limit)
            added = len(snippets) - before
            attempts += 1
            print(
                f"  top-up search {attempts}: gaps={gaps[:6]} +{added} urls, "
                f"fetched {fetched}",
                file=sys.stderr,
                flush=True,
            )
        # Only read pages that are about this resort; a page about another hill in
        # another country adds facts that the grounding step would reject anyway.
        about_resort, _ = _drop_pages_about_other_places(snippets, name, state, country=country)
        candidates = _unmined_pages(about_resort, mined)[:TOPUP_PAGES_PER_ROUND]
        if not candidates:
            print("  top-up: no unread pages left in the pile", file=sys.stderr, flush=True)
            if round_index > 0 and added == 0 and fetched == 0:
                break
            continue
        for page in candidates:
            if done():
                break
            page_id = str(page.get("id") or "")
            mined.add(page_id)
            trimmed = _trim_page(page)
            windows_read += len(split_page_windows(str(trimmed["body"]), limit=TOPUP_WINDOWS_PER_PAGE))
            started = time.time()
            facts = read_page_facts(
                trimmed,
                resort_label=resort_label,
                name=name,
                base_url=base_url,
                model=model,
                num_ctx=num_ctx,
                timeout=timeout,
                already=notes,
            )
            tagged = [
                {"topic": fact.get("topic") or "other", "text": fact["text"], "snippet_ids": [page_id]}
                for fact in facts
                if page_id
            ]
            grounded = _keep_grounded(tagged, about_resort, resort_name=name)
            clean, page_errors = sanitize_notes(grounded, name=name, snippets=about_resort)
            errors.extend(page_errors)
            before_count = len(notes)
            notes = dedupe_notes([*notes, *clean])
            print(
                f"  top-up page {page_id} ({str(page.get('url') or '')[:70]}): "
                f"{len(facts)} facts, +{len(notes) - before_count} new notes "
                f"({len(notes)} total) in {round(time.time() - started)}s",
                file=sys.stderr,
                flush=True,
            )
            if checkpoint is not None:
                checkpoint(
                    "top_up",
                    {
                        "notes": notes,
                        "progress": {
                            "top_up_page": page_id,
                            "top_up_windows": windows_read,
                            "top_up_searches": attempts,
                        },
                    },
                )
    print(
        f"top-up done: {len(notes)} notes, {note_chars(notes)} chars after "
        f"{windows_read} page windows and {attempts} searches",
        file=sys.stderr,
        flush=True,
    )
    return notes, attempts, errors, windows_read, mined
