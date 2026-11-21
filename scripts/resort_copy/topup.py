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
    fact_min_for,
    search_retries_for,
    write_floor_for,
)
from .notes import (
    _content_words,
    _near_duplicate,
    read_page_facts,
    split_page_windows,
)
from .queries import add_web_snippets, topic_queries
from .snippets import _drop_pages_about_other_places, _keep_grounded
from .validators import sanitize_notes
from .web_fetch import fetch_resort_pages

CheckpointCallback = Callable[[str, dict[str, Any]], None]


# Pages read per top-up round, windows read per page, and the hard cap on fact calls per
# resort. On CPU each window is one model call of a few minutes, so 12 windows is the
# most a single resort may spend here on top of its sticky pass.
TOPUP_PAGES_PER_ROUND = 4
TOPUP_WINDOWS_PER_PAGE = 3
TOPUP_MAX_WINDOWS = 12


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
    src = str(page.get("source") or "")
    bonus = 2 if src == "wikipedia" else 1 if page.get("fetched") else 0
    return (bonus, len(str(page.get("body") or "")))


def _unmined_pages(snippets: list[dict[str, Any]], mined: set[str]) -> list[dict[str, Any]]:
    pages = [
        s
        for s in snippets
        if str(s.get("id") or "") not in mined and len(str(s.get("body") or s.get("text") or "")) >= 200
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
) -> tuple[list[dict[str, Any]], int, list[dict[str, str]], int]:
    """Grow the note pile toward the tier fact minimum.

    Round 0 mines pages already fetched. Each later round searches the gap topics,
    fetches new pages, and mines those. Stops at the fact minimum, or once the window
    budget is spent and the pile at least clears the write floor.

    Returns (notes, search_attempts, validation_errors, windows_read).
    """
    fact_min = fact_min_for(tier)
    floor = write_floor_for(tier)
    rounds = search_retries_for(tier) + 1
    mined: set[str] = set()
    attempts = 0
    windows_read = 0
    errors: list[dict[str, str]] = []
    notes = dedupe_notes(notes)

    def done() -> bool:
        if len(notes) >= fact_min:
            return True
        return windows_read >= TOPUP_MAX_WINDOWS and len(notes) >= floor

    if len(notes) >= fact_min:
        return notes, attempts, errors, windows_read
    print(
        f"top-up: {len(notes)} notes, need {floor} to write and {fact_min} to stop "
        f"({_tier_key(tier)})",
        file=sys.stderr,
        flush=True,
    )
    for round_index in range(rounds):
        if done():
            break
        if round_index > 0:
            gaps = gap_topics(notes)
            queries = topic_queries(name, state, country, gaps, tier=tier)
            before = len(snippets)
            add_web_snippets(snippets, queries, use_ddg=use_ddg)
            fetched = fetch_resort_pages(snippets, name=name, state=state, limit=page_limit)
            attempts += 1
            print(
                f"  top-up search {attempts}: gaps={gaps[:6]} +{len(snippets) - before} urls, "
                f"fetched {fetched}",
                file=sys.stderr,
                flush=True,
            )
        # Only read pages that are about this resort; a page about another hill in
        # another country adds facts that the grounding step would reject anyway.
        about_resort, _ = _drop_pages_about_other_places(snippets, name, state, country=country)
        candidates = _unmined_pages(about_resort, mined)[:TOPUP_PAGES_PER_ROUND]
        if not candidates:
            # Nothing new to read; the next round searches for more pages.
            print("  top-up: no unread pages left in the pile", file=sys.stderr, flush=True)
            continue
        for page in candidates:
            if done() or windows_read >= TOPUP_MAX_WINDOWS:
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
        f"top-up done: {len(notes)} notes after {windows_read} page windows and {attempts} searches",
        file=sys.stderr,
        flush=True,
    )
    return notes, attempts, errors, windows_read
