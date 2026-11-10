"""Run the five passes for one resort."""
from __future__ import annotations

import sys
import time
from typing import Any, Callable

from . import bedrock

from .config import (
    PAGE_KEY_POINT_CHARS,
    REQUIRED_TOPICS,
    SEARCH_SYSTEM_TOKENS,
    TIER_BEATS,
    TIER_NOTE_CAP,
    TIER_PAGES,
    TIER_SEARCH_CTX,
    TIER_SNIPPETS,
    _tier_key,
    fact_min_for,
)
from .key_points import (
    assemble_key_points,
)
from .notes import (
    _color_notes,
    _missing_topics,
    read_page_facts,
    search_notes,
)
from .queries import (
    add_web_snippets,
    followup_queries,
)
from .snippets import (
    _keep_grounded,
    _select_snippets,
    _trim_snippet,
)
from .web_fetch import (
    fetch_resort_pages,
)
from .writing import (
    _reader_fixes,
    critique_draft,
    edit_draft,
    extend_draft,
    outline_beats,
    write_draft,
)

CheckpointCallback = Callable[[str, dict[str, Any]], None]


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
    page_cache: dict[str, list[dict[str, Any]]] | None = None,
    note_cap: int | None = None,
    checkpoint: CheckpointCallback | None = None,
    search_phase: str = "initial",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    trimmed = _select_snippets(snippets, limit=snippet_limit, chars=650, name=name, state=state)
    page_cache = {} if page_cache is None else page_cache
    page_facts: list[dict[str, Any]] = []
    for page in trimmed:
        if not page.get("fetched"):
            continue
        url = str(page.get("url") or "")
        if url not in page_cache:
            t0 = time.time()
            page_cache[url] = read_page_facts(
                page,
                resort_label=resort_label,
                name=name,
                base_url=base_url,
                model=model,
                num_ctx=num_ctx,
                timeout=timeout,
            )
            print(
                f"    read {len(page_cache[url])} facts from {url} in {round(time.time() - t0)}s",
                file=sys.stderr,
                flush=True,
            )
        page_facts.extend({**fact, "snippet_ids": [str(page.get("id"))]} for fact in page_cache[url])
        if checkpoint is not None:
            checkpoint(
                f"{search_phase}_page_facts",
                {
                    "progress": {
                        f"{search_phase}_page_facts": page_facts,
                        "last_page_url": url,
                    }
                },
            )
    # The key-point call only needs the top of each page. Its facts were read above.
    shown = [
        _trim_snippet(s, PAGE_KEY_POINT_CHARS) if s.get("fetched") else s
        for s in trimmed
    ]
    # About 3.2 characters per token for web prose with urls. Drop the weakest sources to fit.
    reply_tokens = max(1800, fact_min * 160)
    budget_chars = int((num_ctx - reply_tokens - SEARCH_SYSTEM_TOKENS) * 3.2)
    block = bedrock.format_snippets_for_prompt(shown)
    while len(block) > budget_chars and len(shown) > 4:
        shown.pop()
        block = bedrock.format_snippets_for_prompt(shown)
    if not block:
        return trimmed, [], {}
    print(
        f"search: {len(shown)} sources ({sum(1 for s in trimmed if s.get('fetched'))} read in full, "
        f"{len(page_facts)} page facts), {len(block)} characters, context {num_ctx}",
        file=sys.stderr,
        flush=True,
    )
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
        page_facts=page_facts,
        cap=note_cap,
        on_part=(
            lambda label, payload: checkpoint(
                f"{search_phase}_{label.replace(' ', '_')}",
                {"progress": {f"{search_phase}_search_payload": payload}},
            )
            if checkpoint is not None
            else None
        ),
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
    checkpoint: CheckpointCallback | None = None,
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
    search_ctx = max(num_ctx, TIER_SEARCH_CTX.get(_tier_key(tier), 8192))
    page_limit = TIER_PAGES.get(_tier_key(tier), 4)
    read = fetch_resort_pages(snippets, name=name, state=state, limit=page_limit)
    print(f"pages: read {read} of up to {page_limit}", file=sys.stderr, flush=True)
    if checkpoint is not None:
        checkpoint(
            "pages_fetched",
            {
                "progress": {
                    "research_snippets": [
                        {
                            "id": s.get("id"),
                            "source": s.get("source"),
                            "title": s.get("title"),
                            "url": s.get("url"),
                            "read_in_full": bool(s.get("fetched")),
                            "text": s.get("text"),
                        }
                        for s in snippets
                    ]
                }
            },
        )
    page_cache: dict[str, list[dict[str, Any]]] = {}
    note_cap = TIER_NOTE_CAP.get(_tier_key(tier), fact_min)
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
        num_ctx=search_ctx,
        timeout=timeout,
        page_cache=page_cache,
        note_cap=note_cap,
        checkpoint=checkpoint,
    )
    gaps = _missing_topics(notes)
    need_color = len(_color_notes(notes)) < fact_min
    if use_ddg and (need_color or gaps):
        extra_queries = followup_queries(
            name, state, country, gaps, need_color=need_color, tier=tier
        )
        print(
            f"follow-up search: {len(extra_queries)} queries",
            file=sys.stderr,
            flush=True,
        )
        add_web_snippets(snippets, extra_queries)
        if checkpoint is not None:
            checkpoint(
                "followup_research_complete",
                {
                    "progress": {
                        "followup_research_snippets": [
                            {
                                "id": s.get("id"),
                                "source": s.get("source"),
                                "title": s.get("title"),
                                "url": s.get("url"),
                                "read_in_full": bool(s.get("fetched")),
                                "text": s.get("text"),
                            }
                            for s in snippets
                        ]
                    }
                },
            )
        print("follow-up search complete; extracting notes again", file=sys.stderr, flush=True)
        read += fetch_resort_pages(snippets, name=name, state=state, limit=page_limit)
        trimmed_again, notes_again, points_again = _extract_notes(
            snippets,
            resort_label=resort_label,
            name=name,
            state=state,
            country=country,
            fact_min=fact_min,
            snippet_limit=limit,
            base_url=base_url,
            model=search_model,
            num_ctx=search_ctx,
            timeout=timeout,
            page_cache=page_cache,
            note_cap=note_cap,
            checkpoint=checkpoint,
            search_phase="followup",
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
            trimmed = trimmed_again
            notes = notes_again
            key_points = points_again
            gaps = _missing_topics(notes)
    if checkpoint is not None:
        checkpoint(
            "search_complete",
            {
                "search_fact_min": fact_min,
                "search_fact_count": len(notes),
                "search_color_count": len(_color_notes(notes)),
                "search_gaps": gaps,
                "notes": notes,
                "key_points": key_points,
            },
        )
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
                {
                    "id": s.get("id"),
                    "source": s.get("source"),
                    "title": s.get("title"),
                    "url": s.get("url"),
                    "read_in_full": bool(s.get("fetched")),
                    "text": s.get("text"),
                }
                for s in trimmed
            ],
        }
    print("outline: starting", file=sys.stderr, flush=True)
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
    if checkpoint is not None:
        checkpoint("outline_complete", {"outline": beats})
    print("write: starting", file=sys.stderr, flush=True)
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
    if checkpoint is not None:
        checkpoint("draft_complete", {"draft": draft})
    print("critique: starting", file=sys.stderr, flush=True)
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
    if checkpoint is not None:
        checkpoint("critique_complete", {"critique": critique})
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
    if checkpoint is not None:
        checkpoint("edit_complete", {"final": final})
    print("edit/extend: starting", file=sys.stderr, flush=True)
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
    if checkpoint is not None:
        checkpoint("extend_complete", {"final": final})
    measured = bedrock.measure_copy(final, tier)
    result = {
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
            {
                    "id": s.get("id"),
                    "source": s.get("source"),
                    "title": s.get("title"),
                    "url": s.get("url"),
                    "read_in_full": bool(s.get("fetched")),
                }
            for s in trimmed
        ],
    }
    if checkpoint is not None:
        checkpoint("complete", result)
    return result


def key_point_record(result: dict[str, Any], *, winter_sports_id: str | None, page_id: str | None) -> dict[str, Any]:
    return {
        "winterSportsId": winter_sports_id,
        "pageId": page_id,
        "resort": result.get("resort_label"),
        "tier": result.get("tier"),
        "key_points": result.get("key_points") or {},
    }
