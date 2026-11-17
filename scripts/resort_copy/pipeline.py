"""Run the five passes for one resort."""
from __future__ import annotations

import sys
import time
from typing import Any, Callable

from . import bedrock

from .config import (
    PAGE_KEY_POINT_CHARS,
    SEARCH_SYSTEM_TOKENS,
    TIER_BEATS,
    TIER_NOTE_CAP,
    TIER_PAGES,
    TIER_SEARCH_CTX,
    TIER_SNIPPETS,
    _tier_key,
    fact_min_for,
    search_retries_for,
)
from .filters import (
    filter_row,
    missing_amenity_topics,
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
    topic_queries,
)
from .snippets import (
    _keep_grounded,
    _select_snippets,
    _trim_snippet,
)
from .web_fetch import (
    fetch_resort_pages,
)
from .validators import (
    is_placeholder_name,
    sanitize_notes,
    section_for_note,
    short_reason_for,
    unused_notes,
    note_char_target,
    note_chars,
)
from .writing import (
    _reader_fixes,
    critique_for_draft,
    edit_draft,
    finish_prose,
    length_control_loop,
    outline_beats,
    produce_draft,
    write_draft,
)

CheckpointCallback = Callable[[str, dict[str, Any]], None]

# Extra searches while the notes are still shorter than the upper print size.
_CHAR_FILL: tuple[tuple[str, ...], ...] = (
    ("price", "season", "school"),
    ("history", "access", "lodging"),
    ("food", "night", "rentals"),
    ("snowtubing", "ski_instructors", "people"),
    ("pass", "who", "scene"),
    ("year_started", "public_or_private", "terrain"),
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
    page_cache: dict[str, list[dict[str, Any]]] | None = None,
    note_cap: int | None = None,
    checkpoint: CheckpointCallback | None = None,
    search_phase: str = "initial",
    validation_errors: list[dict[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], int]:
    trimmed, dropped = _select_snippets(
        snippets, limit=snippet_limit, chars=650, name=name, state=state, country=country
    )
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
        return trimmed, [], {}, dropped
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
    notes, errors = sanitize_notes(notes, name=name, snippets=trimmed)
    if validation_errors is not None:
        validation_errors.extend(errors)
    points = assemble_key_points(notes, raw_points, trimmed, name)
    return trimmed, notes, points, dropped


def _gap_topics(notes: list[dict[str, Any]]) -> list[str]:
    gaps = list(_missing_topics(notes))
    have = {section_for_note(note) for note in notes}
    for section in (
        "history",
        "access",
        "terrain",
        "prices",
        "passes",
        "lodging",
        "scene",
        "who",
        "season",
        "school",
    ):
        if section not in have:
            gaps.append(section)
    return gaps


def _research_view(snippets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
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
    use_llm_critic = bool(critic_model) and critic_model not in {"deterministic", "none"}
    critic_label = critic_model if use_llm_critic else "deterministic"
    print(
        f"models search={search_model} writer={writer_model} critic={critic_label}",
        file=sys.stderr,
        flush=True,
    )
    placeholder, why = is_placeholder_name(name)
    if placeholder:
        return {
            "resort_label": resort_label,
            "tier": _tier_key(tier),
            "status": "skipped",
            "error": why,
            "ok": False,
            "notes": [],
            "validation_errors": [{"topic": "name", "reason": why, "text": name}],
            "short_reason": None,
            "unused_notes": [],
        }
    fact_min = fact_min_for(tier)
    char_target = note_char_target(tier)
    stage_timings: dict[str, float] = {}
    validation_errors: list[dict[str, str]] = []
    snippets_dropped = 0
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
    note_cap = max(TIER_NOTE_CAP.get(_tier_key(tier), fact_min), char_target // 60)

    def _pull(phase: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], int]:
        started = time.time()
        pulled = _extract_notes(
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
            search_phase=phase,
            validation_errors=validation_errors,
        )
        stage_timings[phase] = round(time.time() - started, 2)
        print(
            f"    {phase}: {stage_timings[phase]}s, {len(pulled[1])} notes, {note_chars(pulled[1])} note characters",
            file=sys.stderr,
            flush=True,
        )
        return pulled

    trimmed, notes, key_points, dropped = _pull("initial")
    snippets_dropped += dropped
    gaps = _gap_topics(notes)
    amenity_gaps = missing_amenity_topics(notes, trimmed, key_points)
    attempts = 0
    while use_ddg and attempts < search_retries_for(tier) and (
        len(notes) < fact_min or amenity_gaps or note_chars(notes) < char_target
    ):
        attempts += 1
        before_chars = note_chars(notes)
        asked = list(dict.fromkeys([*gaps, *amenity_gaps]))
        if note_chars(notes) < char_target:
            asked.extend(_CHAR_FILL[attempts % len(_CHAR_FILL)])
        extra_queries = topic_queries(name, state, country, asked, tier=tier)
        print(
            f"topic search {attempts}: {len(extra_queries)} queries, "
            f"{before_chars}/{char_target} note characters",
            file=sys.stderr,
            flush=True,
        )
        before = len(snippets)
        add_web_snippets(snippets, extra_queries)
        if len(snippets) == before:
            print("topic search added no new snippets", file=sys.stderr, flush=True)
            break
        if checkpoint is not None:
            checkpoint("followup_research_complete", {"progress": {"followup_research_snippets": _research_view(snippets)}})
        read += fetch_resort_pages(snippets, name=name, state=state, limit=page_limit)
        trimmed_again, notes_again, points_again, dropped_again = _pull(f"retry_{attempts}")
        snippets_dropped += dropped_again
        grew = note_chars(notes_again) > before_chars
        if note_chars(notes_again) >= before_chars:
            trimmed, notes, key_points = trimmed_again, notes_again, points_again
        gaps = _gap_topics(notes)
        amenity_gaps = missing_amenity_topics(notes, trimmed, key_points)
        if not grew:
            print(
                f"topic search kept {note_chars(notes)} note characters",
                file=sys.stderr,
                flush=True,
            )
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
                "validation_errors": validation_errors,
                "snippets_dropped": snippets_dropped,
            },
        )
    filters = filter_row(
        resort=resort_label,
        tier=_tier_key(tier),
        country=country,
        state=state,
        notes=notes,
        snippets=trimmed,
        key_points=key_points,
    )
    if len(notes) < fact_min:
        return {
            "resort_label": resort_label,
            "tier": _tier_key(tier),
            "status": "insufficient_data",
            "error": (
                f"{len(notes)} valid facts is below the {fact_min} fact minimum for {_tier_key(tier)} "
                f"after {attempts} extra searches"
            ),
            "ok": False,
            "layout_goal": {
                "paragraphs": list(bedrock.copy_goal_for_tier(tier).paragraphs),
                "lines": list(bedrock.copy_goal_for_tier(tier).lines),
                "characters": list(bedrock.copy_goal_for_tier(tier).characters),
            },
            "models": {
                "search": search_model,
                "outline": search_model,
                "writer": writer_model,
                "critic": critic_label,
                "editor": writer_model,
            },
            "host": base_url,
            "search_fact_min": fact_min,
            "search_fact_count": len(notes),
            "search_color_count": len(_color_notes(notes)),
            "search_gaps": gaps,
            "search_attempts": attempts,
            "notes": notes,
            "key_points": key_points,
            "validation_errors": validation_errors,
            "snippets_dropped": snippets_dropped,
            "stage_timings": stage_timings,
            "short_reason": None,
            "unused_notes": [],
            "filters": filters,
            "snippets": _research_view(trimmed),
        }
    if search_only:
        return {
            "resort_label": resort_label,
            "tier": tier,
            "status": "complete",
            "ok": True,
            "model": search_model,
            "host": base_url,
            "search_fact_min": fact_min,
            "search_fact_count": len(notes),
            "search_color_count": len(_color_notes(notes)),
            "search_gaps": gaps,
            "search_attempts": attempts,
            "validation_errors": validation_errors,
            "snippets_dropped": snippets_dropped,
            "stage_timings": stage_timings,
            "short_reason": None,
            "unused_notes": [],
            "filters": filters,
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
        page_ctx = max(num_ctx, 8192)
    elif _tier_key(tier) == "medium":
        page_ctx = max(num_ctx, 8192)
    started = time.time()
    beats = outline_beats(
        notes,
        resort_label=resort_label,
        beat_cap=beat_cap,
        base_url=base_url,
        model=search_model,
        num_ctx=page_ctx,
        timeout=timeout,
    )
    stage_timings["outline"] = round(time.time() - started, 2)
    if checkpoint is not None:
        checkpoint("outline_complete", {"outline": beats})
    print("write: starting", file=sys.stderr, flush=True)
    started = time.time()
    draft = produce_draft(
        notes,
        beats,
        resort_label=resort_label,
        tier=tier,
        base_url=base_url,
        model=writer_model,
        num_ctx=page_ctx,
        timeout=timeout,
        name=name,
    )
    if not draft.strip():
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
    draft = finish_prose(draft, notes, name=name, country=country, state=state, tier=tier)
    stage_timings["draft"] = round(time.time() - started, 2)
    if checkpoint is not None:
        checkpoint("draft_complete", {"draft": draft})
    print("critique: starting", file=sys.stderr, flush=True)
    started = time.time()
    critique = critique_for_draft(
        draft,
        notes,
        tier=tier,
        name=name,
        country=country,
        state=state,
        base_url=base_url,
        model=critic_model,
        num_ctx=page_ctx,
        timeout=timeout,
        use_llm=use_llm_critic,
    )
    critique["fixes"] = list(critique.get("fixes") or []) + _reader_fixes(draft, notes, name)
    stage_timings["critique"] = round(time.time() - started, 2)
    if checkpoint is not None:
        checkpoint("critique_complete", {"critique": critique})
    started = time.time()
    if critique.get("fixes"):
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
    else:
        final = draft
    final = finish_prose(final, notes, name=name, country=country, state=state, tier=tier)
    print("length: starting", file=sys.stderr, flush=True)
    final, short_reason, leftover = length_control_loop(
        final,
        notes,
        resort_label=resort_label,
        tier=tier,
        base_url=base_url,
        model=writer_model,
        num_ctx=page_ctx,
        timeout=timeout,
    )
    final = finish_prose(final, notes, name=name, country=country, state=state, tier=tier)
    if short_reason_for(final, notes, tier) is None:
        short_reason = None
    else:
        short_reason = short_reason or short_reason_for(final, notes, tier)
    leftover = unused_notes(final, notes)
    stage_timings["edit"] = round(time.time() - started, 2)
    if checkpoint is not None:
        checkpoint("extend_complete", {"final": final, "short_reason": short_reason})
    measured = bedrock.measure_copy(final, tier)
    result = {
        "resort_label": resort_label,
        "tier": _tier_key(tier),
        "status": "complete",
        "ok": True,
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
            "critic": critic_label,
            "editor": writer_model,
        },
        "host": base_url,
        "search_fact_min": fact_min,
        "search_fact_count": len(notes),
        "search_color_count": len(_color_notes(notes)),
        "search_gaps": gaps,
        "search_attempts": attempts,
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
        "short_reason": short_reason,
        "unused_notes": [item.get("text") for item in leftover],
        "validation_errors": validation_errors,
        "snippets_dropped": snippets_dropped,
        "stage_timings": stage_timings,
        "filters": filters,
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
        "filters": result.get("filters") or {},
    }
