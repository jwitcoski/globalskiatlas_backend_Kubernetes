"""Run the five passes for one resort."""
from __future__ import annotations

import sys
import time
from typing import Any, Callable

from . import bedrock

from .config import (
    TIER_BEATS,
    TIER_PAGES,
    TIER_SEARCH_CTX,
    TIER_SNIPPETS,
    _tier_key,
    fact_min_for,
    write_floor_for,
)
from .filters import (
    filter_row,
)
from .notes import (
    _color_notes,
    _missing_topics,
)
from .stickies import (
    run_sticky_search,
)
from .snippets import (
    _keep_grounded,
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
    print("search: sticky-question pass with growing page pile", file=sys.stderr, flush=True)
    started = time.time()
    trimmed, notes, key_points, dropped = run_sticky_search(
        snippets,
        resort_label=resort_label,
        name=name,
        state=state,
        country=country,
        tier=tier,
        base_url=base_url,
        model=search_model,
        num_ctx=search_ctx,
        timeout=timeout,
        use_ddg=use_ddg,
        page_limit=page_limit,
        char_target=char_target,
        checkpoint=checkpoint,
    )
    stage_timings["sticky_search"] = round(time.time() - started, 2)
    snippets_dropped += dropped
    notes = _keep_grounded(notes, trimmed, resort_name=name)
    notes, sticky_errors = sanitize_notes(notes, name=name, snippets=trimmed)
    validation_errors.extend(sticky_errors)
    gaps = _gap_topics(notes)
    attempts = 0
    print(
        f"    sticky_search: {stage_timings['sticky_search']}s, {len(notes)} notes, "
        f"{note_chars(notes)} note characters",
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
    write_floor = write_floor_for(tier)
    if len(notes) < write_floor:
        return {
            "resort_label": resort_label,
            "tier": _tier_key(tier),
            "status": "insufficient_data",
            "error": (
                f"{len(notes)} valid facts is below the {write_floor} needed to write a {_tier_key(tier)} page "
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
        # Twice the print size is about 16,000 characters of notes. 8192 tokens clips that pile.
        page_ctx = max(num_ctx, 12288)
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
