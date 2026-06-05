"""Scribus layout for regional facts page (page 01 after overview)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from atlas.book_gen.constants import RESORT_CATEGORY_LABEL
from atlas.book_gen.wiki_style import (
    C_BODY,
    C_STATS_LABEL,
    C_SUBTITLE,
    C_TITLE,
    TextRun,
    type_scale_for_slot,
)

if TYPE_CHECKING:
    from atlas.book_gen.regional_facts import RegionalFacts


def runs_region_facts_header(facts: RegionalFacts) -> list[TextRun]:
    """Compact title + summary for chart-heavy facts page."""
    scale = type_scale_for_slot("full")
    runs: list[TextRun] = []

    title = f"{facts.region_title} Ski Facts"
    runs.append(TextRun(title + "\n", scale.title * 1.12, C_TITLE))
    subtitle = (
        f"{facts.resort_count:,} downhill resorts  ·  "
        f"{facts.total_trails:,} trails  ·  "
        f"{facts.total_lifts:,} lifts  ·  "
        f"{facts.total_acres:,.0f} skiable acres"
    )
    runs.append(TextRun(subtitle, scale.subtitle * 0.95, C_SUBTITLE))
    return runs


def runs_region_facts_records(facts: RegionalFacts) -> list[TextRun]:
    """Key superlatives in two compact columns (plain text, newline-separated)."""
    scale = type_scale_for_slot("full")
    runs: list[TextRun] = []
    runs.append(TextRun("RECORDS\n", scale.stats_label * 1.05, C_STATS_LABEL))

    lines: list[str] = []
    for rec in facts.resort_records[:4]:
        lines.append(f"{rec.label}: {rec.name} ({rec.detail})")
    for rec in facts.trail_lift_records[:2]:
        lines.append(f"{rec.label}: {rec.name} ({rec.detail})")

    if lines:
        body = "  ·  ".join(lines)
        runs.append(TextRun(body, scale.body * 0.88, C_BODY))
    return runs


def runs_region_facts_corner(facts: RegionalFacts) -> list[TextRun]:
    """Resort & trail/lift superlatives not covered by the facts charts."""
    scale = type_scale_for_slot("full")
    runs: list[TextRun] = []
    lbl = scale.stats_label * 0.88
    body = scale.body * 0.78

    records = list(facts.resort_records) + list(facts.trail_lift_records)
    if not records:
        return runs

    runs.append(TextRun("RECORDS\n", lbl, C_STATS_LABEL))
    for rec in records:
        runs.append(TextRun(f"{rec.label}\n", lbl, C_STATS_LABEL))
        runs.append(TextRun(f"{rec.name} — {rec.detail}\n", body, C_BODY))

    return runs


def _has_corner_records(facts: RegionalFacts) -> bool:
    return bool(facts.resort_records or facts.trail_lift_records)


def runs_region_facts(facts: RegionalFacts) -> list[TextRun]:
    """Full text layout (used when no charts are available)."""
    scale = type_scale_for_slot("full")
    runs: list[TextRun] = []

    def add(text: str, size: float, color: str = C_BODY) -> None:
        if text:
            runs.append(TextRun(text, size, color))

    title = f"{facts.region_title} Ski Facts"
    add(title + "\n", scale.title * 1.15, C_TITLE)
    subtitle = (
        f"Records and statistics from {facts.resort_count:,} downhill ski "
        f"resort{'s' if facts.resort_count != 1 else ''} in {facts.region_title}"
    )
    if facts.book_resort_count is not None and facts.book_resort_count != facts.resort_count:
        subtitle += f" ({facts.book_resort_count:,} featured in this chapter)"
    add(subtitle + "\n\n", scale.subtitle, C_SUBTITLE)

    add("AT A GLANCE\n", scale.stats_label * 1.1, C_STATS_LABEL)
    acres_s = f"{facts.total_acres:,.0f} skiable acres"
    add(
        f"{facts.resort_count:,} resorts  ·  {facts.total_trails:,} trails  ·  "
        f"{facts.total_lifts:,} lifts  ·  {acres_s}\n\n",
        scale.body,
    )

    if facts.tier_counts:
        add("RESORTS BY SIZE\n", scale.stats_label * 1.1, C_STATS_LABEL)
        order = (
            "small_hill",
            "ski_mountain",
            "multiple_mountains",
            "mega_resort",
        )
        parts: list[str] = []
        for cat in order:
            n = facts.tier_counts.get(cat, 0)
            if n:
                label = RESORT_CATEGORY_LABEL.get(cat, cat)
                parts.append(f"{label}: {n}")
        add(" · ".join(parts) + "\n\n", scale.body)

    if facts.resort_records:
        add("RESORT RECORDS\n", scale.stats_label * 1.1, C_STATS_LABEL)
        for rec in facts.resort_records:
            add(f"{rec.label}\n", scale.stats_label, C_STATS_LABEL)
            add(f"{rec.name} — {rec.detail}\n\n", scale.body)

    if facts.trail_lift_records or facts.lift_type_summary:
        add("TRAILS & LIFTS\n", scale.stats_label * 1.1, C_STATS_LABEL)
        for rec in facts.trail_lift_records:
            add(f"{rec.label}\n", scale.stats_label, C_STATS_LABEL)
            add(f"{rec.name} — {rec.detail}\n\n", scale.body)
        if facts.lift_type_summary:
            add("Lift types (all resorts)\n", scale.stats_label, C_STATS_LABEL)
            add(facts.lift_type_summary + "\n", scale.body)

    return runs


def _append_bottom_chart_grid(
    items: list[tuple[Any, ...]],
    *,
    x0: float,
    bottom_y: float,
    content_w: float,
    bottom_h: float,
    gap: float,
    charts: dict[str, str],
    facts: RegionalFacts | None = None,
) -> None:
    """Bottom half: scatter top-left; lifts + vertical trail difficulty bottom-left; records right."""
    slots: list[tuple[str | None, int, int]] = [
        (charts.get("scatter"), 0, 0),
        (charts.get("trail_difficulty"), 1, 0),
        (charts.get("lift_types"), 0, 1),
    ]
    present = [(path, col, row) for path, col, row in slots if path]
    n = len(present)
    if not n:
        return

    col_w = (content_w - gap) / 2.0

    if n == 1:
        path, _, _ = present[0]
        items.append(("image", x0, bottom_y, content_w, bottom_h, path))
        return

    if n == 2:
        for i, (path, _, _) in enumerate(present):
            x = x0 + i * (col_w + gap)
            items.append(("image", x, bottom_y, col_w, bottom_h, path))
        return

    if n == 3:
        row_h = (bottom_h - gap) / 2.0
        col_w = (content_w - gap) / 2.0
        top_y = bottom_y
        bot_y = bottom_y + row_h + gap
        grid_bottom = bot_y + row_h
        left_x = x0
        right_x = x0 + col_w + gap
        scatter = charts.get("scatter")
        diff = charts.get("trail_difficulty")
        lifts = charts.get("lift_types")
        has_records = facts is not None and _has_corner_records(facts)

        if scatter:
            items.append(("image", left_x, top_y, col_w, row_h, scatter))

        if lifts and diff:
            sub_gap = gap
            lifts_w = (col_w - sub_gap) * 0.58
            diff_w = col_w - sub_gap - lifts_w
            items.append(("image", left_x, bot_y, lifts_w, row_h, lifts))
            items.append(
                (
                    "image",
                    left_x + lifts_w + sub_gap,
                    bot_y,
                    diff_w,
                    row_h,
                    diff,
                )
            )
        elif lifts:
            items.append(("image", left_x, bot_y, col_w, row_h, lifts))
        elif diff:
            items.append(("image", left_x, bot_y, col_w, row_h, diff))

        if has_records:
            records_h = grid_bottom - top_y
            scale = type_scale_for_slot("full")
            items.append(
                (
                    "shape",
                    right_x,
                    top_y,
                    col_w,
                    records_h,
                    "AtlasStatsBg",
                )
            )
            items.append(
                (
                    "text",
                    right_x + 8.0,
                    top_y + 6.0,
                    col_w - 16.0,
                    records_h - 12.0,
                    runs_region_facts_corner(facts),
                    scale.linesp * 0.88,
                )
            )
        elif diff:
            items.append(("image", right_x, top_y, col_w, grid_bottom - top_y, diff))
        elif lifts:
            items.append(
                (
                    "image",
                    x0,
                    bot_y,
                    content_w,
                    row_h,
                    lifts,
                )
            )
        return

    row_h = (bottom_h - gap) / 2.0
    for path, col, row in present:
        x = x0 + col * (col_w + gap)
        y = bottom_y + row * (row_h + gap)
        items.append(("image", x, y, col_w, row_h, path))


def layout_region_facts_page(
    facts: RegionalFacts,
    *,
    page_x: float,
    page_y: float,
    content_x: float,
    content_y: float,
    content_w: float,
    content_h: float,
    elevation_only: bool = False,
) -> list[tuple[Any, ...]]:
    """
    Return layout items: ("text", x, y, w, h, runs, linesp) | ("image", x, y, w, h, path).
    Coordinates are absolute canvas positions (page_x + offsets).
    """
    scale = type_scale_for_slot("full")
    charts = facts.chart_paths or {}
    has_charts = bool(charts)

    x0 = page_x + content_x
    y0 = page_y + content_y
    gap = 8.0
    items: list[tuple[Any, ...]] = []

    if has_charts:
        header_h = 40.0
        use_split = bool(charts.get("elevation_range")) and not elevation_only
        records_h = 0.0 if use_split else 44.0
        chart_area_h = content_h - header_h - records_h - gap
        if records_h:
            chart_area_h -= gap

        items.append(
            (
                "text",
                x0,
                y0,
                content_w,
                header_h,
                runs_region_facts_header(facts),
                scale.linesp,
            )
        )

        if charts.get("elevation_range"):
            chart_y_elev = y0 + header_h + gap
            if elevation_only:
                elev_h = chart_area_h
                items.append(
                    (
                        "image",
                        x0,
                        chart_y_elev,
                        content_w,
                        elev_h,
                        charts["elevation_range"],
                    )
                )
            else:
                # Top half: elevation; bottom half: 2×2 chart grid.
                elev_h = (chart_area_h - gap) * 0.5
                items.append(
                    (
                        "image",
                        x0,
                        chart_y_elev,
                        content_w,
                        elev_h,
                        charts["elevation_range"],
                    )
                )

                bottom_y = chart_y_elev + elev_h + gap
                bottom_h = chart_area_h - elev_h - gap
                _append_bottom_chart_grid(
                    items,
                    x0=x0,
                    bottom_y=bottom_y,
                    content_w=content_w,
                    bottom_h=bottom_h,
                    gap=gap,
                    charts=charts,
                    facts=facts,
                )
        else:
            row_h = (chart_area_h - gap) / 2.0
            chart_y1 = y0 + header_h + gap
            chart_y2 = chart_y1 + row_h + gap
            half_w = (content_w - gap) / 2.0

            if charts.get("scatter"):
                items.append(
                    ("image", x0, chart_y1, content_w, row_h, charts["scatter"])
                )
            if charts.get("trail_difficulty"):
                items.append(
                    ("image", x0, chart_y2, half_w, row_h, charts["trail_difficulty"])
                )
            if charts.get("lift_types"):
                items.append(
                    (
                        "image",
                        x0 + half_w + gap,
                        chart_y2,
                        half_w,
                        row_h,
                        charts["lift_types"],
                    )
                )

        if records_h > 0:
            records_y = y0 + content_h - records_h
            items.append(
                (
                    "text",
                    x0,
                    records_y,
                    content_w,
                    records_h,
                    runs_region_facts_records(facts),
                    scale.linesp * 0.95,
                )
            )
    else:
        pad = 14.0
        items.append(
            (
                "shape",
                x0,
                y0,
                content_w,
                content_h,
                "AtlasStatsBg",
            )
        )
        items.append(
            (
                "text",
                x0 + pad,
                y0 + pad,
                content_w - 2 * pad,
                content_h - 2 * pad,
                runs_region_facts(facts),
                scale.linesp,
            )
        )

    return items
