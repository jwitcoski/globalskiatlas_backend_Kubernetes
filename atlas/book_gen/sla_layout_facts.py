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


def layout_region_facts_page(
    facts: RegionalFacts,
    *,
    page_x: float,
    page_y: float,
    content_x: float,
    content_y: float,
    content_w: float,
    content_h: float,
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
        header_h = 52.0
        records_h = 44.0
        chart_area_h = content_h - header_h - records_h - 2 * gap

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
            # Upper half of page content: elevation mountains; smaller charts below.
            elev_h = chart_area_h * 0.54
            small_block_h = chart_area_h - elev_h - gap
            small_row_h = (small_block_h - gap) / 2.0
            col_w = (content_w - gap) / 2.0

            chart_y_elev = y0 + header_h + gap
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

            y1 = chart_y_elev + elev_h + gap
            if charts.get("scatter"):
                items.append(
                    ("image", x0, y1, col_w, small_row_h, charts["scatter"])
                )
            if charts.get("tiers"):
                items.append(
                    (
                        "image",
                        x0 + col_w + gap,
                        y1,
                        col_w,
                        small_row_h,
                        charts["tiers"],
                    )
                )
            y2 = y1 + small_row_h + gap
            if charts.get("trail_difficulty"):
                items.append(
                    ("image", x0, y2, col_w, small_row_h, charts["trail_difficulty"])
                )
            right_bottom = charts.get("base_elevations") or charts.get("lift_types")
            if right_bottom:
                items.append(
                    (
                        "image",
                        x0 + col_w + gap,
                        y2,
                        col_w,
                        small_row_h,
                        right_bottom,
                    )
                )
        else:
            row_h = (chart_area_h - gap) / 2.0
            chart_y1 = y0 + header_h + gap
            chart_y2 = chart_y1 + row_h + gap
            col_w_left = content_w * 0.58
            col_w_right = content_w - col_w_left - gap

            if charts.get("scatter"):
                items.append(
                    ("image", x0, chart_y1, col_w_left, row_h, charts["scatter"])
                )
            if charts.get("tiers"):
                items.append(
                    (
                        "image",
                        x0 + col_w_left + gap,
                        chart_y1,
                        col_w_right,
                        row_h,
                        charts["tiers"],
                    )
                )

            half_w = (content_w - gap) / 2.0
            if charts.get("trail_difficulty"):
                items.append(
                    ("image", x0, chart_y2, half_w, row_h, charts["trail_difficulty"])
                )
            right_bottom = charts.get("base_elevations") or charts.get("lift_types")
            if right_bottom:
                items.append(
                    ("image", x0 + half_w + gap, chart_y2, half_w, row_h, right_bottom)
                )

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
