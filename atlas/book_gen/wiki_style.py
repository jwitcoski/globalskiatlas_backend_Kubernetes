"""
Wiki resort page visual design — mirrors GlobalSkiAtlas_2/wiki/css/style.css.

CSS variables:
  --resort-location-color: #5b8fb4
  --resort-title-color: #1a365d
  --resort-subtitle-color: #5b8fb4
  --resort-stats-bg: #e8f0f7
  --resort-stats-label: #2c5282
  --resort-body-color: #2d3748
  --resort-footer-color: #718096
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

# Scribus document color names (RGB injected in build_document_root)
C_TITLE = "AtlasTitle"
C_LOCATION = "AtlasLocation"
C_SUBTITLE = "AtlasSubtitle"
C_STATS_BG = "AtlasStatsBg"
C_STATS_LABEL = "AtlasStatsLabel"
C_BODY = "AtlasBody"
C_FOOTER = "AtlasFooter"

DOCUMENT_COLORS: tuple[tuple[str, int, int, int], ...] = (
    ("AtlasTitle", 26, 54, 93),
    ("AtlasLocation", 91, 143, 180),
    ("AtlasSubtitle", 91, 143, 180),
    ("AtlasStatsBg", 232, 240, 247),
    ("AtlasStatsLabel", 44, 82, 130),
    ("AtlasBody", 45, 55, 72),
    ("AtlasFooter", 113, 128, 150),
)

FONT_SANS = "Arial Regular"


@dataclass(frozen=True)
class TextRun:
    text: str
    fontsize: float
    fcolor: str = C_BODY
    font: str = FONT_SANS


@dataclass(frozen=True)
class TypeScale:
    location: float
    title: float
    subtitle: float
    stats_label: float
    stats_value: float
    trail: float
    body: float
    drop_cap: float
    footer: float
    linesp: float
    stats_pad: float


# Base sizes (pt) tuned for 8.5×11 content area; scaled per slot.
_BASE = TypeScale(
    location=8.0,
    title=17.0,
    subtitle=9.5,
    stats_label=6.5,
    stats_value=8.0,
    trail=7.5,
    body=10.0,
    drop_cap=26.0,
    footer=7.5,
    linesp=12.0,
    stats_pad=6.0,
)

_SLOT_SCALE: dict[str, float] = {
    "quarter": 0.58,
    "half": 0.78,
    "full": 1.0,
    "spread": 1.0,
}


def type_scale_for_slot(slot: str) -> TypeScale:
    s = _SLOT_SCALE.get(slot, 1.0)

    def sc(v: float) -> float:
        return max(4.5, round(v * s, 1))

    return TypeScale(
        location=sc(_BASE.location),
        title=sc(_BASE.title),
        subtitle=sc(_BASE.subtitle),
        stats_label=sc(_BASE.stats_label),
        stats_value=sc(_BASE.stats_value),
        trail=sc(_BASE.trail),
        body=sc(_BASE.body),
        drop_cap=sc(_BASE.drop_cap),
        footer=sc(_BASE.footer),
        linesp=sc(_BASE.linesp),
        stats_pad=sc(_BASE.stats_pad),
    )


def runs_location(text: str, scale: TypeScale) -> list[TextRun]:
    return [TextRun(text, scale.location, C_LOCATION)]


def runs_title(text: str, scale: TypeScale) -> list[TextRun]:
    return [TextRun(text, scale.title, C_TITLE)]


def runs_subtitle(text: str, scale: TypeScale) -> list[TextRun]:
    return [TextRun(text, scale.subtitle, C_SUBTITLE)]


def runs_footer(text: str, scale: TypeScale) -> list[TextRun]:
    return [TextRun(text, scale.footer, C_FOOTER)]


def runs_trail_breakdown(text: str, scale: TypeScale) -> list[TextRun]:
    if not text.strip():
        return []
    if text.lower().startswith("trail breakdown"):
        label, _, rest = text.partition(":")
        return [
            TextRun(label + ": ", scale.trail, C_STATS_LABEL),
            TextRun(rest.strip() or text, scale.trail, C_BODY),
        ]
    return [TextRun(text, scale.trail, C_BODY)]


def runs_stat_line(label: str, value: str, scale: TypeScale) -> list[TextRun]:
    return [
        TextRun(label + " ", scale.stats_label, C_STATS_LABEL),
        TextRun(value, scale.stats_value, C_BODY),
    ]


def runs_stats_pairs(pairs: Sequence[tuple[str, str]], scale: TypeScale) -> list[TextRun]:
    runs: list[TextRun] = []
    for i, (label, value) in enumerate(pairs):
        if i > 0:
            runs.append(TextRun("\n", scale.stats_value, C_BODY))
        runs.extend(runs_stat_line(label, value, scale))
    return runs


def runs_body_plain(text: str, scale: TypeScale) -> list[TextRun]:
    return [TextRun(text, scale.body, C_BODY)] if text else []


def runs_drop_cap_body(
    drop: str,
    body_after: str,
    scale: TypeScale,
) -> list[TextRun]:
    runs: list[TextRun] = []
    if drop:
        runs.append(TextRun(drop, scale.drop_cap, C_TITLE))
    if body_after:
        runs.append(TextRun(body_after, scale.body, C_BODY))
    return runs
