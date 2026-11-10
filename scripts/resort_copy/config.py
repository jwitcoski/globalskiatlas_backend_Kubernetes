"""Tier tables, topic lists, and defaults shared by every pass."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from . import bedrock


TIER_CHOICES: tuple[str, ...] = (*bedrock.PLATE_TIERS, *bedrock._TIER_ALIASES)


# Extra visit facts on top of the anchors. The town is not one of them.
TIER_FACT_MIN: dict[str, int] = {
    "small": 6,
    "medium": 12,
    "large": 15,
    "mega": 15,
}


TIER_SNIPPETS: dict[str, int] = {
    "small": 16,
    "medium": 26,
    "large": 30,
    "mega": 30,
}


# Pages read in full (not just the search blurb) before the search pass.
TIER_PAGES: dict[str, int] = {
    "small": 4,
    "medium": 8,
    "large": 10,
}


PAGE_EXCERPT_CHARS = 1400


ARCHIVE_PAGES = 3


# Each page is read on its own for facts. The key-point call only sees the top of it.
PAGE_FACTS_PER_PAGE = 6


PAGE_KEY_POINT_CHARS = 600


# The search pass sends instructions, sources, and room for the JSON reply in one window.
TIER_SEARCH_CTX: dict[str, int] = {
    "small": 8192,
    "medium": 8192,
    "large": 10240,
}


SEARCH_SYSTEM_TOKENS = 1400


# Visit facts kept for the writer. A one-page resort needs more than the minimum.
TIER_NOTE_CAP: dict[str, int] = {
    "small": 6,
    "medium": 24,
    "large": 32,
}


# Enough beats for the anchors plus a few visit facts. The writer still gets every note.
TIER_BEATS: dict[str, int] = {
    "small": 8,
    "medium": 16,
    "large": 24,
    "mega": 24,
}


# The atlas already knows the town from OpenStreetMap. Do not spend a fact on it.
REQUIRED_TOPICS: tuple[str, ...] = (
    "year_started",
    "pass",
    "public_or_private",
    "setting",
)


COLOR_TOPICS: tuple[str, ...] = (
    "famous_people",
    "lodging",
    "scene",
    "review",
    "access",
    "price",
    "who",
    "other",
)


DEFAULT_HOST = "http://192.168.68.59:11434"


KEY_POINTS_PATH = Path("output/resort_key_points/key_points.jsonl")


DEFAULT_MODEL = "llama3.1:8b"


DEFAULT_NUM_CTX = 4096


def _tier_key(tier: str) -> str:
    return bedrock.canonical_tier(tier)


def fact_min_for(tier: str) -> int:
    return TIER_FACT_MIN.get(_tier_key(tier), TIER_FACT_MIN["medium"])


def _cell(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, float) and value != value:
            continue
        text = str(value).strip()
        if text and text.lower() != "nan":
            return text
    return None
