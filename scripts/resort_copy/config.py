"""Tier tables, topic lists, and defaults shared by every pass."""
from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from typing import Any

from . import bedrock


AI_BOX_HOSTNAMES: frozenset[str] = frozenset({"3070", "homeserver"})


def _on_ai_box() -> bool:
    """True when this process runs on one of the AI boxes (3070 or homeserver).

    `AI_BOX=1` forces it on any host; `AI_BOX=0` forces it off.
    """
    forced = (os.environ.get("AI_BOX") or "").strip()
    if forced in {"1", "0"}:
        return forced == "1"
    return socket.gethostname().strip().lower() in AI_BOX_HOSTNAMES


def _box_data_root() -> Path:
    return Path.home() / "ski"


def output_root() -> Path:
    """Where checkpoints and key points are written.

    Defaults to the repo's `output/`. On an AI box (3070, homeserver), defaults to
    `~/ski/output` so a laptop shutdown cannot take the data with it.
    `GLOBALS_SKI_OUTPUT` always wins. Relative paths stay relative to the
    working directory, as before.
    """
    raw = (os.environ.get("GLOBALS_SKI_OUTPUT") or "").strip()
    if raw:
        return Path(raw)
    if _on_ai_box():
        return _box_data_root() / "output"
    return Path("output")


def output_path(*parts: str) -> Path:
    return output_root().joinpath(*parts)


def cache_root() -> Path:
    """Where fetched pages and research registries are cached.

    Same reasoning as `output_root`: on an AI box this should be local disk, not a
    network share back to the laptop. `GLOBALS_SKI_CACHE` overrides it.
    """
    raw = (os.environ.get("GLOBALS_SKI_CACHE") or "").strip()
    if raw:
        return Path(raw)
    if _on_ai_box():
        return _box_data_root() / "cache"
    return Path("cache")


def _plate_choices() -> tuple[str, ...]:
    """small, medium, large, plus layout aliases. The retired mega plate is not a tier."""
    seen: list[str] = []
    for name in (*bedrock.PLATE_TIERS, *bedrock._TIER_ALIASES):
        if name == "mega" or str(name).startswith("mega"):
            continue
        if bedrock.canonical_tier(name) not in {"small", "medium", "large"}:
            continue
        if name not in seen:
            seen.append(name)
    return tuple(seen)


TIER_CHOICES: tuple[str, ...] = _plate_choices()


def _tier_file() -> dict[str, Any]:
    path = Path(__file__).with_name("tier_config.json")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


_TIER_FILE = _tier_file()


# Extra visit facts on top of the anchors. The town is not one of them.
# Medium and large minima are high enough that a thin search cannot invent a full page.
TIER_FACT_MIN: dict[str, int] = {
    "small": 6,
    "medium": 22,
    "large": 45,
}


TIER_SNIPPETS: dict[str, int] = {
    "small": 16,
    "medium": 36,
    "large": 48,
}


# Pages read in full (not just the search blurb) before the search pass.
TIER_PAGES: dict[str, int] = {
    "small": 4,
    "medium": 8,
    "large": 12,
}


PAGE_EXCERPT_CHARS = 1400


# Old bullets were "25 words or fewer". Triple that so ~45 large-resort hits
# can fill the print plate without needing 90 separate facts.
NOTE_TEXT_MAX_WORDS = 75
NOTE_TEXT_RULE = (
    f"Each note is up to {NOTE_TEXT_MAX_WORDS} words. "
    "Lead with the fact, then add extra color from the same source "
    "(a named detail, a second clause, why it matters). "
    "Do not stop at one clause when the source has more. "
    "Do not invent. A thin source can stay short."
)


ARCHIVE_PAGES = 3


# Each page is read on its own for facts. The key-point call only sees the top of it.
# Enough separate facts from one page that the note pile can run past the printed page.
PAGE_FACTS_PER_PAGE = 10


# Fact extraction walks the stored page body in these windows (10 facts each).
PAGE_FACT_WINDOW_CHARS = PAGE_EXCERPT_CHARS


PAGE_FACT_MAX_WINDOWS = 12


# Soft cap so a huge marketing site cannot explode the fact loop.
PAGE_BODY_MAX_CHARS = PAGE_FACT_WINDOW_CHARS * PAGE_FACT_MAX_WINDOWS


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
    "small": 16,
    "medium": 40,
    "large": 70,
}


TIER_SEARCH_RETRIES: dict[str, int] = {
    "small": 2,
    "medium": 2,
    "large": 3,
}


# Enough beats for the anchors plus a few visit facts. The writer still gets every note.
TIER_BEATS: dict[str, int] = {
    "small": 8,
    "medium": 16,
    "large": 24,
}


def _overlay_tier_tables() -> None:
    """tier_config.json wins over the defaults above."""
    tiers = _TIER_FILE.get("tiers") if isinstance(_TIER_FILE.get("tiers"), dict) else {}
    tables = {
        "search_fact_min": TIER_FACT_MIN,
        "snippets": TIER_SNIPPETS,
        "pages": TIER_PAGES,
        "note_cap": TIER_NOTE_CAP,
        "search_retries": TIER_SEARCH_RETRIES,
        "beats": TIER_BEATS,
    }
    for name, block in tiers.items():
        if name not in {"small", "medium", "large"} or not isinstance(block, dict):
            continue
        for key, table in tables.items():
            if key in block:
                table[name] = int(block[key])


_overlay_tier_tables()


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


KEY_POINTS_PATH = output_path("resort_key_points", "key_points.jsonl")


DEFAULT_MODEL = "llama3.1:8b"


DEFAULT_NUM_CTX = 4096


def _tier_key(tier: str) -> str:
    key = bedrock.canonical_tier(tier)
    if key not in {"small", "medium", "large"}:
        return "medium"
    return key


def _tier_block(tier: str) -> dict[str, Any]:
    tiers = _TIER_FILE.get("tiers") if isinstance(_TIER_FILE.get("tiers"), dict) else {}
    block = tiers.get(_tier_key(tier), {})
    return block if isinstance(block, dict) else {}


def fact_min_for(tier: str) -> int:
    block = _tier_block(tier)
    if "search_fact_min" in block:
        return int(block["search_fact_min"])
    return TIER_FACT_MIN.get(_tier_key(tier), TIER_FACT_MIN["medium"])


def write_floor_for(tier: str) -> int:
    """How many distinct notes are required before a page is written.

    Small 6, medium and large 22. Character length is a separate gate
    (`write_char_floor_for`). Both must pass.
    """
    return min(fact_min_for(tier), 22)


def write_char_floor_for(tier: str) -> int:
    """How many characters of note text are required before a page is written.

    The printed plate minimum: small 1450, medium 3700, large 7400. A resort can
    have 22 notes and still fail this if they are too short to fill the plate.
    """
    return int(bedrock.copy_goal_for_tier(tier).characters[0])


def search_ladder_for(tier: str) -> tuple[str, ...]:
    """Print tier stays put. Search may step up a size if the pantry is still thin.

    Small keeps researching like medium; medium like large; large stays large then
    gets an extra top-up pass (see LARGE_EXTRA_WINDOWS in topup).
    """
    key = _tier_key(tier)
    if key == "small":
        return ("small", "medium")
    if key == "medium":
        return ("medium", "large")
    return ("large",)


def note_cap_for(tier: str) -> int:
    block = _tier_block(tier)
    if "note_cap" in block:
        return int(block["note_cap"])
    return TIER_NOTE_CAP.get(_tier_key(tier), fact_min_for(tier))


def search_retries_for(tier: str) -> int:
    block = _tier_block(tier)
    if "search_retries" in block:
        return int(block["search_retries"])
    return TIER_SEARCH_RETRIES.get(_tier_key(tier), 2)


def section_writing_for(tier: str) -> bool:
    block = _tier_block(tier)
    if "section_writing" in block:
        return bool(block["section_writing"])
    return False


def configured_models() -> dict[str, str]:
    raw = _TIER_FILE.get("models")
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    return {}


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
