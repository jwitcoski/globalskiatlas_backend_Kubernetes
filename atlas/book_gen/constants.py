"""Book layout constants aligned with GlobalSkiAtlas_2 wiki."""

from __future__ import annotations

RESORT_FACT_MAX_RANK: dict[str, int] = {
    "small_hill": 4,
    "ski_mountain": 10,
    "multiple_mountains": 11,
    "mega_resort": 14,
    "unknown": 4,
}

RESORT_TEXT_LIMIT: dict[str, int] = {
    "small_hill": 1500,
    "ski_mountain": 3000,
    "multiple_mountains": 5500,
    "mega_resort": 11000,
    "unknown": 1500,
}

RESORT_CATEGORY_LABEL: dict[str, str] = {
    "small_hill": "Small hill",
    "ski_mountain": "Ski mountain",
    "multiple_mountains": "Multiple mountains",
    "mega_resort": "Mega resort",
    "unknown": "Not a downhill ski hill",
}

# Page units per resort (wiki-analyze-parquet-categories.js)
PAGE_FRACTION_BY_CATEGORY: dict[str, float] = {
    "small_hill": 0.25,
    "ski_mountain": 0.5,
    "multiple_mountains": 1.0,
    "mega_resort": 2.0,
    "unknown": 0.0,
}

# Wiki resortSizeCategory → QGIS map export tier
CATEGORY_TO_MAP_TIER: dict[str, str] = {
    "small_hill": "small",
    "ski_mountain": "medium",
    "multiple_mountains": "large",
    "mega_resort": "mega",
}

SLOT_NAMES = ("quarter", "half", "full", "spread")

# Max characters rendered per book slot (tune in book.yaml before regenerating Bedrock copy).
DEFAULT_SLOT_BODY_CHAR_LIMIT: dict[str, int | None] = {
    "quarter": 400,
    "half": 900,
    "full": None,
    "spread": None,
}

# Book chapter order: small → medium → large → mega, then A–Z by title within each tier.
CATEGORY_BOOK_ORDER: dict[str, int] = {
    "small_hill": 0,
    "ski_mountain": 1,
    "multiple_mountains": 2,
    "mega_resort": 3,
    "unknown": 99,
}


def slot_body_char_limits(book_config: dict | None = None) -> dict[str, int | None]:
    """Resolve per-slot body char limits from book.yaml over defaults."""
    limits = dict(DEFAULT_SLOT_BODY_CHAR_LIMIT)
    raw = (book_config or {}).get("slot_body_char_limit")
    if isinstance(raw, dict):
        for key, val in raw.items():
            if val is None:
                limits[str(key)] = None
            else:
                limits[str(key)] = int(val)
    return limits
