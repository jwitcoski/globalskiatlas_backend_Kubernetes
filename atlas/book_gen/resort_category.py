"""Resort size category — matches GlobalSkiAtlas_2/scripts/wiki-ingest-parquet.js."""

from __future__ import annotations

from typing import Any, Optional

NOT_DOWNHILL = "not a downhill ski resort"
RESORT_TYPE_KEYS = ("resort_type", "Resort Type")


def _get_prop(obj: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k]
    return None


def _num(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return None if n != n else n  # NaN


def is_not_downhill(row: dict[str, Any]) -> bool:
    v = _get_prop(row, RESORT_TYPE_KEYS)
    return (
        v is not None
        and str(v).strip().casefold() == NOT_DOWNHILL.casefold()
    )


def resort_size_category(row: dict[str, Any]) -> str:
    """Return wiki resortSizeCategory for a parquet or API row."""
    if is_not_downhill(row):
        return "unknown"

    trails = _num(
        _get_prop(
            row,
            (
                "downhill_trails",
                "downhillTrails",
                "Downhill Trails",
                "downhill_trail_count",
            ),
        )
    )
    lifts = _num(
        _get_prop(
            row,
            ("total_lifts", "totalLifts", "Total Lifts", "lifts", "lift_count"),
        )
    )
    acres = _num(
        _get_prop(
            row,
            (
                "skiable_terrain_acres",
                "skiableTerrainAcres",
                "Skiable Terrain Acres",
            ),
        )
    )
    if acres is None:
        ha = _num(
            _get_prop(row, ("skiable_terrain_ha", "skiableTerrainHa", "Skiable Terrain Ha"))
        )
        if ha is not None:
            acres = ha * 2.471

    has_trails = trails is not None and trails >= 0
    has_lifts = lifts is not None and lifts >= 0
    if not has_trails or not has_lifts:
        return "unknown"

    t = trails
    a = acres if (acres is not None and acres >= 0) else 0.0

    if t >= 200 or a >= 10000:
        return "mega_resort"
    if t >= 100 or a >= 5000:
        return "multiple_mountains"
    if t >= 50 or a >= 1000:
        return "ski_mountain"
    return "small_hill"


def page_fraction(category: str) -> float:
    from atlas.book_gen.constants import PAGE_FRACTION_BY_CATEGORY

    return PAGE_FRACTION_BY_CATEGORY.get(category, 0.0)


def slot_for_fraction(frac: float) -> str:
    if frac <= 0:
        return "skip"
    if frac <= 0.25:
        return "quarter"
    if frac <= 0.5:
        return "half"
    if frac <= 1.0:
        return "full"
    return "spread"
