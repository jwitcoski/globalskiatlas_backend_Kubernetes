"""Print-layout dimensions shared by data_to_qgis and export_layouts."""

from __future__ import annotations

from typing import Literal

# ski_atlas_small_medium_template.qgz (fallback / legacy) — main map LayoutItem (mm)
MAIN_MAP_FRAME_WIDTH_MM = 107.95
MAIN_MAP_FRAME_HEIGHT_MM = 139.7

LayoutTier = Literal[
    "small",
    "medium",
    "large",
    "mega",
    "small_medium",
    "small_landscape",
    "medium_landscape",
    "large_landscape",
    "mega_landscape",
]


def main_map_frame_mm(tier: LayoutTier | str) -> tuple[float, float]:
    """Main trail-map frame size (mm); export page matches this (InDesign places copy separately).

    small/medium: compact plates. large: A4 width × half height. mega: full A4.
    *_landscape tiers swap width and height vs the base tier so users can pick orientation.
    """
    key = str(tier).strip().lower()
    m: dict[str, tuple[float, float]] = {
        "small": (105.0, 74.25),
        "medium": (105.0, 148.5),
        "large": (210.0, 148.5),
        "mega": (210.0, 297.0),
        "small_medium": (MAIN_MAP_FRAME_WIDTH_MM, MAIN_MAP_FRAME_HEIGHT_MM),
        # Swapped dimensions (see build_atlas_layout_templates).
        "small_landscape": (74.25, 105.0),
        "medium_landscape": (148.5, 105.0),
        "large_landscape": (148.5, 210.0),
        "mega_landscape": (297.0, 210.0),
    }
    if key not in m:
        raise KeyError(f"Unknown layout tier {tier!r}")
    return m[key]


def _bounds_look_geographic(bounds: tuple[float, float, float, float]) -> bool:
    """True when bounds are lon/lat degrees (not projected map units)."""
    xmin, ymin, xmax, ymax = bounds
    return (
        max(abs(xmin), abs(xmax), abs(ymin), abs(ymax)) <= 360
        and abs(xmax - xmin) <= 360
        and abs(ymax - ymin) <= 180
    )


def expand_bounds_to_main_map_aspect(
    bounds: tuple[float, float, float, float],
    *,
    frame_width_mm: float | None = None,
    frame_height_mm: float | None = None,
) -> tuple[float, float, float, float]:
    """Symmetrically expand xmin..ymax so (xmax-xmin)/(ymax-ymin) matches the layout frame.

    The map item has a fixed width:height in mm. QGIS scales the geographic extent to
    fit inside that rectangle; if the extent's width/height ratio in map CRS does not
    match the frame ratio, it letterboxes (unused white inside the frame).

    For EPSG:4326 extents we approximate "width" in *meters* by scaling longitude span
    by cos(latitude) at the extent midpoint, so the aspect match is much closer to the
    true rendered aspect. Projected CRS bounds (e.g. EPSG:5070) already use map units.
    """
    xmin, ymin, xmax, ymax = bounds
    w = xmax - xmin
    h = ymax - ymin
    if w <= 0 or h <= 0:
        return bounds
    import math

    fw = frame_width_mm if frame_width_mm is not None else MAIN_MAP_FRAME_WIDTH_MM
    fh = frame_height_mm if frame_height_mm is not None else MAIN_MAP_FRAME_HEIGHT_MM
    target_wh = fw / fh

    geographic = _bounds_look_geographic(bounds)
    if geographic:
        lat_mid = (ymin + ymax) / 2.0
        cos_lat = max(0.01, math.cos(math.radians(lat_mid)))
        cur_wh = (w * cos_lat) / h
    else:
        cos_lat = 1.0
        cur_wh = w / h

    if cur_wh < target_wh:
        w_new = (h * target_wh) / cos_lat
        dx = (w_new - w) / 2.0
        return (xmin - dx, ymin, xmax + dx, ymax)
    h_new = (w * cos_lat) / target_wh
    dy = (h_new - h) / 2.0
    return (xmin, ymin - dy, xmax, ymax + dy)


def expand_bounds_for_rotation(
    bounds: tuple[float, float, float, float],
    rotation_deg: float,
) -> tuple[float, float, float, float]:
    r"""Conservatively expand bounds so a rotated map doesn't clip the target area.

    QGIS rotates the map item view. If we set an extent tightly around the ski area
    and then apply a rotation, corners can clip. A conservative fix is to expand the
    extent by a factor \(s = |cos θ| + |sin θ|\) about the center.
    """
    import math

    xmin, ymin, xmax, ymax = bounds
    w = xmax - xmin
    h = ymax - ymin
    if w <= 0 or h <= 0:
        return bounds
    theta = math.radians(rotation_deg % 360.0)
    s = abs(math.cos(theta)) + abs(math.sin(theta))
    # s is in [1, sqrt(2)] for 2D rotations
    w2 = w * s
    h2 = h * s
    cx = (xmin + xmax) / 2.0
    cy = (ymin + ymax) / 2.0
    return (cx - w2 / 2.0, cy - h2 / 2.0, cx + w2 / 2.0, cy + h2 / 2.0)
