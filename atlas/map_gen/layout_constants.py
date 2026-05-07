"""Print-layout dimensions shared by data_to_qgis and export_layouts."""

from __future__ import annotations

# ski_atlas_small_medium_template.qgz — main map LayoutItem (mm)
MAIN_MAP_FRAME_WIDTH_MM = 107.95
MAIN_MAP_FRAME_HEIGHT_MM = 139.7


def expand_bounds_to_main_map_aspect(
    bounds: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    """Symmetrically expand xmin..ymax so (xmax-xmin)/(ymax-ymin) matches the layout frame.

    The map item has a fixed width:height in mm. QGIS scales the geographic extent to
    fit inside that rectangle; if the extent's width/height ratio in map CRS does not
    match the frame ratio, it letterboxes (unused white inside the frame).

    For EPSG:4326 extents we approximate "width" in *meters* by scaling longitude span
    by cos(latitude) at the extent midpoint, so the aspect match is much closer to the
    true rendered aspect.
    """
    xmin, ymin, xmax, ymax = bounds
    w = xmax - xmin
    h = ymax - ymin
    if w <= 0 or h <= 0:
        return bounds
    import math

    target_wh = MAIN_MAP_FRAME_WIDTH_MM / MAIN_MAP_FRAME_HEIGHT_MM

    lat_mid = (ymin + ymax) / 2.0
    cos_lat = max(0.01, math.cos(math.radians(lat_mid)))
    # approximate width/height in meters: dx_deg * cos(lat) / dy_deg
    cur_wh = (w * cos_lat) / h

    if cur_wh < target_wh:
        # need wider: compute required longitude span in degrees
        w_new = (h * target_wh) / cos_lat
        dx = (w_new - w) / 2.0
        return (xmin - dx, ymin, xmax + dx, ymax)
    # need taller: compute required latitude span in degrees
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
