"""Conservative exclusion / hazard polygons in local meters."""
from __future__ import annotations

import logging

import numpy as np
from shapely.geometry import Polygon, box, mapping
from shapely.ops import unary_union

from game_export.config import GameExportConfig
from game_export.coords import LocalCRS
from game_export.vectors import write_local_geojson

log = logging.getLogger("game_export")


def _buffer_feats(features: list, dist: float) -> list:
    out = []
    for f in features:
        from shapely.geometry import shape

        g = shape(f["geometry"])
        if g.is_empty:
            continue
        try:
            b = g.buffer(dist)
        except Exception:
            continue
        if b.is_empty:
            continue
        out.append(b)
    return out


def steep_polygons(slope_deg: np.ndarray, transform, local: LocalCRS, threshold: float, cell_m: float):
    """Vectorize cells steeper than threshold (simple grid squares)."""
    polys = []
    rows, cols = slope_deg.shape
    west = transform.c
    north = transform.f
    step = max(1, int(round(4.0 / max(cell_m, 0.1))))
    for r in range(0, rows, step):
        for c in range(0, cols, step):
            v = slope_deg[r, c]
            if not np.isfinite(v) or v <= threshold:
                continue
            e0 = west + c * cell_m
            n1 = north - r * cell_m
            e1 = e0 + cell_m
            n0 = n1 - cell_m
            x0, z0 = e0 - local.origin_easting_m, n0 - local.origin_northing_m
            x1, z1 = e1 - local.origin_easting_m, n1 - local.origin_northing_m
            # local geojson uses east, north (not game z)
            polys.append(box(x0, z0, x1, z1))
    if not polys:
        return []
    # Merge to keep GeoJSON smaller
    u = unary_union(polys)
    if u.geom_type == "Polygon":
        return [u]
    return list(u.geoms)


def nodata_boundary(elev: np.ndarray, transform, local: LocalCRS, cell_m: float, edge_buffer: float):
    rows, cols = elev.shape
    west = transform.c
    north = transform.f
    # Valid bbox in local meters, then buffer inward as "terrain boundary" hazard ring
    valid = np.isfinite(elev)
    if not valid.any():
        return []
    rs, cs = np.where(valid)
    r0, r1 = int(rs.min()), int(rs.max())
    c0, c1 = int(cs.min()), int(cs.max())
    e0 = west + c0 * cell_m
    e1 = west + (c1 + 1) * cell_m
    n1 = north - r0 * cell_m
    n0 = north - (r1 + 1) * cell_m
    x0 = e0 - local.origin_easting_m
    x1 = e1 - local.origin_easting_m
    y0 = n0 - local.origin_northing_m
    y1 = n1 - local.origin_northing_m
    outer = box(x0, y0, x1, y1)
    inner = outer.buffer(-edge_buffer)
    if inner.is_empty:
        return [outer]
    ring = outer.difference(inner)
    return [ring] if not ring.is_empty else []


def build_exclusions(
    layers: dict,
    slope_deg,
    transform,
    local: LocalCRS,
    cfg: GameExportConfig,
    elev,
    cell_m: float,
    out_path,
) -> list:
    feats = []

    def add(geoms, hazard_type, src="derived"):
        for i, g in enumerate(geoms):
            if g is None or g.is_empty:
                continue
            feats.append(
                {
                    "type": "Feature",
                    "geometry": mapping(g),
                    "properties": {
                        "id": f"hazard:{hazard_type}:{i}",
                        "hazard_type": hazard_type,
                        "source": src,
                        "note": "Conservative prototype buffer; not a safety product",
                    },
                }
            )

    from shapely.geometry import shape

    add(_buffer_feats(layers.get("buildings") or [], cfg.building_buffer_m), "building")
    add(_buffer_feats(layers.get("water") or [], cfg.water_buffer_m), "water")
    add(_buffer_feats(layers.get("cliffs") or [], cfg.cliff_buffer_m), "cliff")
    road_geoms = []
    for f in layers.get("roads") or []:
        hw = (f.get("properties") or {}).get("highway") or (f.get("properties") or {}).get("tags", {}).get("highway")
        tags = (f.get("properties") or {}).get("tags") or {}
        hw = hw or tags.get("highway")
        if hw in set(cfg.highway_hazard_types):
            road_geoms.append(f)
    add(_buffer_feats(road_geoms, cfg.road_buffer_m), "road")
    add(
        steep_polygons(slope_deg, transform, local, cfg.steep_hazard_degrees, cell_m),
        "steep_terrain",
        "dem",
    )
    add(nodata_boundary(elev, transform, local, cell_m, cfg.terrain_boundary_buffer_m), "terrain_boundary", "dem")

    # Piste corridors (not hazards) — written separately by caller using same helper
    write_local_geojson(out_path, feats, local, "exclusion-zones")
    log.info("Wrote %s exclusion features", len(feats))
    return feats


def piste_corridors(piste_features: list, cfg: GameExportConfig) -> list:
    from shapely.geometry import shape

    out = []
    half = cfg.piste_corridor_default_half_width_m
    half = min(max(half, cfg.piste_corridor_min_half_width_m), cfg.piste_corridor_max_half_width_m)
    for f in piste_features:
        g = shape(f["geometry"])
        if g.geom_type in ("Polygon", "MultiPolygon"):
            c = g
        else:
            c = g.buffer(half)
        props = dict(f.get("properties") or {})
        props["corridor_half_width_m"] = half
        props["note"] = "Game corridor, not official run width"
        out.append({"type": "Feature", "geometry": mapping(c), "properties": props})
    return out
