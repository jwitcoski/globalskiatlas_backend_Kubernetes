"""OSM piste/lift/forest vectors for the clay homepage / wiki scene."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shapely.geometry import LineString, mapping, shape

from game_export.config import GameExportConfig
from game_export.coords import LocalCRS
from game_export.osm_stripdown import (
    is_keep_piste,
    is_lift_pylon_or_station,
    prefer_line_piste_features,
)
from game_export.routes import _as_lines, densify_line, polygon_descent_flowline
from game_export.vectors import collect_layers, write_local_geojson

log = logging.getLogger("game_export")

_PROP_KEYS = (
    "id",
    "name",
    "piste:difficulty",
    "piste:type",
    "piste_difficulty",
    "piste_type",
    "difficulty",
    "aerialway",
    "tags",
    "other_tags",
    "landuse",
    "natural",
)


def _copy_props(props: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in _PROP_KEYS:
        if key in props and props[key] not in (None, ""):
            out[key] = props[key]
    return out


def build_piste_trail_features(
    piste_features: list[dict],
    elev,
    transform,
    local: LocalCRS,
    cfg: GameExportConfig,
    *,
    spacing_m: float = 16.0,
    min_length_m: float = 18.0,
) -> list[dict]:
    """Downhill centerlines: prefer mapped lines; polygon-only → descent flowline."""
    src = prefer_line_piste_features(
        [f for f in piste_features if is_keep_piste(f.get("properties") or {})]
    )
    trails: list[dict] = []
    for feat in src:
        props = feat.get("properties") or {}
        geom = shape(feat["geometry"])
        lines: list[LineString] = []
        if geom.geom_type in ("Polygon", "MultiPolygon"):
            polys = geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
            for poly in polys:
                fl = polygon_descent_flowline(poly, elev, transform, local, spacing_m)
                if fl is not None and fl.length >= min_length_m:
                    lines.append(fl)
        else:
            lines = [ln for ln in _as_lines(geom) if ln.length >= min_length_m]
        for line in lines:
            coords = densify_line(list(line.coords), spacing_m)
            if len(coords) < 2:
                continue
            trails.append(
                {
                    "type": "Feature",
                    "geometry": mapping(LineString(coords)),
                    "properties": _copy_props(props),
                }
            )
    return trails


def _forest_polygon_features(forest_features: list[dict]) -> list[dict]:
    """Keep wood/forest polygons only (client plants an even grid from these)."""
    out: list[dict] = []
    for feat in forest_features:
        try:
            geom = shape(feat["geometry"])
        except Exception:
            continue
        if geom.is_empty or geom.geom_type not in ("Polygon", "MultiPolygon"):
            continue
        if geom.area <= 25:
            continue
        props = _copy_props(feat.get("properties") or {})
        props.setdefault("natural", "wood")
        out.append(
            {
                "type": "Feature",
                "geometry": mapping(geom),
                "properties": props,
            }
        )
    return out


def write_homepage_vectors(
    out: Path,
    *,
    cfg: GameExportConfig,
    inputs,
    local: LocalCRS,
    elev,
    transform,
    to_proj,
    repairs: list | None = None,
) -> dict[str, int]:
    layers, layer_repairs = collect_layers(
        inputs.osm_nearby,
        inputs.pistes,
        inputs.lifts,
        to_proj,
        local,
        cfg,
        mode="clay",
    )
    if repairs is not None:
        repairs.extend(layer_repairs)

    vectors_dir = out / "vectors"
    vectors_dir.mkdir(parents=True, exist_ok=True)

    piste_src = [
        f
        for f in (layers.get("pistes") or [])
        if is_keep_piste(f.get("properties") or {})
    ]
    trail_feats = build_piste_trail_features(
        piste_src,
        elev,
        transform,
        local,
        cfg,
    )
    write_local_geojson(vectors_dir / "piste-trails.geojson", trail_feats, local, "piste-trails")

    lift_feats = [
        f
        for f in (layers.get("lifts") or [])
        if not is_lift_pylon_or_station(f.get("properties") or {})
    ]
    write_local_geojson(vectors_dir / "lifts.geojson", lift_feats, local, "lifts")

    forest_feats = _forest_polygon_features(layers.get("forest") or [])
    write_local_geojson(vectors_dir / "forest.geojson", forest_feats, local, "forest")
    # Remove legacy tree-points if rebuilding an existing folder.
    legacy_trees = vectors_dir / "tree-points.geojson"
    if legacy_trees.is_file():
        legacy_trees.unlink()

    # Island rim for the wiki clay viewer (convex AOI in local meters).
    try:
        from shapely.geometry import mapping as shp_mapping

        poly_local = None
        ski = inputs.ski_area
        if ski is not None and getattr(ski, "geometry", None) is not None:
            from game_export.coords import geom_to_local, geom_to_projected

            poly_local = geom_to_local(geom_to_projected(ski.geometry, to_proj), local)
        if poly_local is not None and not poly_local.is_empty:
            buf = poly_local.buffer(80)  # meters
            write_local_geojson(
                vectors_dir / "ski-area-buffer.geojson",
                [
                    {
                        "type": "Feature",
                        "geometry": shp_mapping(buf),
                        "properties": {
                            "winter_sports_id": cfg.winter_sports_id,
                            "kind": "ski_area_buffer",
                        },
                    }
                ],
                local,
                "ski-area-buffer",
            )
    except Exception as exc:
        log.warning("ski-area-buffer write skipped: %s", exc)

    counts = {
        "pistes": len(piste_src),
        "piste_trails": len(trail_feats),
        "lifts": len(lift_feats),
        "forest": len(forest_feats),
        "tree_points": 0,
    }
    log.info(
        "Homepage vectors: pistes=%s trail_lines=%s lifts=%s forest_polys=%s",
        counts["pistes"],
        counts["piste_trails"],
        counts["lifts"],
        counts["forest"],
    )
    return counts
