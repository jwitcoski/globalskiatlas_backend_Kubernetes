"""OSM piste/lift vectors for the homepage hero scene."""
from __future__ import annotations

import logging
import random
from pathlib import Path
from typing import Any

import numpy as np
from shapely.geometry import LineString, Point, mapping, shape

from game_export.config import GameExportConfig
from game_export.coords import LocalCRS
from game_export.routes import _as_lines, densify_line, polygon_descent_flowline
from game_export.terrain import sample_elev
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
    """Downhill centerlines for every mapped OSM piste (lines + polygon flowlines)."""
    trails: list[dict] = []
    for feat in piste_features:
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


def _forest_polygons(forest_features: list[dict]) -> list:
    polys = []
    for feat in forest_features:
        geom = shape(feat["geometry"])
        if geom.geom_type == "Polygon":
            polys.append(geom)
        elif geom.geom_type == "MultiPolygon":
            polys.extend(geom.geoms)
    return [p for p in polys if not p.is_empty and p.area > 25]


def build_tree_point_features(
    forest_features: list[dict],
    elev,
    transform,
    local: LocalCRS,
    *,
    forest_count: int = 480,
    scatter_count: int = 200,
    seed: int = 20260823,
) -> list[dict]:
    """Pre-placed bright-tree locations in OSM forest + open slopes."""
    rng = random.Random(seed)
    feats: list[dict] = []

    def add_point(x: float, y: float) -> bool:
        z = sample_elev(elev, transform, x + local.origin_easting_m, y + local.origin_northing_m)
        if not np.isfinite(z):
            return False
        feats.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [x, y]},
                "properties": {"elevation_m": float(z), "kind": "tree"},
            }
        )
        return True

    polys = _forest_polygons(forest_features)
    if polys:
        areas = [p.area for p in polys]
        total = sum(areas)
        for poly, area in zip(polys, areas):
            target = max(2, int(round(forest_count * area / total)))
            minx, miny, maxx, maxy = poly.bounds
            added = 0
            tries = 0
            while added < target and tries < target * 30:
                tries += 1
                x = rng.uniform(minx, maxx)
                y = rng.uniform(miny, maxy)
                if not poly.contains(Point(x, y)):
                    continue
                if add_point(x, y):
                    added += 1

    rows, cols = elev.shape
    cell = abs(float(transform.a))
    margin = cell * 3
    max_e = cols * cell - margin
    max_n = rows * cell - margin
    added = 0
    tries = 0
    while added < scatter_count and tries < scatter_count * 40:
        tries += 1
        x = rng.uniform(margin, max_e)
        y = rng.uniform(margin, max_n)
        in_forest = any(p.contains(Point(x, y)) for p in polys) if polys else False
        if in_forest and rng.random() < 0.65:
            continue
        if add_point(x, y):
            added += 1

    return feats


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
    )
    if repairs is not None:
        repairs.extend(layer_repairs)

    vectors_dir = out / "vectors"
    vectors_dir.mkdir(parents=True, exist_ok=True)

    trail_feats = build_piste_trail_features(
        layers.get("pistes") or [],
        elev,
        transform,
        local,
        cfg,
    )
    write_local_geojson(vectors_dir / "piste-trails.geojson", trail_feats, local, "piste-trails")
    write_local_geojson(vectors_dir / "lifts.geojson", layers.get("lifts") or [], local, "lifts")
    tree_feats = build_tree_point_features(
        layers.get("forest") or [],
        elev,
        transform,
        local,
        seed=cfg.seed,
    )
    write_local_geojson(vectors_dir / "tree-points.geojson", tree_feats, local, "tree-points")

    counts = {
        "pistes": len(layers.get("pistes") or []),
        "piste_trails": len(trail_feats),
        "lifts": len(layers.get("lifts") or []),
        "forest": len(layers.get("forest") or []),
        "tree_points": len(tree_feats),
    }
    log.info(
        "Homepage vectors: pistes=%s trail_lines=%s lifts=%s forest=%s trees=%s",
        counts["pistes"],
        counts["piste_trails"],
        counts["lifts"],
        counts["forest"],
        counts["tree_points"],
    )
    return counts
