#!/usr/bin/env python3
"""Unit tests for export-time OSM stripdown (no DEM / S3 required)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import geopandas as gpd
from shapely.geometry import LineString, Point, box

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from game_export.osm_stripdown import (  # noqa: E402
    hard_clip_gdf,
    is_keep_piste,
    prefer_line_piste_features,
    prefer_line_pistes,
    strip_for_export,
)


def _piste_gdf(rows):
    return gpd.GeoDataFrame(rows, crs="EPSG:4326")


def test_keep_downhill_and_snowpark_drop_xc_sled():
    assert is_keep_piste({"piste:type": "downhill", "name": "Outer Limits"})
    assert is_keep_piste({"piste_type": "snowpark", "name": "Park"})
    assert is_keep_piste({"tags": {"piste:type": "alpine"}})
    assert not is_keep_piste({"piste:type": "nordic", "name": "Tour"})
    assert not is_keep_piste({"piste:type": "sled", "name": "Tube"})
    assert not is_keep_piste({"piste:type": "downhill", "name": "Catamount Trail XC"})
    assert not is_keep_piste({"piste:type": "skitour"})


def test_prefer_line_over_overlapping_polygon():
    line = LineString([(-72.80, 43.62), (-72.80, 43.60)])
    poly = box(-72.801, 43.60, -72.799, 43.62)
    gdf = _piste_gdf(
        [
            {
                "name": "Superstar",
                "tags": json.dumps({"piste:type": "downhill"}),
                "geometry": line,
            },
            {
                "name": "Superstar",
                "tags": json.dumps({"piste:type": "downhill"}),
                "geometry": poly,
            },
            {
                "name": "Only Poly",
                "tags": json.dumps({"piste:type": "downhill"}),
                "geometry": box(-72.79, 43.60, -72.788, 43.61),
            },
        ]
    )
    out = prefer_line_pistes(gdf)
    assert len(out) == 2
    assert sum(1 for g in out.geometry if g.geom_type == "LineString") == 1
    assert sum(1 for g in out.geometry if g.geom_type == "Polygon") == 1
    assert set(out["name"]) == {"Superstar", "Only Poly"}


def test_prefer_line_features_list():
    feats = [
        {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": [[0, 0], [0, 100]]},
            "properties": {"name": "A"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-1, 0], [1, 0], [1, 100], [-1, 100], [-1, 0]]],
            },
            "properties": {"name": "A"},
        },
    ]
    out = prefer_line_piste_features(feats)
    assert len(out) == 1
    assert out[0]["geometry"]["type"] == "LineString"


def test_hard_clip_drops_outside_and_clips_crossing():
    aoi = box(-72.81, 43.60, -72.79, 43.62)
    inside = LineString([(-72.805, 43.615), (-72.800, 43.605)])
    outside = LineString([(-72.90, 43.70), (-72.89, 43.71)])
    crossing = LineString([(-72.805, 43.615), (-72.70, 43.615)])
    gdf = _piste_gdf(
        [
            {"name": "in", "geometry": inside},
            {"name": "out", "geometry": outside},
            {"name": "cross", "geometry": crossing},
        ]
    )
    out = hard_clip_gdf(gdf, aoi, pad_m=50)
    names = set(out["name"])
    assert "in" in names
    assert "out" not in names
    assert "cross" in names
    cross = out.loc[out["name"] == "cross"].geometry.iloc[0]
    assert cross.bounds[2] <= aoi.buffer(0.01).bounds[2] + 1e-6


def test_strip_for_export_drops_roads_points_xc():
    aoi = box(-72.81, 43.60, -72.79, 43.62)
    osm = _piste_gdf(
        [
            {
                "tags": json.dumps({"highway": "residential"}),
                "geometry": LineString([(-72.805, 43.61), (-72.800, 43.61)]),
            },
            {
                "tags": json.dumps({"landuse": "forest", "natural": "wood"}),
                "geometry": box(-72.808, 43.605, -72.802, 43.612),
            },
            {
                "tags": json.dumps({"natural": "tree"}),
                "geometry": Point(-72.804, 43.608),
            },
            {
                "tags": json.dumps({"boundary": "administrative", "admin_level": "8"}),
                "geometry": box(-72.82, 43.59, -72.78, 43.63),
            },
            {
                "tags": json.dumps({"natural": "bare_rock"}),
                "geometry": box(-72.806, 43.618, -72.804, 43.619),
            },
        ]
    )
    pistes = _piste_gdf(
        [
            {
                "name": "DH",
                "tags": json.dumps({"piste:type": "downhill"}),
                "geometry": LineString([(-72.805, 43.618), (-72.805, 43.602)]),
            },
            {
                "name": "XC Loop",
                "tags": json.dumps({"piste:type": "nordic"}),
                "geometry": LineString([(-72.803, 43.618), (-72.803, 43.602)]),
            },
            {
                "name": "Sled",
                "tags": json.dumps({"piste:type": "sled"}),
                "geometry": LineString([(-72.801, 43.618), (-72.801, 43.602)]),
            },
        ]
    )
    lifts = _piste_gdf(
        [
            {
                "tags": json.dumps({"aerialway": "chair_lift"}),
                "geometry": LineString([(-72.804, 43.602), (-72.804, 43.618)]),
            },
            {
                "tags": json.dumps({"aerialway": "pylon"}),
                "geometry": Point(-72.804, 43.610),
            },
        ]
    )
    osm2, pistes2, lifts2 = strip_for_export(osm, pistes, lifts, aoi, mode="game")
    assert len(pistes2) == 1
    assert pistes2.iloc[0]["name"] == "DH"
    assert len(lifts2) == 1
    # forest + bare_rock kept; road/admin/tree-point dropped
    assert len(osm2) == 2
    tags = [json.loads(t) if isinstance(t, str) else t for t in osm2["tags"]]
    naturals = {t.get("natural") or t.get("landuse") for t in tags}
    assert "wood" in naturals or "forest" in naturals
    assert "bare_rock" in naturals


def main() -> int:
    test_keep_downhill_and_snowpark_drop_xc_sled()
    test_prefer_line_over_overlapping_polygon()
    test_prefer_line_features_list()
    test_hard_clip_drops_outside_and_clips_crossing()
    test_strip_for_export_drops_roads_points_xc()
    print("ok: osm_stripdown tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
