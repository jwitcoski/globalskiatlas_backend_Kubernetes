#!/usr/bin/env python3
"""Smoke test for game_export using a tiny synthetic Montage-like fixture.

Does not require Pennsylvania pipeline outputs.
Run: python scripts/test_game_export_smoke.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
import yaml
from rasterio.transform import from_bounds
from shapely.geometry import LineString, Polygon, box

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from game_export.cli import main as game_export_main  # noqa: E402


def _write_dem(path: Path) -> None:
    # ~400 m x 400 m around Montage
    west, south, east, north = -75.668, 41.348, -75.662, 41.353
    w, h = 80, 80
    transform = from_bounds(west, south, east, north, w, h)
    # Higher in the north (row 0)
    rows = np.linspace(480, 320, h, dtype=np.float32)
    dem = np.repeat(rows[:, None], w, axis=1)
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="int16",
        crs="EPSG:4326",
        transform=transform,
        nodata=-32768,
    ) as dst:
        dst.write(dem.astype(np.int16), 1)


def _write_ski(path: Path, wid: str) -> None:
    poly = box(-75.6675, 41.3485, -75.6625, 41.3525)
    gdf = gpd.GeoDataFrame(
        [{"winter_sports_id": wid, "name": "Montage Mountain", "region": "north-america/us/pennsylvania", "geometry": poly}],
        crs="EPSG:4326",
    )
    gdf.to_parquet(path, index=False)


def _write_vectors(region_dir: Path, wid: str) -> None:
    piste = LineString([(-75.665, 41.3522), (-75.665, 41.3490)])
    lift = LineString([(-75.6642, 41.3492), (-75.6642, 41.3520)])
    bldg = box(-75.666, 41.3495, -75.6657, 41.3498)
    tags_p = json.dumps({"piste:type": "downhill", "name": "Smoke Run"})
    tags_l = json.dumps({"aerialway": "chair_lift", "name": "Smoke Lift"})
    tags_b = json.dumps({"building": "yes"})
    gpd.GeoDataFrame(
        [{"osm_type": "way", "osm_id": 1, "winter_sports_id": wid, "tags": tags_p, "geometry": piste}],
        crs="EPSG:4326",
    ).to_parquet(region_dir / "pistes.parquet", index=False)
    gpd.GeoDataFrame(
        [{"osm_type": "way", "osm_id": 2, "winter_sports_id": wid, "tags": tags_l, "geometry": lift}],
        crs="EPSG:4326",
    ).to_parquet(region_dir / "lifts.parquet", index=False)
    gpd.GeoDataFrame(
        [
            {"osm_type": "way", "osm_id": 1, "winter_sports_id": wid, "tags": tags_p, "geometry": piste},
            {"osm_type": "way", "osm_id": 2, "winter_sports_id": wid, "tags": tags_l, "geometry": lift},
            {"osm_type": "way", "osm_id": 3, "winter_sports_id": wid, "tags": tags_b, "geometry": bldg},
        ],
        crs="EPSG:4326",
    ).to_parquet(region_dir / "osm_near_winter_sports.parquet", index=False)


def main() -> int:
    wid = "45096232"
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        region = root / "output" / "north-america" / "us" / "pennsylvania"
        region.mkdir(parents=True)
        _write_ski(region / "ski_areas.parquet", wid)
        _write_vectors(region, wid)
        dem = region / "dems" / f"{wid}.tif"
        _write_dem(dem)
        cfg = yaml.safe_load((REPO / "config" / "resorts" / "montage_mountain_pa.yaml").read_text())
        cfg["route_min_vertical_drop_m"] = 10
        cfg["route_min_length_m"] = 40
        cfg["heightfield_resolution_m"] = 8
        cfg["collision_heightfield_resolution_m"] = 16
        cfg["terrain_mesh_resolution_m"] = 16
        cfg_path = root / "resort.yaml"
        cfg_path.write_text(yaml.dump(cfg), encoding="utf-8")
        code = game_export_main(
            [
                "--config",
                str(cfg_path),
                "--data-root",
                str(root / "output"),
                "--cache-dir",
                str(root / "cache"),
                "--out-root",
                str(root / "output" / "game_scenes"),
                "--force",
            ]
        )
        if code != 0:
            print("export failed", code, file=sys.stderr)
            return code
        scenes = list((root / "output" / "game_scenes" / "montage_mountain_pa").iterdir())
        assert scenes, "no scene dir"
        scene = scenes[0]
        manifest_path = scene / "scene-manifest.json"
        assert manifest_path.is_file()
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        required = [
            scene / "terrain" / "heightfield-u16.bin",
            scene / "terrain" / "heightfield-metadata.json",
            scene / "terrain" / "collision-heightfield-u16.bin",
            scene / "terrain" / "terrain-mesh.glb",
            scene / "qa" / "validation-report.json",
            scene / "qa" / "validation-report.md",
            scene / "qa" / "overview-hillshade-pistes.png",
            scene / "qa" / "overview-slope-routes.png",
            scene / "attribution" / "ATTRIBUTION.md",
            scene / "vectors" / "pistes.geojson",
            scene / "vectors" / "exclusion-zones.geojson",
            scene / "gameplay" / "routes.graph.json",
        ]
        missing = [str(p) for p in required if not p.is_file()]
        if missing:
            print("missing files:", missing, file=sys.stderr)
            return 1
        hf = (scene / "terrain" / "heightfield-u16.bin").read_bytes()
        if len(hf) < 16:
            print("empty heightfield", file=sys.stderr)
            return 1
        json.loads((scene / "qa" / "validation-report.json").read_text(encoding="utf-8"))
        json.loads((scene / "gameplay" / "routes.graph.json").read_text(encoding="utf-8"))
        glb = (scene / "terrain" / "terrain-mesh.glb").read_bytes()
        if glb[:4] != b"glTF":
            print("invalid glb", file=sys.stderr)
            return 1
        print("SMOKE OK", scene)
        print("manifest keys", sorted(manifest.keys())[:8], "...")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
