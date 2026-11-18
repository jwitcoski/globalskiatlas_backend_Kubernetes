#!/usr/bin/env python3
"""Prove resolve_dem reuses dems/scene hull GeoTIFF instead of remosaicking."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from unittest import mock

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from shapely.geometry import box

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from game_export.config import load_resort_config  # noqa: E402
from game_export import inputs as inputs_mod  # noqa: E402


def _write_dem(path: Path, west: float, south: float, east: float, north: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    width, height = 32, 32
    data = np.full((height, width), 500, dtype=np.float32)
    transform = from_bounds(west, south, east, north, width, height)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=-32768,
    ) as dst:
        dst.write(data, 1)


def test_resolve_dem_reuses_scene_hull(tmp_path: Path | None = None) -> None:
    root = Path(tmp_path) if tmp_path else Path(tempfile.mkdtemp(prefix="dem_reuse_"))
    cfg = load_resort_config(REPO / "config" / "resorts" / "montage_mountain_pa.yaml")
    wid = cfg.winter_sports_id
    # Coverage geom inside the DEM
    west, south, east, north = -75.70, 41.32, -75.60, 41.40
    cover = box(west + 0.02, south + 0.02, east - 0.02, north - 0.02)
    scene = root / "dems" / "scene" / f"{wid}.tif"
    _write_dem(scene, west, south, east, north)

    cache = root / "cache"
    mosaic = mock.Mock(side_effect=AssertionError("should not remosaic when scene DEM covers hull"))

    with mock.patch.object(inputs_mod, "_mosaic_from_skadi_cache", mosaic):
        path, note = inputs_mod.resolve_dem(
            root,
            cfg,
            cover,
            cache,
            fetch_skadi=True,
            coverage_geom=cover,
        )

    assert path.resolve() == scene.resolve(), path
    assert "GeoTIFF covering OSM hull" in note
    mosaic.assert_not_called()
    print("ok: resolve_dem reused", path)


def test_write_osm_hull_copies_game_export_cache() -> None:
    """_write_osm_hull_dem should seed cache/game_export_dems/...-osmhull.tif."""
    import geopandas as gpd
    from shapely.geometry import LineString, Polygon

    sys.path.insert(0, str(REPO / "scripts"))
    import run_one_resort as ror

    with tempfile.TemporaryDirectory(prefix="dem_hull_") as raw:
        work = Path(raw)
        wid = "45096232"
        region = "north-america/us/pennsylvania"
        ski = gpd.GeoDataFrame(
            {"winter_sports_id": [wid]},
            geometry=[Polygon([(-75.68, 41.34), (-75.62, 41.34), (-75.62, 41.38), (-75.68, 41.38)])],
            crs="EPSG:4326",
        )
        piste = gpd.GeoDataFrame(
            {"winter_sports_id": [wid]},
            geometry=[LineString([(-75.67, 41.35), (-75.63, 41.37)])],
            crs="EPSG:4326",
        )
        ski.to_parquet(work / "ski_areas.parquet")
        piste.to_parquet(work / "pistes.parquet")

        def fake_mosaic(geom, cache_dir, fetch):
            # Return a tiny DEM grid for the geom bounds
            minx, miny, maxx, maxy = geom.bounds
            arr = np.full((16, 16), 400, dtype=np.int16)
            return arr, minx - 0.01, miny - 0.01, maxx + 0.01, maxy + 0.01

        # Imported inside _write_osm_hull_dem from game_export.inputs
        with mock.patch("game_export.inputs._mosaic_from_skadi_cache", side_effect=fake_mosaic):
            dest = ror._write_osm_hull_dem(work, wid, region)

        assert dest.is_file()
        hull = work / "cache" / "game_export_dems" / "north-america" / "us" / "pennsylvania" / f"{wid}-osmhull.tif"
        assert hull.is_file(), hull
        print("ok: hull cache seeded at", hull)


if __name__ == "__main__":
    test_resolve_dem_reuses_scene_hull()
    test_write_osm_hull_copies_game_export_cache()
    print("check ok")
