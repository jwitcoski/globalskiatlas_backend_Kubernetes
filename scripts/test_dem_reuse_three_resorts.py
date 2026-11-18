#!/usr/bin/env python3
"""Local DEM-reuse check on Montage, Greylock, and Wissahickon work dirs.

If the staged scene DEM does not cover the OSM hull (old ski-AOI DEM),
remosaic once into dems/scene/{wid}.tif (as contours would), then assert
the next resolve_dem call reuses it with no remosaic.
"""
from __future__ import annotations

import shutil
import sys
import time
from pathlib import Path
from unittest import mock

import geopandas as gpd
from shapely.ops import unary_union

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from game_export.config import load_resort_config  # noqa: E402
from game_export import inputs as inputs_mod  # noqa: E402

CASES = [
    {
        "yaml": REPO / "config/resorts/montage_mountain_pa.yaml",
        "work": REPO / "output/one_resort_montage",
    },
    {
        "yaml": REPO / "config/resorts/mount_greylock_ski_club_united_states_of_america.yaml",
        "work": REPO / "output/one_resort_greylock",
    },
    {
        "yaml": REPO / "config/resorts/wissahickon_ski_club_united_states_of_america.yaml",
        "work": REPO / "output/one_resort_wissahickon",
    },
]


def _load(work: Path, name: str, wid: str):
    path = work / "combined" / name
    if not path.is_file():
        path = work / name
    if not path.is_file():
        return None
    frame = gpd.read_parquet(path)
    if "winter_sports_id" in frame.columns:
        frame = frame[frame["winter_sports_id"].astype(str) == str(wid)]
    return frame


def _coverage(work: Path, wid: str):
    ski = _load(work, "ski_areas.parquet", wid)
    pistes = _load(work, "pistes.parquet", wid)
    lifts = _load(work, "lifts.parquet", wid)
    if lifts is not None and not lifts.empty:
        lifts = inputs_mod._drop_lift_pylons(lifts)
    cover = inputs_mod.osm_feature_hull(pistes, lifts)
    polygon = ski.geometry.iloc[0] if ski is not None and not ski.empty else None
    if cover is not None and polygon is not None and not polygon.is_empty:
        return unary_union([cover, polygon]).convex_hull
    return cover if cover is not None else polygon


def _best_existing_dem(work: Path, wid: str, region: str) -> Path | None:
    candidates = [
        work / "dems" / "scene" / f"{wid}.tif",
        work / "cache" / "game_export_dems" / Path(*region.split("/")) / f"{wid}-osmhull.tif",
        work / "combined" / "dems" / Path(*region.split("/")) / f"{wid}.tif",
        work / "dems" / Path(*region.split("/")) / f"{wid}.tif",
    ]
    return next((p for p in candidates if p.is_file()), None)


def _stage_scene(work: Path, wid: str, src: Path) -> Path:
    scene = work / "dems" / "scene" / f"{wid}.tif"
    scene.parent.mkdir(parents=True, exist_ok=True)
    if src.resolve() != scene.resolve():
        shutil.copy2(src, scene)
    return scene


def run_case(yaml_path: Path, work: Path) -> dict:
    cfg = load_resort_config(yaml_path)
    wid = cfg.winter_sports_id
    cover = _coverage(work, wid)
    if cover is None or cover.is_empty:
        raise RuntimeError(f"{cfg.resort_id}: empty coverage")

    existing = _best_existing_dem(work, wid, cfg.region)
    remosaic_setup_s = 0.0
    setup_note = "seeded existing DEM"
    if existing is not None and inputs_mod._dem_valid_over_geom(existing, cover):
        scene = _stage_scene(work, wid, existing)
    else:
        # Contours path: produce a hull DEM once, then game_export must reuse it.
        setup_note = "remosaic once into dems/scene (setup)"
        t0 = time.perf_counter()
        path, note = inputs_mod.resolve_dem(
            work,
            cfg,
            cover,
            work / "cache",
            fetch_skadi=True,
            coverage_geom=cover,
        )
        remosaic_setup_s = time.perf_counter() - t0
        scene = _stage_scene(work, wid, path)
        print(f"  setup remosaic {remosaic_setup_s:.1f}s -> {scene} ({note})", flush=True)

    if not inputs_mod._dem_valid_over_geom(scene, cover):
        raise RuntimeError(f"{cfg.resort_id}: scene DEM still does not cover hull")

    mosaic = mock.Mock(side_effect=AssertionError("remosaic not allowed"))
    t0 = time.perf_counter()
    with mock.patch.object(inputs_mod, "_mosaic_from_skadi_cache", mosaic):
        path, note = inputs_mod.resolve_dem(
            work,
            cfg,
            cover,
            work / "cache",
            fetch_skadi=True,
            coverage_geom=cover,
        )
    elapsed = time.perf_counter() - t0
    mosaic.assert_not_called()
    if path.resolve() != scene.resolve() and "GeoTIFF covering OSM hull" not in note:
        raise AssertionError(f"expected scene reuse, got path={path} note={note}")
    return {
        "resort": cfg.display_name,
        "wid": wid,
        "setup": setup_note,
        "setup_seconds": round(remosaic_setup_s, 3),
        "reuse_seconds": round(elapsed, 3),
        "dem_path": str(path),
        "note": note,
    }


def main() -> int:
    rows = []
    failed = 0
    for case in CASES:
        name = case["yaml"].stem
        print(f"\n=== {name} ===", flush=True)
        try:
            row = run_case(case["yaml"], case["work"])
            rows.append(row)
            print(
                f"PASS {row['resort']} wid={row['wid']} "
                f"reuse={row['reuse_seconds']}s setup={row['setup']} "
                f"({row['setup_seconds']}s)\n"
                f"  dem={row['dem_path']}\n  note={row['note']}",
                flush=True,
            )
        except Exception as exc:
            failed += 1
            print(f"FAIL {name}: {exc}", flush=True)
    print(f"\n{len(rows)} passed, {failed} failed")
    return 0 if failed == 0 and len(rows) == 3 else 1


if __name__ == "__main__":
    raise SystemExit(main())
