#!/usr/bin/env python3
"""
Generate sample resort GeoPackage for building the QGIS template.

Runs data_to_qgis with --limit 1, then copies the GeoPackage files to atlas/map_gen/templates/seed/
so you can add those layers in QGIS when creating atlas_resort_template.qgz.

Usage:
  python -m atlas.map_gen.create_template_seed
  python -m atlas.map_gen.create_template_seed --resort 12345 --region europe/iceland
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

# Import from same package
from atlas.map_gen.data_to_qgis import (
    FILE_CONTOURS,
    FILE_OSM_LINES,
    FILE_OSM_POINTS,
    FILE_OSM_POLYGONS,
    FILE_SKI_AREA,
    FILE_SKI_BUFFER,
    load_config,
    run_resorts,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate sample resort data for QGIS template")
    parser.add_argument("--resort", type=str, default=None, help="Single resort winter_sports_id")
    parser.add_argument("--region", type=str, default=None, help="Region for --resort")
    parser.add_argument("--input-dir", type=Path, default=None, help="Override input parquet dir")
    parser.add_argument("--work-dir", type=Path, default=None, help="Override work dir")
    args = parser.parse_args()

    root = _repo_root()
    config = load_config()
    input_dir = args.input_dir or root / config.get("input_dir", "output/combined")
    work_dir = args.work_dir or root / config.get("work_dir", "atlas_work")
    if not input_dir.is_absolute():
        input_dir = root / input_dir
    if not work_dir.is_absolute():
        work_dir = root / work_dir

    count = run_resorts(
        input_dir,
        work_dir,
        config,
        resort_id=args.resort,
        region_filter=args.region,
        all_resorts=args.resort is None,
        limit=1,
    )
    if count == 0:
        print("No resort data generated. Check input_dir and that ski_areas.parquet exists.", file=sys.stderr)
        return 1

    # Find the first resort dir (work_dir/resorts/<region>/<id>/)
    resorts_dir = work_dir / "resorts"
    resort_dirs = list(resorts_dir.rglob("*"))
    resort_dir = None
    for d in resort_dirs:
        if d.is_dir() and (d / FILE_SKI_AREA).exists():
            resort_dir = d
            break

    if not resort_dir:
        print("Resort dir with GeoPackage layers not found.", file=sys.stderr)
        return 1

    seed_dir = root / "atlas" / "map_gen" / "templates" / "seed"
    seed_dir.mkdir(parents=True, exist_ok=True)

    files = [
        FILE_SKI_AREA,
        FILE_SKI_BUFFER,
        FILE_OSM_POLYGONS,
        FILE_OSM_LINES,
        FILE_OSM_POINTS,
        FILE_CONTOURS,
    ]
    for fn in files:
        src = resort_dir / fn
        if src.exists():
            shutil.copy2(src, seed_dir / fn)
            print(f"  Copied {fn}")

    print(f"\nSample data in: {seed_dir}")
    print("Add each .gpkg in QGIS with the exact layer names from templates/README.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
