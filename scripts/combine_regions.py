#!/usr/bin/env python3
"""
Combine regional pipeline outputs into a single global dataset.
Reads output/<region>/*.parquet for each region, adds a 'region' column,
concatenates, and writes to output/combined/.

Usage:
  python scripts/combine_regions.py [--output-dir output] [--regions iceland south-america africa ...]
  # Or: python scripts/combine_regions.py   # auto-discovers regions from output/
"""
import argparse
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd


PARQUET_FILES = [
    "ski_areas.parquet",
    "lifts.parquet",
    "pistes.parquet",
    "osm_near_winter_sports.parquet",
]
TABULAR_FILES = [
    "ski_areas_analyzed.parquet",
]
ALL_FILES = PARQUET_FILES + TABULAR_FILES


def discover_regions(output_dir: Path) -> list[str]:
    """Find region subdirs that have at least one expected parquet file."""
    regions = []
    if not output_dir.exists():
        return regions
    for d in output_dir.iterdir():
        if d.is_dir() and d.name != "combined":
            has_data = any((d / f).exists() for f in ALL_FILES)
            if has_data:
                regions.append(d.name)
    return sorted(regions)


def combine_geoparquet(region_paths: list[tuple[str, Path]], out_path: Path) -> int:
    """Read geoparquet from each region, add region column, concatenate, write."""
    gdfs = []
    for region, p in region_paths:
        if not p.exists():
            continue
        gdf = gpd.read_parquet(p)
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")
        elif not gdf.crs:
            gdf.set_crs("EPSG:4326", inplace=True)
        gdf["region"] = region
        gdfs.append(gdf)
    if not gdfs:
        return 0
    combined = pd.concat(gdfs, ignore_index=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(out_path, index=False)
    return len(combined)


def combine_tabular(region_paths: list[tuple[str, Path]], out_path: Path) -> int:
    """Read tabular parquet from each region, add region column, concatenate, write."""
    dfs = []
    for region, p in region_paths:
        if not p.exists():
            continue
        df = pd.read_parquet(p)
        df["region"] = region
        dfs.append(df)
    if not dfs:
        return 0
    combined = pd.concat(dfs, ignore_index=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(out_path, index=False)
    return len(combined)


def main():
    parser = argparse.ArgumentParser(
        description="Combine regional pipeline outputs into output/combined/"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="output",
        help="Base output directory containing region subfolders (default: output)",
    )
    parser.add_argument(
        "-r", "--regions",
        nargs="*",
        help="Regions to combine (default: auto-discover from output dir)",
    )
    parser.add_argument(
        "--combined-dir",
        default=None,
        help="Where to write combined files (default: <output-dir>/combined)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    combined_dir = Path(args.combined_dir) if args.combined_dir else output_dir / "combined"

    regions = args.regions if args.regions else discover_regions(output_dir)
    if not regions:
        print("No regions found. Run pipeline for at least one region first.", file=sys.stderr)
        print(f"  Example: docker compose -f docker-compose.south-america.yml up", file=sys.stderr)
        sys.exit(1)

    print(f"Combining {len(regions)} region(s): {', '.join(regions)}")
    print(f"Output: {combined_dir}/")

    total_rows = 0
    for filename in PARQUET_FILES:
        paths = [(r, output_dir / r / filename) for r in regions]
        n = combine_geoparquet(paths, combined_dir / filename)
        if n > 0:
            print(f"  {filename}: {n} rows")
            total_rows += n

    for filename in TABULAR_FILES:
        paths = [(r, output_dir / r / filename) for r in regions]
        n = combine_tabular(paths, combined_dir / filename)
        if n > 0:
            print(f"  {filename}: {n} rows")
            total_rows += n

    print(f"Done. Combined output in {combined_dir}/")


if __name__ == "__main__":
    main()
