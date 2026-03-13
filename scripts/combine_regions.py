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
# Pipeline outputs from elevation and buffer steps (optional per region)
EXTRA_GEOPARQUET_FILES = [
    "ski_area_contours.parquet",
    "ski_area_elevation_points.parquet",
    "ski_areas_1000ft_buffer.parquet",
]
EXTRA_TABULAR_FILES = [
    "ski_areas_elevation.parquet",
]
ALL_FILES = PARQUET_FILES + TABULAR_FILES


def _dir_has_data(d: Path) -> bool:
    """True if dir has any expected parquet or ski_areas_analyzed.csv."""
    for f in ALL_FILES:
        if (d / f).exists():
            return True
    if (d / "ski_areas_analyzed.csv").exists():
        return True
    return False


def discover_regions(output_dir: Path) -> list[str]:
    """Find region subdirs that have at least one expected parquet/csv file.
    Top-level dirs with data (e.g. south-america) -> one region.
    Nested dirs (e.g. asia/japan/hokkaido) -> one region per leaf dir with data.
    """
    regions = []
    if not output_dir.exists():
        return regions
    for d in output_dir.iterdir():
        if not d.is_dir() or d.name == "combined":
            continue
        if _dir_has_data(d):
            regions.append(d.name)
        else:
            for subpath in d.rglob("*"):
                if not subpath.is_dir():
                    continue
                if _dir_has_data(subpath):
                    rel = subpath.relative_to(output_dir)
                    name = str(rel).replace("\\", "/")
                    if name not in regions:
                        regions.append(name)
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


def combine_lifts_geoparquet(region_paths: list[tuple[str, Path]], lifts_path: Path, pylons_stations_path: Path) -> tuple[int, int]:
    """Combine lifts, split aerialway=pylon and aerialway=station into separate file for faster downstream use."""
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
        return 0, 0
    combined = pd.concat(gdfs, ignore_index=True)
    lifts_path.parent.mkdir(parents=True, exist_ok=True)
    # Split pylon and station (infrastructure) into separate file
    if "aerialway" in combined.columns:
        mask = combined["aerialway"].isin(["pylon", "station"])
        main = combined[~mask]
        infra = combined[mask]
    else:
        main = combined
        infra = combined.iloc[0:0] if len(combined) > 0 else combined
    main.to_parquet(lifts_path, index=False)
    if len(infra) > 0:
        infra.to_parquet(pylons_stations_path, index=False)
    return len(main), len(infra)


def combine_tabular(region_paths: list[tuple[str, Path]], out_path: Path) -> int:
    """Read tabular parquet (or CSV fallback for ski_areas_analyzed) from each region, add region column, concatenate, write."""
    dfs = []
    for region, p in region_paths:
        if p.exists():
            df = pd.read_parquet(p)
        else:
            csv_p = p.with_suffix(".csv")
            if csv_p.exists():
                df = pd.read_csv(csv_p)
            else:
                continue
        df["region"] = region
        dfs.append(df)
    if not dfs:
        return 0
    combined = pd.concat(dfs, ignore_index=True)
    # Coerce object columns to string (some regions have int in name etc) for parquet
    for col in combined.columns:
        if combined[col].dtype == object:
            combined[col] = combined[col].apply(lambda x: "" if pd.isna(x) else str(x))
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

    def region_path(region: str, filename: str) -> Path:
        """Path to region's parquet file (region may be 'asia' or 'asia/japan/hokkaido')."""
        parts = region.split("/")
        return output_dir / Path(*parts) / filename

    total_rows = 0
    for filename in PARQUET_FILES:
        if filename == "lifts.parquet":
            paths = [(r, region_path(r, filename)) for r in regions]
            n_main, n_infra = combine_lifts_geoparquet(
                paths, combined_dir / "lifts.parquet", combined_dir / "lifts_pylons_stations.parquet"
            )
            if n_main > 0:
                print(f"  lifts.parquet: {n_main} rows")
                total_rows += n_main
            if n_infra > 0:
                print(f"  lifts_pylons_stations.parquet: {n_infra} rows")
        else:
            paths = [(r, region_path(r, filename)) for r in regions]
            n = combine_geoparquet(paths, combined_dir / filename)
            if n > 0:
                print(f"  {filename}: {n} rows")
                total_rows += n

    for filename in TABULAR_FILES:
        paths = [(r, region_path(r, filename)) for r in regions]
        n = combine_tabular(paths, combined_dir / filename)
        if n > 0:
            print(f"  {filename}: {n} rows")
            total_rows += n

    for filename in EXTRA_GEOPARQUET_FILES:
        paths = [(r, region_path(r, filename)) for r in regions]
        paths = [(r, p) for r, p in paths if p.exists()]
        if not paths:
            continue
        n = combine_geoparquet(paths, combined_dir / filename)
        if n > 0:
            print(f"  {filename}: {n} rows")
            total_rows += n

    for filename in EXTRA_TABULAR_FILES:
        paths = [(r, region_path(r, filename)) for r in regions]
        paths = [(r, p) for r, p in paths if p.exists()]
        if not paths:
            continue
        n = combine_tabular(paths, combined_dir / filename)
        if n > 0:
            print(f"  {filename}: {n} rows")
            total_rows += n

    print(f"Done. Combined output in {combined_dir}/")


if __name__ == "__main__":
    main()
