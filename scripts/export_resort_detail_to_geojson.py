#!/usr/bin/env python3
"""
Export combined resort-context GeoParquet layers to GeoJSON for tippecanoe / PMTiles.

Reads from output/combined/ (or --input-dir):
  - osm_near_winter_sports.parquet
  - ski_areas_1000ft_buffer.parquet
  - ski_area_contours.parquet

Writes to --output-dir (default: output/pmtiles_staging/resort).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd


def _ensure_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if not gdf.crs or gdf.crs.to_epsg() != 4326:
        if gdf.crs:
            return gdf.to_crs("EPSG:4326")
        gdf = gdf.copy()
        gdf.set_crs("EPSG:4326", inplace=True)
    return gdf


def export_geoparquet_to_geojson(parquet_path: Path, geojson_path: Path) -> int:
    """Export a GeoParquet file to GeoJSON. Returns feature count."""
    gdf = gpd.read_parquet(parquet_path)
    gdf = _ensure_wgs84(gdf)
    geojson_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(geojson_path, driver="GeoJSON")
    return len(gdf)


def export_osm_near_to_geojson(
    parquet_path: Path,
    geojson_path: Path,
    *,
    strip_tags: bool = False,
    truncate_tags: int | None = None,
) -> int:
    """Export OSM nearby GeoParquet; optionally drop or truncate the tags column (large JSON strings)."""
    gdf = gpd.read_parquet(parquet_path)
    gdf = _ensure_wgs84(gdf)
    if strip_tags and "tags" in gdf.columns:
        gdf = gdf.drop(columns=["tags"])
    elif truncate_tags is not None and truncate_tags > 0 and "tags" in gdf.columns:
        gdf = gdf.copy()

        def _trunc(val):
            if val is None or pd.isna(val):
                return val
            s = str(val)
            if len(s) <= truncate_tags:
                return s
            return s[: truncate_tags - 3] + "..."

        gdf["tags"] = gdf["tags"].map(_trunc)
    geojson_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(geojson_path, driver="GeoJSON")
    return len(gdf)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export resort-detail GeoParquet layers to GeoJSON for PMTiles builds",
    )
    parser.add_argument(
        "-i",
        "--input-dir",
        type=Path,
        default=Path("output/combined"),
        help="Directory with combined parquet files (default: output/combined)",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("output/pmtiles_staging/resort"),
        help="Output directory for GeoJSON (default: output/pmtiles_staging/resort)",
    )
    parser.add_argument(
        "--strip-osm-tags",
        action="store_true",
        help="Drop the tags column from osm_near_winter_sports to reduce tile property size",
    )
    parser.add_argument(
        "--truncate-osm-tags",
        type=int,
        default=None,
        metavar="N",
        help="Truncate OSM tags string to N characters (ignored if --strip-osm-tags)",
    )
    args = parser.parse_args()
    input_dir: Path = args.input_dir
    output_dir: Path = args.output_dir

    if not input_dir.exists():
        print(f"Input directory not found: {input_dir}", file=sys.stderr)
        sys.exit(1)

    jobs: list[tuple[str, str, str]] = [
        ("osm_near_winter_sports.parquet", "osm_near_winter_sports.geojson", "osm"),
        ("ski_areas_1000ft_buffer.parquet", "ski_areas_1000ft_buffer.geojson", "buffer"),
        ("ski_area_contours.parquet", "ski_area_contours.geojson", "contours"),
    ]

    for parquet_name, geojson_name, kind in jobs:
        geojson_path = output_dir / geojson_name
        src = input_dir / parquet_name
        if not src.exists():
            print(f"  Skipping {parquet_name} (not found)")
            continue
        try:
            if kind == "osm":
                n = export_osm_near_to_geojson(
                    src,
                    geojson_path,
                    strip_tags=args.strip_osm_tags,
                    truncate_tags=args.truncate_osm_tags,
                )
            else:
                n = export_geoparquet_to_geojson(src, geojson_path)
            print(f"  {kind}: {geojson_path.name} ({n} features)")
        except Exception as e:
            print(f"  {parquet_name}: {e}", file=sys.stderr)

    print(f"Done. GeoJSON files in {output_dir}/")


if __name__ == "__main__":
    main()
