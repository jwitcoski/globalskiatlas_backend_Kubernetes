#!/usr/bin/env python3
"""
Export combined parquet layers to GeoJSON for MapTiler, ArcGIS, or other web map use.

Reads from output/combined/*.parquet and writes to output/combinedmaptiler/*.geojson.
- lifts.parquet -> lifts.geojson
- pistes.parquet -> pistes.geojson
- ski_areas.parquet -> ski_areas.geojson
- ski_areas_analyzed.parquet -> ski_areas_analyzed.geojson (point geometry from centroid_lat/lon)
"""
import argparse
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


def export_geoparquet_to_geojson(parquet_path: Path, geojson_path: Path) -> int:
    """Export a GeoParquet file to GeoJSON. Returns feature count."""
    gdf = gpd.read_parquet(parquet_path)
    if not gdf.crs or gdf.crs.to_epsg() != 4326:
        if gdf.crs:
            gdf = gdf.to_crs("EPSG:4326")
        else:
            gdf.set_crs("EPSG:4326", inplace=True)
    geojson_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(geojson_path, driver="GeoJSON")
    return len(gdf)


def export_ski_areas_analyzed_to_geojson(parquet_path: Path, geojson_path: Path) -> int:
    """Export ski_areas_analyzed.parquet (tabular) to GeoJSON using centroid_lat/lon as point geometry."""
    df = pd.read_parquet(parquet_path)
    if "centroid_lon" not in df.columns or "centroid_lat" not in df.columns:
        print(f"  ski_areas_analyzed: no centroid columns, skipping GeoJSON", file=sys.stderr)
        return 0
    # Drop rows with missing centroid for geometry
    valid = df["centroid_lon"].notna() & df["centroid_lat"].notna()
    df = df[valid].copy()
    geometry = [Point(lon, lat) for lon, lat in zip(df["centroid_lon"], df["centroid_lat"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    geojson_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(geojson_path, driver="GeoJSON")
    return len(gdf)


def main():
    parser = argparse.ArgumentParser(description="Export combined parquet to GeoJSON for MapTiler, ArcGIS, or other web map use")
    parser.add_argument(
        "-i", "--input-dir",
        default="output/combined",
        help="Input directory with parquet files (default: output/combined)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="output/combinedmaptiler",
        help="Output directory for GeoJSON files (default: output/combinedmaptiler)",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists():
        print(f"Input directory not found: {input_dir}", file=sys.stderr)
        sys.exit(1)

    layers = [
        ("lifts.parquet", "lifts.geojson", export_geoparquet_to_geojson),
        ("pistes.parquet", "pistes.geojson", export_geoparquet_to_geojson),
        ("ski_areas.parquet", "ski_areas.geojson", export_geoparquet_to_geojson),
        ("ski_areas_analyzed.parquet", "ski_areas_analyzed.geojson", export_ski_areas_analyzed_to_geojson),
    ]

    for parquet_name, geojson_name, export_fn in layers:
        src = input_dir / parquet_name
        dst = output_dir / geojson_name
        if not src.exists():
            print(f"  Skipping {parquet_name} (not found)")
            continue
        try:
            n = export_fn(src, dst)
            print(f"  {geojson_name}: {n} features")
        except Exception as e:
            print(f"  {parquet_name} -> {geojson_name}: {e}", file=sys.stderr)

    print(f"Done. GeoJSON files in {output_dir}/")


if __name__ == "__main__":
    main()
