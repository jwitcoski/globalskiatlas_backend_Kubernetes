#!/usr/bin/env python3
"""
Produce a 1000 ft (305 m) buffer outline around each ski area polygon for production maps.

Reads ski_areas.parquet, buffers each polygon by 305 m, writes:
  - ski_areas_1000ft_buffer.geojson
  - ski_areas_1000ft_buffer.parquet

Use for map clipping, outline layer, or "area of interest" boundary. Same 305 m radius
as OSM nearby extraction and contour extent.

Usage:
  python scripts/ski_area_1000ft_buffer.py -i output/europe/iceland/ski_areas.parquet -o output/europe/iceland
  python scripts/ski_area_1000ft_buffer.py -d output/europe/iceland
"""
import argparse
import sys
from pathlib import Path
from typing import Any

import geopandas as gpd

# 305 m ≈ 1000 ft; matches extract_nearby_from_pbf and ski_area_elevation_contours
BUFFER_METERS = 305


def _buffer_geom_meters(geom: Any, meters: float) -> Any:
    """Buffer geometry by meters (uses local UTM for accuracy)."""
    from shapely.ops import transform
    if geom is None or geom.is_empty:
        return geom
    try:
        import pyproj
    except ImportError:
        deg = meters / 111320.0
        return geom.buffer(deg)
    centroid = geom.centroid
    lon, lat = centroid.x, centroid.y
    utm_zone = int((lon + 180) / 6) + 1
    hem = "north" if lat >= 0 else "south"
    crs = f"+proj=utm +zone={utm_zone} +{hem} +datum=WGS84 +units=m"
    proj = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    inv = pyproj.Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    geom_proj = transform(proj.transform, geom)
    buffered = geom_proj.buffer(meters)
    return transform(inv.transform, buffered)


def main() -> None:
    p = argparse.ArgumentParser(description="1000 ft buffer outline around ski areas for production maps")
    p.add_argument("-i", "--input", type=str, help="ski_areas.parquet path")
    p.add_argument("-o", "--output-dir", type=str, help="Output directory (default: same as input)")
    p.add_argument("-d", "--data-dir", type=str, help="Data dir: use <data-dir>/ski_areas.parquet and write to <data-dir>")
    p.add_argument("-m", "--meters", type=float, default=BUFFER_METERS, help=f"Buffer distance in meters (default: {BUFFER_METERS} = 1000 ft)")
    args = p.parse_args()

    if args.data_dir:
        data_dir = Path(args.data_dir)
        input_path = data_dir / "ski_areas.parquet"
        output_dir = data_dir
    elif args.input:
        input_path = Path(args.input)
        output_dir = Path(args.output_dir) if args.output_dir else input_path.parent
    else:
        p.error("Use -i/--input or -d/--data-dir")

    if not input_path.exists():
        # Write empty buffer outputs so pipeline continues (e.g. after failed geojson->parquet)
        output_dir.mkdir(parents=True, exist_ok=True)
        gdf_empty = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        geojson_path = output_dir / "ski_areas_1000ft_buffer.geojson"
        parquet_path = output_dir / "ski_areas_1000ft_buffer.parquet"
        gdf_empty.to_file(geojson_path, driver="GeoJSON")
        gdf_empty.to_parquet(parquet_path, index=False)
        print(f"No ski_areas.parquet; wrote empty buffer outputs.", file=sys.stderr)
        sys.exit(0)
    output_dir.mkdir(parents=True, exist_ok=True)

    gdf = gpd.read_parquet(input_path)
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    if not gdf.crs:
        gdf.set_crs("EPSG:4326", inplace=True)

    # Keep properties; buffer geometry
    cols = [c for c in gdf.columns if c != "geometry"]
    buffered_geoms = []
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty or geom.geom_type == "Point":
            buffered_geoms.append(geom)
            continue
        buffered_geoms.append(_buffer_geom_meters(geom, args.meters))

    out = gpd.GeoDataFrame(gdf[cols], geometry=buffered_geoms, crs=gdf.crs)

    geojson_path = output_dir / "ski_areas_1000ft_buffer.geojson"
    parquet_path = output_dir / "ski_areas_1000ft_buffer.parquet"
    out.to_file(geojson_path, driver="GeoJSON")
    out.to_parquet(parquet_path, index=False)
    print(f"Wrote {geojson_path} ({len(out)} features)")
    print(f"Wrote {parquet_path}")


if __name__ == "__main__":
    main()
