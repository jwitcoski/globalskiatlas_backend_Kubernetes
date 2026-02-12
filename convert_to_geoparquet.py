#!/usr/bin/env python3
"""
Convert ski area data to GeoParquet format.
Requires: geopandas, shapely, pyarrow (see requirements.txt)

Commands:
  ski  - ski_areas (analyzed + winter_sports JSON) -> ski_areas.parquet
  osm  - osm_near_winter_sports.json -> osm_near_winter_sports.parquet
  all  - all pipeline outputs in a data dir: ski_areas.geojson, lifts.geojson,
         pistes.geojson -> .parquet; ski_areas_analyzed.csv -> .parquet
         (Run after enrich + analyze so GeoJSON/CSV exist.)
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon, LineString


def _get_centroid(element: dict) -> Optional[tuple]:
    """Get (lon, lat) centroid from bounds or geometry."""
    bounds = element.get("bounds")
    if bounds:
        lat = (bounds["minlat"] + bounds["maxlat"]) / 2
        lon = (bounds["minlon"] + bounds["maxlon"]) / 2
        return (lon, lat)
    geom = element.get("geometry")
    if geom and len(geom) > 0:
        lats = [p["lat"] for p in geom]
        lons = [p["lon"] for p in geom]
        return (sum(lons) / len(lons), sum(lats) / len(lats))
    return None


def _node_map_from_elements(elements: List[dict]) -> Dict[int, dict]:
    """Build node_id -> {lat, lon} from elements (nodes and ways' refs not used here)."""
    out = {}
    for e in elements:
        if e.get("type") == "node" and "id" in e and "lat" in e and "lon" in e:
            out[e["id"]] = {"lat": e["lat"], "lon": e["lon"]}
    return out


def _resolve_way_geometry_from_nodes(elements: List[dict], node_map: Dict[int, dict]) -> None:
    """
    In-place: add 'geometry' to way elements that have 'nodes' when all nodes
    are in node_map. Ways with missing nodes (e.g. outside regional extract) are left
    without geometry so they are skipped later. Avoids Overpass 'out geom' which
    prints 'node xxx used in way yyy not found' for every missing node.
    """
    for e in elements:
        if e.get("type") != "way":
            continue
        if e.get("geometry"):
            continue
        refs = e.get("nodes")
        if not refs or len(refs) < 2:
            continue
        geom = []
        for nid in refs:
            if nid not in node_map:
                geom = []
                break
            geom.append(dict(node_map[nid]))
        if len(geom) >= 2:
            e["geometry"] = geom


def _geom_to_shapely(elem: dict) -> Optional[Any]:
    """Convert OSM element to Shapely geometry."""
    if elem.get("type") == "node":
        if "lat" in elem and "lon" in elem:
            return Point(elem["lon"], elem["lat"])
        return None
    if elem.get("type") == "way":
        geom = elem.get("geometry")
        if not geom or len(geom) < 2:
            return None
        coords = [(p["lon"], p["lat"]) for p in geom]
        if len(geom) >= 3 and coords[0] == coords[-1]:
            return Polygon(coords)
        return LineString(coords)
    return None


def ski_areas_to_geoparquet(
    analyzed_path: str = "ski_areas_analyzed.json",
    winter_sports_path: str = "winter_sports_test.json",
    output_path: str = "ski_areas.parquet",
) -> Path:
    """Convert ski areas (analyzed + geometry) to GeoParquet."""
    analyzed_path = Path(analyzed_path)
    winter_sports_path = Path(winter_sports_path)
    output_path = Path(output_path)

    print(f"Loading {analyzed_path}...")
    analyzed = json.loads(analyzed_path.read_text(encoding="utf-8"))

    print(f"Loading {winter_sports_path}...")
    ws_data = json.loads(winter_sports_path.read_text(encoding="utf-8"))

    ws_by_id = {}
    for elem in ws_data.get("elements", []):
        if elem.get("type") in ("way", "relation"):
            ws_by_id[(elem["type"], elem["id"])] = elem

    rows = []
    for rec in analyzed:
        ws_id = rec["winter_sports_id"]
        ws_type = rec["winter_sports_type"]
        ws = ws_by_id.get((ws_type, ws_id))
        centroid = _get_centroid(ws) if ws else None
        if not centroid:
            continue
        rows.append({
            **rec,
            "geometry": Point(centroid[0], centroid[1]),
        })

    gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
    gdf.to_parquet(output_path, index=False)
    print(f"Saved {len(gdf)} ski areas to {output_path}")
    return output_path


def _osm_elements_to_rows(elements: List[dict], limit: Optional[int] = None) -> List[dict]:
    """Convert OSM elements to GeoParquet row dicts (shared logic)."""
    if limit:
        elements = elements[:limit]
    rows = []
    for elem in elements:
        geom = _geom_to_shapely(elem)
        if geom is None:
            continue
        row = {
            "osm_type": elem.get("type"),
            "osm_id": elem.get("id"),
            "winter_sports_id": elem.get("winter_sports_id"),
            "winter_sports_name": elem.get("winter_sports_name"),
            "country": elem.get("country"),
            "state": elem.get("state"),
            "State": elem.get("State") or elem.get("state"),
            "Country": elem.get("Country") or elem.get("country"),
            "Ski Area": elem.get("Ski Area") or elem.get("winter_sports_name"),
            "geometry": geom,
        }
        tags = elem.get("tags", {})
        if tags:
            row["tags"] = json.dumps(tags)
        rows.append(row)
    return rows


def osm_elements_to_geoparquet(
    elements: List[dict],
    output_path: Union[str, Path],
    limit: Optional[int] = None,
) -> Path:
    """Convert OSM elements (in memory) to GeoParquet. For batch processing."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = _osm_elements_to_rows(elements, limit)
    if not rows:
        # Write empty GeoDataFrame so file always exists
        gdf = gpd.GeoDataFrame(columns=["osm_type", "osm_id", "winter_sports_id", "winter_sports_name", "country", "state", "tags", "geometry"], crs="EPSG:4326")
        gdf.to_parquet(output_path, index=False)
        return output_path
    gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
    gdf.to_parquet(output_path, index=False)
    return output_path


def osm_nearby_to_geoparquet(
    osm_path: str = "osm_near_winter_sports.json",
    output_path: str = "osm_near_winter_sports.parquet",
    limit: Optional[int] = None,
) -> Path:
    """Convert OSM nearby data to GeoParquet (nodes→points, ways→polygons/lines)."""
    osm_path = Path(osm_path)
    output_path = Path(output_path)

    print(f"Loading {osm_path}...")
    data = json.loads(osm_path.read_text(encoding="utf-8"))
    elements = data.get("elements", [])

    if limit:
        print(f"(Limited to first {limit} elements)")
    rows = _osm_elements_to_rows(elements, limit)
    gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
    gdf.to_parquet(output_path, index=False)
    print(f"Saved {len(gdf)} OSM elements to {output_path}")
    return output_path


def geojson_to_geoparquet(geojson_path: Union[str, Path], output_path: Union[str, Path]) -> Path:
    """Convert a GeoJSON file to GeoParquet."""
    geojson_path = Path(geojson_path)
    output_path = Path(output_path)
    if not geojson_path.exists():
        raise FileNotFoundError(geojson_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf = gpd.read_file(geojson_path)
    if not gdf.crs:
        gdf.set_crs("EPSG:4326", inplace=True)
    gdf = gdf.to_crs("EPSG:4326")
    gdf.to_parquet(output_path, index=False)
    print(f"Saved {len(gdf)} features to {output_path}")
    return output_path


def csv_to_parquet(csv_path: Union[str, Path], output_path: Union[str, Path]) -> Path:
    """Convert a CSV file to Parquet (tabular, no geometry)."""
    csv_path = Path(csv_path)
    output_path = Path(output_path)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(csv_path)
    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")
    return output_path


def export_all_to_parquet(data_dir: Union[str, Path]) -> None:
    """Convert all pipeline outputs in data_dir to Parquet (GeoJSON and CSV → Parquet)."""
    data_dir = Path(data_dir)
    pairs = [
        (data_dir / "ski_areas.geojson", data_dir / "ski_areas.parquet"),
        (data_dir / "lifts.geojson", data_dir / "lifts.parquet"),
        (data_dir / "pistes.geojson", data_dir / "pistes.parquet"),
        (data_dir / "ski_areas_analyzed.csv", data_dir / "ski_areas_analyzed.parquet"),
    ]
    for src, dst in pairs:
        if src.exists():
            try:
                if src.suffix.lower() == ".csv":
                    csv_to_parquet(src, dst)
                else:
                    geojson_to_geoparquet(src, dst)
            except Exception as e:
                print(f"Warning: failed to convert {src} -> {dst}: {e}", file=sys.stderr)
        else:
            print(f"Skipping (not found): {src}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Convert data to GeoParquet")
    sub = parser.add_subparsers(dest="cmd", help="Conversion target")

    p_ski = sub.add_parser("ski", help="Convert ski areas (analyzed + winter_sports)")
    p_ski.add_argument("-a", "--analyzed", default="ski_areas_analyzed.json")
    p_ski.add_argument("-w", "--winter-sports", default="winter_sports_test.json")
    p_ski.add_argument("-o", "--output", default="ski_areas.parquet")

    p_osm = sub.add_parser("osm", help="Convert OSM nearby data")
    p_osm.add_argument("-i", "--input", default="osm_near_winter_sports.json")
    p_osm.add_argument("-o", "--output", default="osm_near_winter_sports.parquet")
    p_osm.add_argument("-l", "--limit", type=int, help="Limit elements (for testing)")

    p_all = sub.add_parser("all", help="Convert all pipeline outputs in data dir to Parquet (geojson + csv)")
    p_all.add_argument("-d", "--data-dir", default="/data", help="Directory containing ski_areas.geojson, lifts.geojson, pistes.geojson, ski_areas_analyzed.csv")

    args = parser.parse_args()

    if args.cmd == "ski":
        ski_areas_to_geoparquet(
            args.analyzed,
            args.winter_sports,
            args.output,
        )
    elif args.cmd == "osm":
        osm_nearby_to_geoparquet(
            args.input,
            args.output,
            getattr(args, "limit", None),
        )
    elif args.cmd == "all":
        export_all_to_parquet(Path(args.data_dir))
    else:
        parser.print_help()
        print("\nExamples:")
        print("  py convert_to_geoparquet.py ski")
        print("  py convert_to_geoparquet.py osm -l 10000")
        print("  py convert_to_geoparquet.py all -d /data")
