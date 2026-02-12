#!/usr/bin/env python3
"""
Extract OSM data within radius of each ski area from a local PBF file.
No Overpass API - uses osmium extract + ogr2ogr. Fully local.
Outputs JSON with every element tagged with the ski area (winter_sports_id,
winter_sports_type, winter_sports_name, country, state). Parquet is produced
in a separate step from this JSON.
"""
import json
import math
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

RADIUS_METERS = int(__import__("os").environ.get("OSM_NEARBY_RADIUS_M", "2000"))


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in meters between two WGS84 points (approximate)."""
    R = 6371000.0  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _bbox_from_centroid(lat: float, lon: float, radius_m: float) -> Tuple[float, float, float, float]:
    """Return (minlon, minlat, maxlon, maxlat) for bbox around centroid."""
    deg_lat = radius_m / 111320.0
    deg_lon = radius_m / (111320.0 * math.cos(math.radians(lat)))
    return (
        lon - deg_lon, lat - deg_lat,
        lon + deg_lon, lat + deg_lat,
    )


def _merged_bbox(features: List[dict], radius_m: float) -> Tuple[float, float, float, float]:
    """Return one bbox that contains every ski area's radius bbox."""
    minlon, minlat, maxlon, maxlat = None, None, None, None
    for ws in features:
        lat, lon = ws["centroid"]
        a, b, c, d = _bbox_from_centroid(lat, lon, radius_m)
        if minlon is None:
            minlon, minlat, maxlon, maxlat = a, b, c, d
        else:
            minlon = min(minlon, a)
            minlat = min(minlat, b)
            maxlon = max(maxlon, c)
            maxlat = max(maxlat, d)
    return (minlon, minlat, maxlon, maxlat)


def _point_from_geojson_feature(feat: dict) -> Optional[Tuple[float, float]]:
    """Return (lat, lon) centroid of a GeoJSON feature for distance check."""
    geom = feat.get("geometry")
    if not geom:
        return None
    try:
        from shapely.geometry import shape
        s = shape(geom)
        if s.is_empty:
            return None
        pt = s.centroid
        return (float(pt.y), float(pt.x))
    except Exception:
        return None


def _load_features_from_geojson(path: Path) -> List[dict]:
    """Load ski area features from GeoJSON, return list with centroid, id, name, etc."""
    from shapely.geometry import shape
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("type") != "FeatureCollection":
        return []
    features = []
    for i, f in enumerate(data.get("features", [])):
        if f.get("type") != "Feature" or not f.get("geometry"):
            continue
        try:
            s = shape(f["geometry"])
            if s.is_empty:
                continue
            pt = s.centroid
            lat, lon = float(pt.y), float(pt.x)
        except Exception:
            continue
        props = f.get("properties") or {}
        oid = props.get("osm_relation_id") or props.get("osm_way_id") or props.get("id") or i
        if isinstance(oid, str) and oid.isdigit():
            oid = int(oid)
        ws_type = "relation" if props.get("osm_relation_id") else "way"
        # Prefer State/Country (from enrich) if present
        country = props.get("Country") or props.get("country")
        state = props.get("State") or props.get("state")
        features.append({
            "id": oid,
            "type": ws_type,
            "centroid": (lat, lon),
            "name": props.get("name") or props.get("Name") or str(oid),
            "country": country,
            "state": state,
        })
    return features


def _geojson_coords_to_osm_geom(coords) -> List[dict]:
    """Convert GeoJSON coords [[lon,lat],...] to OSM geometry [{lat, lon}, ...]."""
    out = []
    for c in coords:
        if len(c) >= 2:
            out.append({"lat": float(c[1]), "lon": float(c[0])})
    return out


def _geojson_feature_to_osm_element(feat: dict, ws_id: int, ws_type: str, ws_name: str, country: Optional[str], state: Optional[str]) -> Optional[dict]:
    """Convert GDAL/ogr2ogr GeoJSON feature to OSM element format."""
    geom = feat.get("geometry")
    if not geom:
        return None
    props = feat.get("properties") or {}
    coords = geom.get("coordinates")
    if not coords:
        return None
    if geom.get("type") == "Point":
        geom_list = [{"lat": coords[1], "lon": coords[0]}]
        elem_type = "node"
    elif geom.get("type") in ("LineString", "MultiLineString"):
        if geom.get("type") == "LineString":
            geom_list = _geojson_coords_to_osm_geom(coords)
        else:
            geom_list = []
            for ring in coords:
                geom_list.extend(_geojson_coords_to_osm_geom(ring))
        elem_type = "way"
    elif geom.get("type") in ("Polygon", "MultiPolygon"):
        if geom.get("type") == "Polygon":
            geom_list = _geojson_coords_to_osm_geom(coords[0])
        else:
            geom_list = _geojson_coords_to_osm_geom(coords[0][0]) if coords else []
        elem_type = "way"
    else:
        return None
    if geom_list and len(geom_list) < 2 and elem_type == "way":
        return None
    elem_id = props.get("osm_id") or props.get("osm_way_id") or props.get("id") or 0
    if isinstance(elem_id, str) and elem_id.isdigit():
        elem_id = int(elem_id)
    tags = {}
    for k, v in props.items():
        if k in ("osm_id", "osm_way_id", "id", "name", "other_tags") or "geometry" in k.lower():
            if k == "name" and v:
                tags["name"] = str(v)
            elif k == "other_tags" and v:
                # HSTORE: "key"=>"value","key2"=>"value2" (or JSON if TAGS_FORMAT=json)
                s = str(v).strip()
                if s.startswith("{"):
                    try:
                        tags.update(json.loads(s))
                    except json.JSONDecodeError:
                        pass
                else:
                    for part in s.split('","'):
                        if "=>" in part:
                            kv = part.replace('"', "").split("=>", 1)
                            if len(kv) == 2:
                                tags[kv[0].strip()] = kv[1].strip()
            continue
        if v is not None and str(v).strip():
            tags[k] = str(v)
    result = {
        "type": elem_type,
        "id": elem_id,
        "tags": tags,
        "geometry": geom_list,
        "winter_sports_id": ws_id,
        "winter_sports_type": ws_type,
        "winter_sports_name": ws_name,
        "country": country,
        "state": state,
        "State": state,
        "Country": country,
        "Ski Area": ws_name,
    }
    if geom_list and len(geom_list) == 1:
        result["lat"] = geom_list[0]["lat"]
        result["lon"] = geom_list[0]["lon"]
    return result


def extract_from_pbf(
    pbf_path: Path,
    ski_areas_path: Path,
    output_path: Path,
    radius_m: int = RADIUS_METERS,
) -> None:
    """Extract OSM data within radius of each ski area from PBF.
    One merged bbox extract (single PBF read), then assign elements to ski areas by distance in Python.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    json_path = output_path.with_suffix(".json") if output_path.suffix.lower() == ".parquet" else output_path

    features = _load_features_from_geojson(ski_areas_path)
    if not features:
        print("Error: No features with geometry in input.", file=sys.stderr)
        sys.exit(1)

    print(f"Extracting OSM data within {radius_m/1000:.1f}km of {len(features)} ski areas from PBF...")
    print(f"PBF: {pbf_path} | Output: {json_path}")
    print("  (one merged bbox extract, then filter by distance in Python)")

    minlon, minlat, maxlon, maxlat = _merged_bbox(features, radius_m)
    bbox_str = f"{minlon},{minlat},{maxlon},{maxlat}"

    all_elements: List[dict] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        extract_pbf = tmp / "extract.pbf"

        # Single osmium extract for the whole region
        try:
            subprocess.run(
                ["osmium", "extract", "-b", bbox_str, str(pbf_path), "-o", str(extract_pbf)],
                check=True, capture_output=True, text=True,
            )
        except (FileNotFoundError, subprocess.CalledProcessError) as e:
            print(f"  osmium extract failed: {e}", file=sys.stderr)
            sys.exit(1)

        if not extract_pbf.exists() or extract_pbf.stat().st_size == 0:
            print("  No data in extract (empty bbox or PBF).", file=sys.stderr)
            json_output = {"version": 0.6, "generator": "extract_nearby_from_pbf.py", "elements": []}
            json_path.write_text(json.dumps(json_output, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"Saved 0 elements to {json_path} (JSON)")
            return

        # Convert to GeoJSON once per layer
        for layer in ["points", "lines", "multilinestrings", "multipolygons"]:
            layer_geojson = tmp / f"{layer}.geojson"
            try:
                subprocess.run(
                    ["ogr2ogr", "-f", "GeoJSON", "-t_srs", "EPSG:4326",
                     "-sql", f"SELECT * FROM {layer}",
                     str(layer_geojson), str(extract_pbf)],
                    check=True, capture_output=True, text=True,
                )
            except (FileNotFoundError, subprocess.CalledProcessError):
                continue

            if not layer_geojson.exists() or layer_geojson.stat().st_size <= 50:
                continue

            data = json.loads(layer_geojson.read_text(encoding="utf-8"))
            for feat in data.get("features", []):
                pt = _point_from_geojson_feature(feat)
                if pt is None:
                    continue
                lat_f, lon_f = pt
                # Which ski areas is this feature within radius of?
                for ws in features:
                    lat_ws, lon_ws = ws["centroid"]
                    if _haversine_m(lat_f, lon_f, lat_ws, lon_ws) <= radius_m:
                        elem = _geojson_feature_to_osm_element(
                            feat, ws["id"], ws["type"], ws["name"],
                            ws.get("country"), ws.get("state"),
                        )
                        if elem:
                            all_elements.append(elem)

    json_output = {"version": 0.6, "generator": "extract_nearby_from_pbf.py", "elements": all_elements}
    json_path.write_text(json.dumps(json_output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {len(all_elements)} elements to {json_path} (JSON)")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Extract OSM data near ski areas from local PBF (outputs JSON)")
    p.add_argument("pbf", help="Path to OSM PBF file")
    p.add_argument("ski_areas", help="Path to ski areas GeoJSON")
    p.add_argument("-o", "--output", default="output/osm_near_winter_sports.json",
                    help="Output JSON path (default: output/osm_near_winter_sports.json)")
    p.add_argument("-r", "--radius", type=int, default=RADIUS_METERS, help="Radius in meters")
    args = p.parse_args()
    extract_from_pbf(
        Path(args.pbf), Path(args.ski_areas), Path(args.output),
        radius_m=args.radius,
    )
