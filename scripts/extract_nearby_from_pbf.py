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

# Max distance (m) from ski area polygon boundary to include OSM features. Default 1000 ft.
RADIUS_METERS = int(__import__("os").environ.get("OSM_NEARBY_RADIUS_M", "305"))
# Max distance (m) for grouping ski areas into one extract. Prevents continent-sized bbox → OOM.
CLUSTER_DIST_M = int(__import__("os").environ.get("OSM_NEARBY_CLUSTER_DIST_M", "300000"))  # 300 km


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
    """Return one bbox that contains every ski area's polygon buffered by radius."""
    minlon, minlat, maxlon, maxlat = None, None, None, None
    for ws in features:
        poly = ws.get("polygon")
        if poly is not None:
            try:
                buffered = _buffer_geom_meters(poly, radius_m)
                b = buffered.bounds
                a, b_minlat, c, b_maxlat = b[0], b[1], b[2], b[3]
            except Exception:
                lat, lon = ws["centroid"]
                a, b_minlat, c, b_maxlat = _bbox_from_centroid(lat, lon, radius_m)
        else:
            lat, lon = ws["centroid"]
            a, b_minlat, c, b_maxlat = _bbox_from_centroid(lat, lon, radius_m)
        if minlon is None:
            minlon, minlat, maxlon, maxlat = a, b_minlat, c, b_maxlat
        else:
            minlon = min(minlon, a)
            minlat = min(minlat, b_minlat)
            maxlon = max(maxlon, c)
            maxlat = max(maxlat, b_maxlat)
    return (minlon, minlat, maxlon, maxlat)


def _cluster_features(features: List[dict], max_dist_m: float) -> List[List[dict]]:
    """Group ski areas within max_dist_m into clusters. Uses union-find."""
    n = len(features)
    parent = list(range(n))

    def find(i: int) -> int:
        if parent[i] != i:
            parent[i] = find(parent[i])
        return parent[i]

    def union(i: int, j: int) -> None:
        pi, pj = find(i), find(j)
        if pi != pj:
            parent[pi] = pj

    for i in range(n):
        for j in range(i + 1, n):
            if _haversine_m(*features[i]["centroid"], *features[j]["centroid"]) <= max_dist_m:
                union(i, j)

    clusters: dict[int, List[dict]] = {}
    for i in range(n):
        root = find(i)
        clusters.setdefault(root, []).append(features[i])

    return list(clusters.values())


def _shape_from_geojson_feature(feat: dict):
    """Return Shapely geometry of a GeoJSON feature, or None."""
    geom = feat.get("geometry")
    if not geom:
        return None
    try:
        from shapely.geometry import shape
        s = shape(geom)
        return s if not s.is_empty else None
    except Exception:
        return None


def _clip_geom_to_polygon(geom, polygon) -> List:
    """Clip geometry to polygon. Returns list of GeoJSON geometry dicts (only parts inside polygon).
    Explodes Multi* and GeometryCollection so each part can be emitted as a separate element."""
    try:
        clipped = geom.intersection(polygon)
    except Exception:
        return []
    if clipped.is_empty:
        return []
    geoms = []
    if clipped.geom_type == "GeometryCollection":
        for g in clipped.geoms:
            if g.is_empty:
                continue
            geoms.extend(_clip_geom_to_polygon(g, polygon))
        return geoms
    if clipped.geom_type in ("MultiLineString", "MultiPolygon"):
        for g in clipped.geoms:
            if not g.is_empty and hasattr(g, "__geo_interface__"):
                geoms.append(g.__geo_interface__)
        return geoms
    if clipped.geom_type in ("Point", "LineString", "Polygon"):
        geoms.append(clipped.__geo_interface__)
        return geoms
    return []


def _buffer_geom_meters(geom, meters: float):
    """Buffer geometry by meters (uses local UTM projection for accuracy)."""
    from shapely.ops import transform
    from shapely.geometry import Point
    try:
        import pyproj
    except ImportError:
        # Fallback: approximate 1 deg lat ≈ 111320 m
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


def _load_features_from_geojson(path: Path) -> List[dict]:
    """Load ski area features from GeoJSON with centroid and polygon for distance filtering."""
    from shapely.geometry import shape, Point
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
            # Keep polygon for distance filter; for points use centroid (buffered in meters later)
            poly = s if s.geom_type in ("Polygon", "MultiPolygon") else pt
        except Exception:
            continue
        props = f.get("properties") or {}
        oid = props.get("osm_relation_id") or props.get("osm_way_id") or props.get("id") or i
        if isinstance(oid, str) and oid.isdigit():
            oid = int(oid)
        ws_type = "relation" if props.get("osm_relation_id") else "way"
        country = props.get("Country") or props.get("country")
        state = props.get("State") or props.get("state")
        features.append({
            "id": oid,
            "type": ws_type,
            "centroid": (lat, lon),
            "polygon": poly,
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


def _process_cluster_extract(
    extract_pbf: Path,
    cluster_features: List[dict],
    radius_m: int,
) -> List[dict]:
    """Run ogr2ogr on extract, filter by distance to ski area polygon, return OSM elements.
    Keeps only elements within radius_m of the ski area polygon boundary."""
    elements: List[dict] = []
    # Precompute buffered polygon for each ski area
    ws_buffered = []
    for ws in cluster_features:
        poly = ws.get("polygon")
        if poly is None:
            continue
        try:
            buf = _buffer_geom_meters(poly, radius_m)
            ws_buffered.append({**ws, "buffered": buf})
        except Exception:
            ws_buffered.append({**ws, "buffered": poly.buffer(radius_m / 111320.0)})

    for layer in ["points", "lines", "multilinestrings", "multipolygons"]:
        layer_geojson = extract_pbf.parent / f"{layer}.geojson"
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
            elem_geom = _shape_from_geojson_feature(feat)
            if elem_geom is None:
                continue
            for ws in ws_buffered:
                if not elem_geom.intersects(ws["buffered"]):
                    continue
                # Clip geometry to 1000 ft buffer so features don't extend miles outside the resort
                clipped_geoms = _clip_geom_to_polygon(elem_geom, ws["buffered"])
                for geo in clipped_geoms:
                    if not geo or not geo.get("coordinates"):
                        continue
                    # Build a feature with clipped geometry (same properties)
                    clipped_feat = {"type": "Feature", "properties": feat.get("properties") or {}, "geometry": geo}
                    elem = _geojson_feature_to_osm_element(
                        clipped_feat, ws["id"], ws["type"], ws["name"],
                        ws.get("country"), ws.get("state"),
                    )
                    if elem:
                        elements.append(elem)
                break  # Assign to first matching ski area only
        layer_geojson.unlink(missing_ok=True)  # Free disk/memory before next layer
    return elements


def extract_from_pbf(
    pbf_path: Path,
    ski_areas_path: Path,
    output_path: Path,
    radius_m: int = RADIUS_METERS,
    cluster_dist_m: int = CLUSTER_DIST_M,
) -> None:
    """Extract OSM data within radius of each ski area from PBF.
    Clusters ski areas by proximity to avoid continent-sized bbox (OOM). One osmium
    extract per cluster, then assign elements to ski areas by distance in Python.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    json_path = output_path.with_suffix(".json") if output_path.suffix.lower() == ".parquet" else output_path

    features = _load_features_from_geojson(ski_areas_path)
    if not features:
        print("No ski areas in input; writing empty OSM nearby output.", file=sys.stderr)
        json_path.write_text(json.dumps({"version": 0.6, "generator": "extract_nearby_from_pbf.py", "elements": []}, indent=2), encoding="utf-8")
        return

    clusters = _cluster_features(features, cluster_dist_m)
    print(f"Extracting OSM data within {radius_m}m ({radius_m/0.3048:.0f} ft) of ski area polygon for {len(features)} areas...")
    print(f"PBF: {pbf_path} | Output: {json_path}")
    print(f"  ({len(clusters)} cluster(s) within {cluster_dist_m/1000:.0f}km to avoid OOM)")

    all_elements: List[dict] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        for ci, cluster in enumerate(clusters):
            print(f"  cluster {ci + 1}/{len(clusters)} ...", file=sys.stderr, flush=True)
            minlon, minlat, maxlon, maxlat = _merged_bbox(cluster, radius_m)
            bbox_str = f"{minlon},{minlat},{maxlon},{maxlat}"
            extract_pbf = tmp / f"extract_{ci}.pbf"

            try:
                r = subprocess.run(
                    ["osmium", "extract", "-b", bbox_str, str(pbf_path), "-o", str(extract_pbf)],
                    capture_output=True, text=True,
                )
                if r.returncode != 0:
                    raise subprocess.CalledProcessError(r.returncode, r.args, r.stdout, r.stderr)
            except FileNotFoundError as e:
                print(f"  osmium extract failed (cluster {ci}): osmium not found", file=sys.stderr)
                sys.exit(1)
            except subprocess.CalledProcessError as e:
                print(f"  osmium extract failed (cluster {ci}): exit {e.returncode}", file=sys.stderr)
                if e.stderr:
                    print(f"  osmium stderr: {e.stderr.strip()}", file=sys.stderr)
                sys.exit(1)

            if extract_pbf.exists() and extract_pbf.stat().st_size > 0:
                cluster_elements = _process_cluster_extract(extract_pbf, cluster, radius_m)
                all_elements.extend(cluster_elements)
            extract_pbf.unlink(missing_ok=True)

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
    p.add_argument("-r", "--radius", type=int, default=RADIUS_METERS,
                    help="Max distance (m) from ski area polygon to include features (default: 305 = 1000 ft)")
    p.add_argument("--cluster-dist", type=int, default=CLUSTER_DIST_M,
                    help="Max distance (m) to group ski areas; smaller = more clusters, less memory (default: 300000)")
    args = p.parse_args()
    extract_from_pbf(
        Path(args.pbf), Path(args.ski_areas), Path(args.output),
        radius_m=args.radius,
        cluster_dist_m=args.cluster_dist,
    )
