#!/usr/bin/env python3
"""
Analyze winter_sports locations: total area, skiable terrain, lifts, downhill trails.
Tag state and country from boundaries (Natural Earth admin 0/1). Get centroid per resort.
Use output/osm_near_winter_sports.json to count features. Write output/ski_areas_analyzed.csv.
Mark as "not a downhill ski resort" when no skiable terrain, no lifts, no downhill pistes.
"""

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Aerialway values that are actual ski lifts (not station, pylon, etc.)
LIFT_TYPES = {
    "chair_lift", "gondola", "cable_car", "drag_lift", "t-bar", "j-bar",
    "platter", "magic_carpet", "rope_tow", "mixed_lift",
}

# piste:difficulty values we count (OSM piste map)
PISTE_DIFFICULTIES = (
    "novice", "easy", "intermediate", "advanced", "expert", "freeride", "extreme",
)

HA_TO_ACRES = 2.47105
M_TO_MI = 1.0 / 1609.344


def _lift_type_label(aerialway: str) -> str:
    """Format aerialway value for display (e.g. chair_lift -> chair lift)."""
    return (aerialway or "").replace("_", " ")


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in meters between two points."""
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(
        math.radians(lat2)
    ) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _way_length_m(geom: List[Dict[str, float]]) -> float:
    """Length of a way in meters."""
    if len(geom) < 2:
        return 0.0
    total = 0.0
    for i in range(len(geom) - 1):
        p1, p2 = geom[i], geom[i + 1]
        total += _haversine_m(p1["lat"], p1["lon"], p2["lat"], p2["lon"])
    return total


def _polygon_area_m2(geom: List[Dict[str, float]]) -> float:
    """Approximate polygon area in square meters (planar projection at centroid)."""
    if len(geom) < 3:
        return 0.0
    lat_c = sum(p["lat"] for p in geom) / len(geom)
    m_per_deg_lat = 111320.0
    m_per_deg_lon = 111320.0 * math.cos(math.radians(lat_c))
    n = len(geom)
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        x1 = geom[i]["lon"] * m_per_deg_lon
        y1 = geom[i]["lat"] * m_per_deg_lat
        x2 = geom[j]["lon"] * m_per_deg_lon
        y2 = geom[j]["lat"] * m_per_deg_lat
        area += x1 * y2 - x2 * y1
    return abs(area) / 2.0


def _get_geometry(elem: dict) -> Optional[List[Dict[str, float]]]:
    """Extract lat/lon geometry from node, way, or relation."""
    if elem.get("type") == "node":
        if "lat" in elem and "lon" in elem:
            return [{"lat": elem["lat"], "lon": elem["lon"]}]
        return None
    if elem.get("type") == "way":
        return elem.get("geometry")
    if elem.get("type") == "relation":
        # Use bounds centroid as rough proxy for relations
        b = elem.get("bounds")
        if b:
            lat = (b["minlat"] + b["maxlat"]) / 2
            lon = (b["minlon"] + b["maxlon"]) / 2
            return [{"lat": lat, "lon": lon}]
        # Or concatenate outer member geometries
        members = elem.get("members", [])
        all_geom = []
        for m in members:
            if m.get("role") == "outer" and m.get("geometry"):
                all_geom.extend(m["geometry"])
        if all_geom:
            return all_geom
    return None


def _geom_from_shapely(geom) -> Optional[List[Dict[str, float]]]:
    """Convert Shapely geometry to OSM-style lat/lon list."""
    if geom is None:
        return None
    from shapely.geometry import Point, LineString, Polygon
    if isinstance(geom, Point):
        return [{"lat": geom.y, "lon": geom.x}]
    if isinstance(geom, (LineString, Polygon)):
        coords = geom.exterior.coords if isinstance(geom, Polygon) else geom.coords
        return [{"lat": c[1], "lon": c[0]} for c in coords]
    return None


def _geojson_ring_to_osm_geom(coords: list) -> List[Dict[str, float]]:
    """Convert GeoJSON ring [[lon,lat],...] to OSM geometry [{lat, lon}, ...]."""
    out = []
    for c in coords:
        if len(c) >= 2:
            out.append({"lon": float(c[0]), "lat": float(c[1])})
    return out


def _get_ws_centroid(ws: dict) -> Optional[Tuple[float, float]]:
    """Get (lat, lon) centroid for a winter_sports feature."""
    bounds = ws.get("bounds")
    if bounds:
        lat = (bounds["minlat"] + bounds["maxlat"]) / 2
        lon = (bounds["minlon"] + bounds["maxlon"]) / 2
        return (lat, lon)
    geom = ws.get("geometry")
    if geom and len(geom) >= 1:
        lats = [p["lat"] for p in geom]
        lons = [p["lon"] for p in geom]
        return (sum(lats) / len(lats), sum(lons) / len(lons))
    return None


def _lookup_country_state_from_boundaries(
    lat: float, lon: float, boundaries_dir: Path
) -> Tuple[Optional[str], Optional[str]]:
    """Look up country and state (admin 1) for a point using Natural Earth shapefiles."""
    try:
        import geopandas as gpd
        from shapely.geometry import Point
    except ImportError:
        return (None, None)
    point = Point(lon, lat)
    country_name, state_name = None, None

    countries_shp = boundaries_dir / "ne_10m_admin_0_countries.shp"
    if countries_shp.exists():
        try:
            gdf = gpd.read_file(countries_shp)
            if not gdf.crs:
                gdf.set_crs("EPSG:4326", inplace=True)
            gdf = gdf.to_crs("EPSG:4326")
            pt_gdf = gpd.GeoDataFrame([{"geometry": point}], crs="EPSG:4326")
            joined = gpd.sjoin(pt_gdf, gdf, how="left", predicate="within")
            idx = joined["index_right"].iloc[0]
            if len(joined) and idx is not None and not (hasattr(idx, "__float__") and math.isnan(idx)):
                row = gdf.loc[idx]
                country_name = row.get("ADMIN") or row.get("NAME") or row.get("NAME_LONG")
                if country_name is not None and hasattr(country_name, "iloc"):
                    country_name = country_name.iloc[0] if len(country_name) else None
                if country_name is not None and (hasattr(country_name, "__float__") and math.isnan(country_name)):
                    country_name = None
        except Exception:
            pass

    states_shp = boundaries_dir / "ne_10m_admin_1_states_provinces.shp"
    if states_shp.exists() and (lat, lon):
        try:
            gdf = gpd.read_file(states_shp)
            if not gdf.crs:
                gdf.set_crs("EPSG:4326", inplace=True)
            gdf = gdf.to_crs("EPSG:4326")
            pt_gdf = gpd.GeoDataFrame([{"geometry": point}], crs="EPSG:4326")
            joined = gpd.sjoin(pt_gdf, gdf, how="left", predicate="within")
            idx = joined["index_right"].iloc[0]
            if len(joined) and idx is not None and not (hasattr(idx, "__float__") and math.isnan(idx)):
                row = gdf.loc[idx]
                state_name = row.get("name") or row.get("NAME") or row.get("NAME_1") or row.get("admin")
                if state_name is not None and hasattr(state_name, "iloc"):
                    state_name = state_name.iloc[0] if len(state_name) else None
                if state_name is not None and (hasattr(state_name, "__float__") and math.isnan(state_name)):
                    state_name = None
        except Exception:
            pass

    return (country_name, state_name)


def _load_winter_sports(path: Path) -> Dict[Tuple[str, int], dict]:
    """Load winter_sports from OSM JSON or GeoJSON. Returns ws_by_id."""
    data = json.loads(path.read_text(encoding="utf-8"))
    ws_by_id: Dict[Tuple[str, int], dict] = {}

    # GeoJSON FeatureCollection
    if data.get("type") == "FeatureCollection" and "features" in data:
        for f in data["features"]:
            if f.get("type") != "Feature" or not f.get("geometry"):
                continue
            props = f.get("properties") or {}
            oid = props.get("osm_relation_id") or props.get("osm_way_id") or props.get("id")
            if oid is None:
                continue
            ws_type = "relation" if props.get("osm_relation_id") else "way"
            if isinstance(oid, str) and oid.isdigit():
                oid = int(oid)
            geom = f["geometry"]
            coords = []
            if geom.get("type") == "MultiPolygon" and geom.get("coordinates"):
                ring = geom["coordinates"][0][0]  # first polygon, outer ring
                coords = _geojson_ring_to_osm_geom(ring)
            elif geom.get("type") == "Polygon" and geom.get("coordinates"):
                ring = geom["coordinates"][0]
                coords = _geojson_ring_to_osm_geom(ring)
            if not coords:
                continue
            lats = [p["lat"] for p in coords]
            lons = [p["lon"] for p in coords]
            ws = {
                "type": ws_type,
                "id": oid,
                "tags": {"name": props.get("name") or props.get("Name") or str(oid)},
                "geometry": coords,
                "bounds": {"minlat": min(lats), "maxlat": max(lats), "minlon": min(lons), "maxlon": max(lons)},
                "country": props.get("country"),
                "state": props.get("state"),
            }
            ws_by_id[(ws_type, oid)] = ws
        return ws_by_id

    # OSM JSON
    for elem in data.get("elements", []):
        if elem.get("type") in ("way", "relation"):
            ws_by_id[(elem["type"], elem["id"])] = elem
    return ws_by_id


def _load_osm_nearby(path: Path) -> List[dict]:
    """Load OSM nearby from JSON or GeoParquet."""
    if path.suffix.lower() == ".parquet":
        import geopandas as gpd
        gdf = gpd.read_parquet(path)
        elements = []
        for _, row in gdf.iterrows():
            tags_raw = row.get("tags")
            try:
                tags = json.loads(tags_raw) if tags_raw and str(tags_raw) != "nan" else {}
            except (TypeError, json.JSONDecodeError):
                tags = {}
            geom = row.get("geometry")
            geom_list = _geom_from_shapely(geom)
            elem = {
                "type": row.get("osm_type"),
                "id": row.get("osm_id"),
                "winter_sports_id": row.get("winter_sports_id"),
                "winter_sports_type": row.get("winter_sports_type"),
                "winter_sports_name": row.get("winter_sports_name"),
                "country": row.get("country"),
                "state": row.get("state"),
                "tags": tags,
                "geometry": geom_list,
            }
            if geom_list and len(geom_list) == 1:
                elem["lat"] = geom_list[0]["lat"]
                elem["lon"] = geom_list[0]["lon"]
            elements.append(elem)
        return elements
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("elements", [])


def analyze(
    winter_sports_path: str = "winter_sports_test.json",
    osm_nearby_path: Union[str, Path] = "osm_near_winter_sports.json",
    output_path: Optional[str] = None,
    boundaries_dir: Optional[Union[str, Path]] = "boundaries",
) -> List[Dict[str, Any]]:
    """Analyze each winter_sports and produce enriched records.
    Tags country/state from boundaries (centroid point-in-polygon). Writes centroid and feature counts.
    Supports winter_sports as OSM JSON or GeoJSON. Reads OSM nearby from JSON (or Parquet)."""
    winter_sports_path = Path(winter_sports_path)
    osm_nearby_path = Path(osm_nearby_path)
    output_path = Path(output_path or "output/ski_areas_analyzed.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    boundaries_path = Path(boundaries_dir) if boundaries_dir else None

    print(f"Loading {winter_sports_path}...")
    ws_by_id = _load_winter_sports(winter_sports_path)

    print(f"Loading {osm_nearby_path}...")
    osm_elements = _load_osm_nearby(osm_nearby_path)

    # Group nearby OSM elements by winter_sports_id (handle int/float from parquet)
    by_ws: Dict[Tuple[str, int], List[dict]] = {}
    for elem in osm_elements:
        ws_id = elem.get("winter_sports_id")
        ws_type = elem.get("winter_sports_type")
        if ws_id is not None and ws_type:
            try:
                key = (str(ws_type), int(float(ws_id)))
                by_ws.setdefault(key, []).append(elem)
            except (TypeError, ValueError):
                pass

    # Default piste width for area estimate (meters)
    PISTE_WIDTH_M = 30.0

    # Iterate over ALL winter_sports from input (not just those with nearby OSM)
    results = []
    for (ws_type, ws_id), ws in ws_by_id.items():
        nearby = by_ws.get((ws_type, ws_id)) or by_ws.get((str(ws_type), ws_id)) or []
        tags = ws.get("tags", {})
        name = tags.get("name:en") or tags.get("name") or f"{ws_type}/{ws_id}"

        # Centroid and country/state from boundaries
        centroid = _get_ws_centroid(ws)
        centroid_lat = round(centroid[0], 6) if centroid else None
        centroid_lon = round(centroid[1], 6) if centroid else None
        country = ws.get("country")
        state = ws.get("state")
        if boundaries_path and boundaries_path.exists() and centroid:
            lat, lon = centroid
            bc, bs = _lookup_country_state_from_boundaries(lat, lon, boundaries_path)
            if bc is not None:
                country = bc
            if bs is not None:
                state = bs

        # 1. Total area (from winter_sports polygon)
        total_area_m2 = 0.0
        geom = _get_geometry(ws)
        if geom and len(geom) >= 3:
            total_area_m2 = _polygon_area_m2(geom)

        # 2. Skiable terrain, trail lengths, piste flags, and piste:difficulty counts
        skiable_m2 = 0.0
        downhill_trail_count = 0
        trail_lengths_m: List[float] = []
        has_gladed = False
        has_snow_park = False
        has_sledding_tubing = False
        difficulty_counts: Dict[str, int] = {d: 0 for d in PISTE_DIFFICULTIES}
        for elem in nearby:
            etags = elem.get("tags", {})
            piste_type = etags.get("piste:type")
            if piste_type == "freestyle":
                has_snow_park = True
            if piste_type in ("sled", "tubing"):
                has_sledding_tubing = True
            if piste_type == "downhill":
                downhill_trail_count += 1
                diff = etags.get("piste:difficulty", "").strip().lower()
                if diff in difficulty_counts:
                    difficulty_counts[diff] += 1
                if etags.get("piste:grooming") == "no":
                    has_gladed = True
                g = _get_geometry(elem)
                if g:
                    if len(g) >= 3 and elem.get("type") == "way":
                        skiable_m2 += _polygon_area_m2(g)
                        # skip polygon in trail lengths (longest/avg are for linear runs)
                    else:
                        width = float(etags.get("piste:width", PISTE_WIDTH_M))
                        length_m = _way_length_m(g)
                        skiable_m2 += length_m * width
                        trail_lengths_m.append(length_m)

        longest_trail_mi = round(max(trail_lengths_m) * M_TO_MI, 2) if trail_lengths_m else 0.0
        avg_trail_mi = round((sum(trail_lengths_m) / len(trail_lengths_m)) * M_TO_MI, 2) if trail_lengths_m else 0.0

        # 3. Total lifts, longest lift, lift type counts
        lift_count = 0
        seen_lift_ways = set()
        lift_type_counts: Dict[str, int] = {}
        max_lift_m = 0.0
        for elem in nearby:
            etags = elem.get("tags", {})
            aw = etags.get("aerialway")
            if aw and aw in LIFT_TYPES:
                key = (elem.get("type"), elem.get("id"))
                if key not in seen_lift_ways:
                    seen_lift_ways.add(key)
                    lift_count += 1
                    lift_type_counts[aw] = lift_type_counts.get(aw, 0) + 1
                    g = _get_geometry(elem)
                    if g and len(g) >= 2:
                        length_m = _way_length_m(g)
                        if length_m > max_lift_m:
                            max_lift_m = length_m
        longest_lift_mi = round(max_lift_m * M_TO_MI, 2) if max_lift_m > 0 else 0.0
        lift_types_str = ", ".join(
            f"{_lift_type_label(aw)}: {c}" for aw, c in sorted(lift_type_counts.items())
        ) if lift_type_counts else ""

        # 4. Classification
        is_downhill_resort = (
            skiable_m2 > 0 or lift_count > 0 or downhill_trail_count > 0
        )
        resort_type = (
            "downhill ski resort"
            if is_downhill_resort
            else "not a downhill ski resort"
        )

        total_area_ha = round(total_area_m2 / 10000, 2)
        skiable_terrain_ha = round(skiable_m2 / 10000, 2)
        rec = {
            "winter_sports_id": ws_id,
            "winter_sports_type": ws_type,
            "name": name,
            "country": country,
            "state": state,
            "centroid_lat": centroid_lat,
            "centroid_lon": centroid_lon,
            "total_area_ha": total_area_ha,
            "total_area_acres": round(total_area_ha * HA_TO_ACRES, 0),
            "skiable_terrain_ha": skiable_terrain_ha,
            "skiable_terrain_acres": round(skiable_terrain_ha * HA_TO_ACRES, 0),
            "total_lifts": lift_count,
            "longest_lift_mi": longest_lift_mi,
            "downhill_trails": downhill_trail_count,
            "longest_trail_mi": longest_trail_mi,
            "avg_trail_mi": avg_trail_mi,
            **{f"trails_{d}": difficulty_counts[d] for d in PISTE_DIFFICULTIES},
            "gladed_terrain": "Yes" if has_gladed else "No",
            "snow_park": "Yes" if has_snow_park else "No",
            "sledding_tubing": "Yes" if has_sledding_tubing else "No",
            "lift_types": lift_types_str,
            "resort_type": resort_type,
        }
        results.append(rec)

    import csv
    fieldnames = [
        "winter_sports_id", "winter_sports_type", "name", "country", "state",
        "centroid_lat", "centroid_lon",
        "total_area_ha", "total_area_acres", "skiable_terrain_ha", "skiable_terrain_acres",
        "total_lifts", "longest_lift_mi", "downhill_trails", "longest_trail_mi", "avg_trail_mi",
        *[f"trails_{d}" for d in PISTE_DIFFICULTIES],
        "gladed_terrain", "snow_park", "sledding_tubing", "lift_types",
        "resort_type",
    ]
    if results:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(results)
        print(f"Saved {len(results)} analyzed ski areas to {output_path}")
    else:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
        print(f"Saved 0 analyzed ski areas to {output_path}")

    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Analyze ski areas from OSM data (JSON); tag country/state from boundaries")
    parser.add_argument(
        "winter_sports",
        nargs="?",
        default="winter_sports_test.json",
        help="Winter sports GeoJSON or OSM JSON",
    )
    parser.add_argument(
        "osm_nearby",
        nargs="?",
        default="output/osm_near_winter_sports.json",
        help="OSM data near winter sports (JSON or Parquet)",
    )
    parser.add_argument("-o", "--output", default="output/ski_areas_analyzed.csv", help="Output CSV file")
    parser.add_argument("-b", "--boundaries", default="boundaries", help="Directory with Natural Earth admin 0/1 shapefiles")
    args = parser.parse_args()
    analyze(args.winter_sports, args.osm_nearby, args.output, boundaries_dir=args.boundaries)
