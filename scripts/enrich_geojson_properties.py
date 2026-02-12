#!/usr/bin/env python3
"""
Add State, Country, and Ski Area properties to GeoJSON features.
Uses Natural Earth boundaries for State/Country; optionally ski area polygons for Ski Area.
Run after extract and lifts_and_pistes so ski_areas.geojson, lifts.geojson, pistes.geojson exist.
"""
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Allow import from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _centroid_from_geojson_geometry(geom: dict) -> Optional[Tuple[float, float]]:
    """Return (lat, lon) centroid from a GeoJSON geometry."""
    try:
        from shapely.geometry import shape
        s = shape(geom)
        if s.is_empty:
            return None
        pt = s.centroid
        return (float(pt.y), float(pt.x))
    except Exception:
        return None


def _scalar(val: Any) -> Optional[str]:
    """Extract a string from a pandas cell (may be Series, nan, etc.)."""
    if val is None:
        return None
    if hasattr(val, "iloc"):
        val = val.iloc[0] if len(val) else None
    if val is None or (hasattr(val, "__float__") and math.isnan(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() != "nan" else None


def _load_boundaries(boundaries_dir: Path) -> Tuple[Any, Any]:
    """Load countries and states GeoDataFrames once. Returns (countries_gdf, states_gdf); either may be None."""
    try:
        import geopandas as gpd
    except ImportError:
        return (None, None)
    boundaries_dir = Path(boundaries_dir)
    countries_gdf, states_gdf = None, None

    countries_shp = boundaries_dir / "ne_10m_admin_0_countries.shp"
    if countries_shp.exists():
        try:
            countries_gdf = gpd.read_file(countries_shp)
            if not countries_gdf.crs:
                countries_gdf.set_crs("EPSG:4326", inplace=True)
            countries_gdf = countries_gdf.to_crs("EPSG:4326")
        except Exception:
            pass

    states_shp = boundaries_dir / "ne_10m_admin_1_states_provinces.shp"
    if states_shp.exists():
        try:
            states_gdf = gpd.read_file(states_shp)
            if not states_gdf.crs:
                states_gdf.set_crs("EPSG:4326", inplace=True)
            states_gdf = states_gdf.to_crs("EPSG:4326")
        except Exception:
            pass

    return (countries_gdf, states_gdf)


def _batch_lookup_country_state(
    centroids: List[Tuple[float, float]],
    countries_gdf: Any,
    states_gdf: Any,
) -> List[Tuple[Optional[str], Optional[str]]]:
    """Look up country and state for many (lat, lon) points in one go. Returns list of (country, state)."""
    if not centroids:
        return []
    try:
        import geopandas as gpd
        from shapely.geometry import Point
    except ImportError:
        return [(None, None)] * len(centroids)
    points = [Point(lon, lat) for lat, lon in centroids]
    pt_gdf = gpd.GeoDataFrame({"geometry": points}, crs="EPSG:4326")
    n = len(centroids)
    country_names: List[Optional[str]] = [None] * n
    state_names: List[Optional[str]] = [None] * n

    if countries_gdf is not None and not countries_gdf.empty:
        try:
            joined = gpd.sjoin(pt_gdf, countries_gdf, how="left", predicate="within")
            if len(joined) > 0:
                first = joined.groupby(level=0).first()
                for i in range(n):
                    if i not in first.index:
                        continue
                    row = first.loc[i]
                    for col in ("ADMIN", "NAME", "NAME_LONG", "SOVEREIGNT"):
                        if col in row.index:
                            country_names[i] = _scalar(row[col])
                            if country_names[i]:
                                break
        except Exception:
            pass

    if states_gdf is not None and not states_gdf.empty:
        try:
            joined = gpd.sjoin(pt_gdf, states_gdf, how="left", predicate="within")
            if len(joined) > 0:
                first = joined.groupby(level=0).first()
                for i in range(n):
                    if i not in first.index:
                        continue
                    row = first.loc[i]
                    for col in ("name", "NAME", "NAME_1", "admin", "ADMIN1"):
                        if col in row.index:
                            state_names[i] = _scalar(row[col])
                            if state_names[i]:
                                break
        except Exception:
            pass

    return list(zip(country_names, state_names))


def _lookup_country_state(lat: float, lon: float, boundaries_dir: Path) -> Tuple[Optional[str], Optional[str]]:
    """Look up country and state for a single point (reads shapefiles; prefer batch for many points)."""
    countries_gdf, states_gdf = _load_boundaries(boundaries_dir)
    results = _batch_lookup_country_state([(lat, lon)], countries_gdf, states_gdf)
    return results[0] if results else (None, None)


def _load_ski_area_polygons(geojson_path: Path) -> List[Tuple[Any, str, Optional[str], Optional[str]]]:
    """Load ski area geometries and names + State/Country. Returns [(geom, name, state, country), ...]."""
    try:
        from shapely.geometry import shape
    except ImportError:
        return []
    data = json.loads(geojson_path.read_text(encoding="utf-8"))
    if data.get("type") != "FeatureCollection" or not data.get("features"):
        return []
    out = []
    for f in data["features"]:
        geom = f.get("geometry")
        if not geom:
            continue
        try:
            s = shape(geom)
            if s.is_empty:
                continue
            props = f.get("properties") or {}
            name = props.get("name") or props.get("Name") or props.get("Ski Area") or "Unknown"
            state = props.get("State") or props.get("state")
            country = props.get("Country") or props.get("country")
            out.append((s, name, state, country))
        except Exception:
            continue
    return out


def _ski_area_at_point(
    lat: float, lon: float,
    ski_polygons: List[Tuple[Any, str, Optional[str], Optional[str]]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (ski_area_name, state, country) if (lat, lon) is inside any polygon."""
    try:
        from shapely.geometry import Point
    except ImportError:
        return (None, None, None)
    pt = Point(lon, lat)
    for poly, name, state, country in ski_polygons:
        if poly.contains(pt):
            return (name, state, country)
    return (None, None, None)


def _build_ski_area_index(
    ski_polygons: List[Tuple[Any, str, Optional[str], Optional[str]]],
) -> Tuple[Any, List[Tuple[str, Optional[str], Optional[str]]]]:
    """Build STRtree from ski area geometries. Returns (tree, list of (name, state, country))."""
    if not ski_polygons:
        return (None, [])
    try:
        from shapely import STRtree
    except ImportError:
        return (None, [])
    geoms = [poly for poly, *_ in ski_polygons]
    meta = [(name, state, country) for _, name, state, country in ski_polygons]
    tree = STRtree(geoms)
    return (tree, meta)


def _ski_area_at_point_indexed(
    lat: float, lon: float,
    tree: Any,
    meta: List[Tuple[str, Optional[str], Optional[str]]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (ski_area_name, state, country) using STRtree; returns first containing polygon."""
    if tree is None or not meta:
        return (None, None, None)
    try:
        from shapely.geometry import Point
    except ImportError:
        return (None, None, None)
    pt = Point(lon, lat)
    idx = tree.query(pt)
    try:
        indices = idx.tolist()
    except AttributeError:
        indices = [idx] if isinstance(idx, int) else list(idx)
    for i in indices:
        i = int(i)
        if i < 0 or i >= len(meta):
            continue
        poly = tree.geometries[i]
        if poly is not None and poly.contains(pt):
            name, state, country = meta[i]
            return (name, state, country)
    return (None, None, None)


def enrich_geojson(
    geojson_path: Path,
    boundaries_dir: Path,
    ski_areas_path: Optional[Path] = None,
    is_ski_areas_file: bool = False,
    boundaries_cache: Optional[Tuple[Any, Any]] = None,
) -> None:
    """
    Add State, Country, Ski Area to each feature's properties.
    - State, Country from boundaries (centroid lookup).
    - Ski Area: if is_ski_areas_file, use feature's name; else if ski_areas_path, point-in-polygon; else null.
    - boundaries_cache: optional (countries_gdf, states_gdf) to avoid reloading shapefiles.
    """
    geojson_path = Path(geojson_path)
    boundaries_dir = Path(boundaries_dir)
    data = json.loads(geojson_path.read_text(encoding="utf-8"))
    if data.get("type") != "FeatureCollection":
        print("Not a FeatureCollection, skipping.", file=sys.stderr)
        return
    features = data.get("features", [])
    if not features:
        print("No features, skipping.", file=sys.stderr)
        return

    # Load boundaries once (use cache if provided)
    if boundaries_cache is not None:
        countries_gdf, states_gdf = boundaries_cache
    else:
        countries_gdf, states_gdf = _load_boundaries(boundaries_dir)

    # For lifts/pistes: load ski area polygons once and build spatial index
    ski_polygons: List[Tuple[Any, str, Optional[str], Optional[str]]] = []
    ski_tree, ski_meta = None, []
    if ski_areas_path and Path(ski_areas_path).exists() and not is_ski_areas_file:
        ski_polygons = _load_ski_area_polygons(Path(ski_areas_path))
        print(f"Loaded {len(ski_polygons)} ski area polygons for Ski Area point-in-polygon", file=sys.stderr)
        ski_tree, ski_meta = _build_ski_area_index(ski_polygons)

    # Collect centroids for batch country/state lookup
    with_centroid: List[Tuple[int, float, float]] = []
    for i, f in enumerate(features):
        try:
            centroid = _centroid_from_geojson_geometry(f.get("geometry"))
        except Exception:
            centroid = None
        if centroid:
            with_centroid.append((i, centroid[0], centroid[1]))

    # Single batch lookup for all country/state
    centroids_only = [(lat, lon) for _, lat, lon in with_centroid]
    batch_results = _batch_lookup_country_state(centroids_only, countries_gdf, states_gdf) if centroids_only else []
    result_by_idx = {with_centroid[j][0]: batch_results[j] for j in range(len(with_centroid))}

    # Assign properties
    for i, f in enumerate(features):
        props = dict(f.get("properties") or {})
        try:
            centroid = _centroid_from_geojson_geometry(f.get("geometry"))
        except Exception:
            centroid = None
        if not centroid:
            props["State"] = None
            props["Country"] = None
            props["Ski Area"] = props.get("Ski Area") if is_ski_areas_file else None
            f["properties"] = props
            continue
        lat, lon = centroid
        try:
            country, state = result_by_idx.get(i, (None, None))
            props["State"] = state
            props["Country"] = country
            if is_ski_areas_file:
                props["Ski Area"] = props.get("name") or props.get("Name") or None
            else:
                if ski_tree is not None and ski_meta:
                    ski_area_name, _, _ = _ski_area_at_point_indexed(lat, lon, ski_tree, ski_meta)
                else:
                    ski_area_name, _, _ = _ski_area_at_point(lat, lon, ski_polygons) if ski_polygons else (None, None, None)
                props["Ski Area"] = ski_area_name
            f["properties"] = props
        except Exception as e:
            print(f"Warning: feature {i}: {e}", file=sys.stderr)
            props["State"] = None
            props["Country"] = None
            props["Ski Area"] = None
            f["properties"] = props

    geojson_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Enriched {len(features)} features in {geojson_path.name}")


def _get_element_centroid(elem: dict) -> Optional[Tuple[float, float]]:
    """Get (lat, lon) from an OSM element (node or way with geometry)."""
    if elem.get("lat") is not None and elem.get("lon") is not None:
        return (float(elem["lat"]), float(elem["lon"]))
    geom = elem.get("geometry")
    if not geom or not isinstance(geom, list):
        return None
    if len(geom) == 1:
        p = geom[0]
        return (float(p["lat"]), float(p["lon"]))
    if len(geom) < 2:
        return None
    lats = [p["lat"] for p in geom]
    lons = [p["lon"] for p in geom]
    return (sum(lats) / len(lats), sum(lons) / len(lons))


def _load_ski_area_state_country_by_name(geojson_path: Path) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """Load ski_areas.geojson and return dict: name -> (state, country)."""
    path = Path(geojson_path)
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("type") != "FeatureCollection" or not data.get("features"):
        return {}
    out = {}
    for f in data.get("features", []):
        props = f.get("properties") or {}
        name = props.get("name") or props.get("Name") or props.get("Ski Area")
        if not name:
            continue
        state = props.get("State") or props.get("state")
        country = props.get("Country") or props.get("country")
        out[name] = (state, country)
    return out


def enrich_osm_nearby_json(
    json_path: Path,
    boundaries_dir: Path,
    ski_areas_path: Optional[Path] = None,
    boundaries_cache: Optional[Tuple[Any, Any]] = None,
) -> None:
    """
    Add State, Country, and Ski Area to each element in osm_near_winter_sports.json.
    State/Country from boundaries (centroid lookup); fallback to ski_areas.geojson by winter_sports_name.
    Ski Area from winter_sports_name.
    """
    json_path = Path(json_path)
    boundaries_dir = Path(boundaries_dir)
    if boundaries_cache is not None:
        countries_gdf, states_gdf = boundaries_cache
    else:
        countries_gdf, states_gdf = _load_boundaries(boundaries_dir)
    name_to_state_country = _load_ski_area_state_country_by_name(ski_areas_path) if ski_areas_path else {}
    data = json.loads(json_path.read_text(encoding="utf-8"))
    elements = data.get("elements", [])
    if not elements:
        print("No elements in JSON, skipping.", file=sys.stderr)
        return
    # First pass: assign from ski area name where possible; collect (idx, lat, lon) for boundary lookup
    need_lookup: List[Tuple[int, float, float]] = []
    for i, elem in enumerate(elements):
        state, country = None, None
        name = elem.get("winter_sports_name") or elem.get("Ski Area")
        if name and name_to_state_country and name in name_to_state_country:
            sa_state, sa_country = name_to_state_country[name]
            state, country = sa_state, sa_country
            elem["State"] = state
            elem["Country"] = country
        if state is None or country is None:
            centroid = _get_element_centroid(elem)
            if centroid:
                lat, lon = centroid
                need_lookup.append((i, lat, lon))
    centroids = [(lat, lon) for _, lat, lon in need_lookup]
    batch_results = _batch_lookup_country_state(centroids, countries_gdf, states_gdf) if centroids else []
    for k, (i, _, _) in enumerate(need_lookup):
        if k < len(batch_results):
            country, state = batch_results[k]
            elements[i]["State"] = state
            elements[i]["Country"] = country
    for elem in elements:
        if "State" not in elem:
            elem["State"] = None
        if "Country" not in elem:
            elem["Country"] = None
        elem["Ski Area"] = elem.get("Ski Area") or elem.get("winter_sports_name")
    json_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Enriched {len(elements)} elements in {json_path.name}")


def _run_enrich_all(data_dir: Path, boundaries_dir: Path) -> None:
    """Run all four enrich steps in one process; exit(1) on first failure."""
    data_dir = Path(data_dir)
    boundaries_dir = Path(boundaries_dir)
    ski_areas_path = data_dir / "ski_areas.geojson"
    lifts_path = data_dir / "lifts.geojson"
    pistes_path = data_dir / "pistes.geojson"
    osm_path = data_dir / "osm_near_winter_sports.json"
    pipeline_start = time.perf_counter()

    # Load boundaries once for all steps (avoids hundreds of shapefile reads)
    boundaries_cache = _load_boundaries(boundaries_dir)

    def step(n: int, total: int, name: str, fn, *args, **kwargs) -> None:
        sys.stdout.flush()
        sys.stderr.flush()
        step_start = time.perf_counter()
        print(f"Enrich step {n}/{total}: {name} ...", file=sys.stderr)
        sys.stderr.flush()
        try:
            fn(*args, **kwargs)
            elapsed = time.perf_counter() - step_start
            print(f"  step {n}/{total} done in {elapsed:.1f}s", file=sys.stderr)
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception as e:
            print(f"Enrich failed at step {n}/{total} '{name}': {e}", file=sys.stderr)
            sys.stderr.flush()
            raise

    step(1, 4, "ski_areas.geojson", enrich_geojson, ski_areas_path, boundaries_dir, None, is_ski_areas_file=True, boundaries_cache=boundaries_cache)
    step(2, 4, "lifts.geojson", enrich_geojson, lifts_path, boundaries_dir, ski_areas_path, is_ski_areas_file=False, boundaries_cache=boundaries_cache)
    step(3, 4, "pistes.geojson", enrich_geojson, pistes_path, boundaries_dir, ski_areas_path, is_ski_areas_file=False, boundaries_cache=boundaries_cache)
    step(4, 4, "osm_near_winter_sports.json", enrich_osm_nearby_json, osm_path, boundaries_dir, ski_areas_path, boundaries_cache=boundaries_cache)
    total_elapsed = time.perf_counter() - pipeline_start
    print(f"All 4 enrich steps completed in {total_elapsed:.1f}s", file=sys.stderr)
    sys.stderr.flush()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Add State, Country, Ski Area to GeoJSON/OSM JSON")
    sub = p.add_subparsers(dest="cmd", help="Command")
    # GeoJSON mode
    pg = sub.add_parser("geojson", help="Enrich a GeoJSON file")
    pg.add_argument("geojson", help="Path to GeoJSON file (modified in place)")
    pg.add_argument("-b", "--boundaries", default="boundaries", help="Boundaries directory")
    pg.add_argument("-s", "--ski-areas", help="Path to ski_areas.geojson for point-in-polygon Ski Area")
    pg.add_argument("--is-ski-areas", action="store_true", help="Input is ski_areas.geojson (Ski Area = name)")
    # OSM JSON mode (osm_near_winter_sports.json)
    po = sub.add_parser("osm", help="Enrich osm_near_winter_sports.json")
    po.add_argument("json_path", help="Path to osm_near_winter_sports.json")
    po.add_argument("-b", "--boundaries", default="boundaries", help="Boundaries directory")
    po.add_argument("-s", "--ski-areas", help="Path to ski_areas.geojson (for State/Country fallback by name)")
    # All-in-one (for Docker: one process, clear ordering, exit 1 on any failure)
    pa = sub.add_parser("all", help="Enrich ski_areas, lifts, pistes, osm_near_winter_sports in /data")
    pa.add_argument("-d", "--data-dir", default="/data", help="Directory containing the four files")
    pa.add_argument("-b", "--boundaries", default="/boundaries", help="Boundaries directory")
    args = p.parse_args()
    if args.cmd == "osm":
        enrich_osm_nearby_json(
            Path(args.json_path),
            Path(args.boundaries),
            Path(args.ski_areas) if getattr(args, "ski_areas", None) else None,
        )
    elif args.cmd == "geojson":
        enrich_geojson(
            Path(args.geojson),
            Path(args.boundaries),
            Path(args.ski_areas) if args.ski_areas else None,
            is_ski_areas_file=args.is_ski_areas,
        )
    elif args.cmd == "all":
        _run_enrich_all(Path(args.data_dir), Path(args.boundaries))
    else:
        p.print_help()
        sys.exit(1)
