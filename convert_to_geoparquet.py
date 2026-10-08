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
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon, LineString
from shapely import make_valid


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
    if not rows:
        gdf = gpd.GeoDataFrame(columns=["osm_type", "osm_id", "winter_sports_id", "winter_sports_name", "country", "state", "tags", "geometry"], crs="EPSG:4326")
    else:
        gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
    gdf.to_parquet(output_path, index=False)
    print(f"Saved {len(gdf)} OSM elements to {output_path}")
    return output_path


def _sanitize_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fix or drop invalid geometries so GeoParquet write succeeds (valid LinearRings)."""
    from shapely import is_valid

    def _is_acceptable(geom):
        if geom is None or geom.is_empty:
            return False
        if geom.geom_type in ("Point", "LineString"):
            return True
        if geom.geom_type == "MultiLineString":
            return all(_is_acceptable(p) for p in geom.geoms)
        if geom.geom_type == "Polygon":
            if geom.exterior is None or len(geom.exterior.coords) < 3:
                return False
            for ring in geom.interiors:
                if len(ring.coords) < 3:
                    return False
            return True
        if geom.geom_type == "MultiPolygon":
            return all(_is_acceptable(p) for p in geom.geoms)
        return False

    geoms = []
    dropped: Dict[str, int] = {}
    for idx, geom in enumerate(gdf.geometry):
        if geom is None or geom.is_empty:
            geoms.append(None)
            continue
        if not _is_acceptable(geom):
            try:
                fixed = make_valid(geom)
                if fixed is not None and not fixed.is_empty and _is_acceptable(fixed):
                    geoms.append(fixed)
                    continue
            except Exception:
                pass
            geoms.append(None)
            dropped[geom.geom_type] = dropped.get(geom.geom_type, 0) + 1
        else:
            geoms.append(geom)
    if dropped:
        print(f"  Dropped invalid geometries by type: {dropped}", file=sys.stderr)
    gdf = gdf.copy()
    gdf["geometry"] = geoms
    gdf = gdf[gdf.geometry.notna()].copy()
    return gdf


PROMOTED_TAGS = ("name", "aerialway", "piste:type", "piste:difficulty")
_HSTORE_PAIR = re.compile(r'"((?:[^"\\]|\\.)*)"=>"((?:[^"\\]|\\.)*)"')


def parse_other_tags(text: Any) -> Dict[str, str]:
    """Parse GDAL's OSM other_tags hstore string ("k"=>"v","k2"=>"v2") into a dict."""
    if not isinstance(text, str) or "=>" not in text:
        return {}
    unescape = lambda s: s.replace('\\"', '"').replace("\\\\", "\\")
    return {unescape(k): unescape(v) for k, v in _HSTORE_PAIR.findall(text)}


def _blank(series: pd.Series) -> pd.Series:
    return series.isna() | series.astype(str).str.strip().isin(["", "nan", "None", "<NA>"])


def _id_text(series: pd.Series) -> pd.Series:
    """OSM ids as plain digit strings ('123', not '123.0'); blanks become NA."""
    num = pd.to_numeric(series, errors="coerce")
    return num.map(lambda v: str(int(v)) if pd.notna(v) else pd.NA).astype("object")


def normalize_osm_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Give every lift/piste row one stable id and real tag columns.
    - osm_type: node | way | relation
    - osm_id: the element id for every row (GDAL leaves it empty on closed-way polygons)
    - osm_uid: "<osm_type>/<osm_id>", unique across element types
    - name, aerialway, piste:type, piste:difficulty filled from other_tags when the column is empty
    """
    gdf = gdf.reset_index(drop=True)
    if len(gdf) == 0:
        for col in ("osm_type", "osm_id", "osm_uid") + PROMOTED_TAGS:
            if col not in gdf.columns:
                gdf[col] = pd.Series(dtype="object")
        return gdf

    for col in ("osm_id", "osm_way_id", "osm_relation_id", "osm_type"):
        if col not in gdf.columns:
            gdf[col] = pd.NA
    node_or_line = _id_text(gdf["osm_id"])
    way_id = _id_text(gdf["osm_way_id"])
    rel_id = _id_text(gdf["osm_relation_id"])
    geom_type = pd.Series(gdf.geom_type.values, index=gdf.index)

    known = gdf["osm_type"].where(~_blank(gdf["osm_type"])).astype("object")
    inferred = pd.Series(pd.NA, index=gdf.index, dtype="object")
    inferred[geom_type.eq("Point")] = "node"
    inferred[geom_type.isin(["LineString", "MultiLineString"])] = "way"
    poly = geom_type.isin(["Polygon", "MultiPolygon"])
    inferred[poly & way_id.notna()] = "way"
    inferred[poly & way_id.isna()] = "relation"
    inferred[rel_id.notna() & way_id.isna()] = "relation"
    osm_type = known.fillna(inferred)

    osm_id = node_or_line.mask(osm_type.eq("way") & way_id.notna(), way_id)
    osm_id = osm_id.mask(osm_type.eq("relation") & rel_id.notna(), rel_id)
    osm_id = osm_id.fillna(way_id).fillna(rel_id)

    gdf["osm_type"] = osm_type
    gdf["osm_id"] = osm_id
    gdf["osm_uid"] = (osm_type.astype(str) + "/" + osm_id.astype(str)).where(
        osm_type.notna() & osm_id.notna()
    )

    tags = gdf["other_tags"] if "other_tags" in gdf.columns else pd.Series(None, index=gdf.index)
    for key in PROMOTED_TAGS:
        if key not in gdf.columns:
            gdf[key] = pd.NA
        missing = _blank(gdf[key])
        needle = f'"{key}"=>'
        candidates = missing & tags.astype(str).str.contains(needle, regex=False, na=False)
        if candidates.any():
            gdf.loc[candidates, key] = tags[candidates].map(lambda t: parse_other_tags(t).get(key))
        gdf[key] = gdf[key].where(~_blank(gdf[key])).astype("object")
    return gdf


def _utm_epsg(lon: float, lat: float) -> int:
    zone = int((lon + 180) // 6) % 60 + 1
    return (32700 if lat < 0 else 32600) + zone


def assign_ski_areas(
    gdf: gpd.GeoDataFrame,
    ski_areas: gpd.GeoDataFrame,
    buffer_m: float = 2000.0,
) -> gpd.GeoDataFrame:
    """
    Set "Ski Area" from the ski area polygons. Lines are tested at both endpoints, other
    geometries at a representative point. A candidate area must lie within buffer_m of a
    tested point. Ranking: most tested points inside the polygon, then nearest, then the
    smallest polygon (so a lift wholly inside a nested sub-area takes the sub-area's name).
    Rows with no area within buffer_m keep their previous value. Country/State are filled
    from the area when missing.
    """
    from shapely import STRtree, distance, get_point

    gdf = gdf.reset_index(drop=True)
    for col in ("Ski Area", "Country", "State"):
        if col not in gdf.columns:
            gdf[col] = pd.NA
    if len(gdf) == 0 or ski_areas is None or len(ski_areas) == 0:
        return gdf

    areas = ski_areas[ski_areas.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    if areas.crs is None:
        areas = areas.set_crs("EPSG:4326")
    areas = areas.to_crs("EPSG:4326")
    name = areas["Ski Area"] if "Ski Area" in areas.columns else pd.Series(pd.NA, index=areas.index)
    if "name" in areas.columns:
        name = name.where(~_blank(name), areas["name"])
    areas["_area_name"] = name
    areas = areas[~_blank(areas["_area_name"])].reset_index(drop=True)
    if len(areas) == 0:
        return gdf
    for col in ("Country", "State"):
        if col not in areas.columns:
            areas[col] = pd.NA

    src = gdf.to_crs("EPSG:4326") if gdf.crs is not None else gdf.set_crs("EPSG:4326")
    geoms = src.geometry.values
    rows, pts = [], []
    for i, g in enumerate(geoms):
        if g is None or g.is_empty:
            continue
        if g.geom_type == "LineString":
            rows += [i, i]
            pts += [get_point(g, 0), get_point(g, -1)]
        elif g.geom_type == "MultiLineString":
            parts = list(g.geoms)
            rows += [i, i]
            pts += [get_point(parts[0], 0), get_point(parts[-1], -1)]
        else:
            rows.append(i)
            pts.append(g.representative_point())
    if not pts:
        return gdf
    probe = gpd.GeoDataFrame({"row": rows}, geometry=pts, crs="EPSG:4326")
    probe["epsg"] = [_utm_epsg(p.x, p.y) for p in probe.geometry]

    pad = 0.1 + buffer_m / 50000.0
    hits = []
    for epsg, grp in probe.groupby("epsg"):
        minx, miny, maxx, maxy = grp.total_bounds
        near = areas.cx[minx - pad : maxx + pad, miny - pad : maxy + pad]
        if len(near) == 0:
            continue
        near_m = near.to_crs(epsg)
        grp_m = grp.to_crs(epsg)
        tree = STRtree(near_m.geometry.values)
        p_idx, a_idx = tree.query(grp_m.geometry.values, predicate="dwithin", distance=buffer_m)
        if len(p_idx) == 0:
            continue
        d = distance(grp_m.geometry.values[p_idx], near_m.geometry.values[a_idx])
        hits.append(pd.DataFrame({
            "row": grp["row"].values[p_idx],
            "area": near.index.values[a_idx],
            "dist": d,
            "area_m2": near_m.geometry.area.values[a_idx],
        }))
    if not hits:
        return gdf

    cand = pd.concat(hits, ignore_index=True)
    cand["inside"] = cand["dist"] <= 1.0
    per = cand.groupby(["row", "area"], as_index=False).agg(
        inside=("inside", "sum"), dist=("dist", "min"), area_m2=("area_m2", "first")
    )
    best = (
        per.sort_values(["row", "inside", "dist", "area_m2"], ascending=[True, False, True, True])
        .drop_duplicates("row")
    )
    target = gdf.index[best["row"].values]
    picked = areas.loc[best["area"].values]
    gdf.loc[target, "Ski Area"] = picked["_area_name"].values
    for col in ("Country", "State"):
        fill = _blank(gdf.loc[target, col]).values
        gdf.loc[target[fill], col] = picked[col].values[fill]
    return gdf


def _read_ski_areas(data_dir: Path) -> Optional[gpd.GeoDataFrame]:
    for name in ("ski_areas.parquet", "ski_areas.geojson"):
        path = data_dir / name
        if path.exists():
            try:
                return gpd.read_parquet(path) if path.suffix == ".parquet" else gpd.read_file(path)
            except Exception as e:
                print(f"Warning: could not read {path}: {e}", file=sys.stderr)
    return None


def normalize_lifts_pistes(gdf: gpd.GeoDataFrame, ski_areas: Optional[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """Stable ids, promoted tag columns, and endpoint/buffer Ski Area for lifts and pistes."""
    gdf = normalize_osm_columns(gdf)
    if ski_areas is not None:
        gdf = assign_ski_areas(gdf, ski_areas)
    return gdf


def normalize_parquet_dir(data_dir: Union[str, Path]) -> None:
    """Rewrite lifts.parquet and pistes.parquet in data_dir in place with normalize_lifts_pistes."""
    data_dir = Path(data_dir)
    ski_areas = _read_ski_areas(data_dir)
    for name in ("lifts.parquet", "pistes.parquet"):
        path = data_dir / name
        if not path.exists():
            continue
        gdf = normalize_lifts_pistes(gpd.read_parquet(path), ski_areas)
        gdf.to_parquet(path, index=False)
        print(f"Normalized {len(gdf)} rows in {path}")


def geojson_to_geoparquet(geojson_path: Union[str, Path], output_path: Union[str, Path]) -> Path:
    """Convert a GeoJSON file to GeoParquet. Invalid geometries are fixed or dropped."""
    geojson_path = Path(geojson_path)
    output_path = Path(output_path)
    if not geojson_path.exists():
        raise FileNotFoundError(geojson_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf = gpd.read_file(geojson_path)
    if not gdf.crs:
        gdf.set_crs("EPSG:4326", inplace=True)
    gdf = gdf.to_crs("EPSG:4326")
    n_before = len(gdf)
    gdf = _sanitize_geometries(gdf)
    n_after = len(gdf)
    if n_before > n_after:
        print(f"  Dropped {n_before - n_after} feature(s) with invalid geometry", file=sys.stderr)
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


def _merge_ski_orientation_into_analyzed(data_dir: Path) -> None:
    """
    Enrich ski_areas_analyzed.parquet with ski_north_angle/map_rotation_deg.

    Source is ski_areas_elevation.parquet produced by scripts/ski_area_elevation_contours.py.
    Join key is winter_sports_id (+ region when present in both tables).
    """
    analyzed_path = data_dir / "ski_areas_analyzed.parquet"
    elevation_path = data_dir / "ski_areas_elevation.parquet"
    if not analyzed_path.exists() or not elevation_path.exists():
        return

    analyzed = pd.read_parquet(analyzed_path)
    elev = pd.read_parquet(elevation_path)

    if "winter_sports_id" not in analyzed.columns or "winter_sports_id" not in elev.columns:
        return
    if "ski_north_angle" not in elev.columns:
        return

    join_cols = ["winter_sports_id"]
    if "region" in analyzed.columns and "region" in elev.columns:
        join_cols.append("region")

    # Normalize join dtypes across pipeline outputs (csv/parquet can differ)
    analyzed = analyzed.copy()
    elev = elev.copy()
    analyzed["winter_sports_id"] = analyzed["winter_sports_id"].astype(str)
    elev["winter_sports_id"] = elev["winter_sports_id"].astype(str)
    if "region" in join_cols:
        analyzed["region"] = analyzed["region"].astype(str)
        elev["region"] = elev["region"].astype(str)

    orient_cols = join_cols + ["ski_north_angle"]
    orient = elev[orient_cols].drop_duplicates(subset=join_cols)

    merged = analyzed.merge(orient, on=join_cols, how="left", suffixes=("", "_elev"))

    # Coalesce legacy columns from previous runs into a single canonical field
    if "ski_north_angle" not in merged.columns:
        if "ski_north_angle_x" in merged.columns and "ski_north_angle_y" in merged.columns:
            merged["ski_north_angle"] = merged["ski_north_angle_x"].combine_first(
                merged["ski_north_angle_y"]
            )
        elif "ski_north_angle_x" in merged.columns:
            merged["ski_north_angle"] = merged["ski_north_angle_x"]
        elif "ski_north_angle_y" in merged.columns:
            merged["ski_north_angle"] = merged["ski_north_angle_y"]
        elif "ski_north_angle_elev" in merged.columns:
            merged["ski_north_angle"] = merged["ski_north_angle_elev"]
    elif "ski_north_angle_elev" in merged.columns:
        merged["ski_north_angle"] = merged["ski_north_angle"].combine_first(
            merged["ski_north_angle_elev"]
        )

    if "ski_north_angle" in merged.columns:
        merged["map_rotation_deg"] = merged["ski_north_angle"].round(1)

    merged.to_parquet(analyzed_path, index=False)
    matched = (
        int(merged["ski_north_angle"].notna().sum())
        if "ski_north_angle" in merged.columns
        else 0
    )
    print(
        f"Enriched {analyzed_path.name} with ski_north_angle/map_rotation_deg "
        f"(matched {matched}/{len(merged)} rows)"
    )


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

    try:
        normalize_parquet_dir(data_dir)
    except Exception as e:
        print(f"Warning: failed to normalize lifts/pistes parquet: {e}", file=sys.stderr)

    # Optional enrichment: carry per-resort map bearing into analyzed parquet
    try:
        _merge_ski_orientation_into_analyzed(data_dir)
    except Exception as e:
        print(
            f"Warning: failed to enrich ski_areas_analyzed.parquet with orientation: {e}",
            file=sys.stderr,
        )


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

    p_norm = sub.add_parser(
        "normalize",
        help="Rewrite lifts.parquet/pistes.parquet in a dir: stable osm_uid, promoted tags, Ski Area by endpoint",
    )
    p_norm.add_argument("-d", "--data-dir", required=True, help="Directory with lifts.parquet, pistes.parquet, ski_areas.parquet")

    args = parser.parse_args()

    if args.cmd == "normalize":
        normalize_parquet_dir(Path(args.data_dir))
    elif args.cmd == "ski":
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
