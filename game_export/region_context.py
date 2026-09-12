"""Natural Earth 10m context (roads, water, places) for wiki region clay scenes.

Clipped to the admin polygon and written in the same local-meter CRS as resorts.geojson.
"""
from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path
from typing import Optional
from urllib.request import urlopen

import geopandas as gpd
import pandas as pd
from shapely.geometry import mapping

from game_export.config import REPO_ROOT
from game_export.coords import LocalCRS, geom_to_local, geom_to_projected, make_transformers
from game_export.region_dem import shift_positive_lons_west, unwrap_dateline_west
from game_export.vectors import write_local_geojson

log = logging.getLogger("game_export")

BOUNDARIES_DIR = REPO_ROOT / "boundaries"
NE_BASE = "https://naciscdn.org/naturalearth/10m"
NE_LAYERS = {
    "roads": ("cultural", "ne_10m_roads"),
    "rivers": ("physical", "ne_10m_rivers_lake_centerlines"),
    "lakes": ("physical", "ne_10m_lakes"),
    "places": ("cultural", "ne_10m_populated_places"),
}

MAX_HIGHWAYS = 400
MAX_RIVERS = 200
MAX_LAKES = 80
MAX_PLACES = 80
MIN_LAKE_M2 = 500_000.0
MIN_HIGHWAY_M = 400.0
MIN_RIVER_M = 800.0
SIMPLIFY_M = 200.0

ROAD_TO_HIGHWAY = {
    "major highway": "motorway",
    "beltway": "trunk",
    "secondary highway": "primary",
    "road": "secondary",
    "ferry route": None,
    "track": None,
}


def _local_from_manifest(manifest: dict) -> LocalCRS:
    cs = manifest["coordinate_system"]
    return LocalCRS(
        source_crs=str(cs.get("source_crs") or "EPSG:4326"),
        projected_crs=str(cs["projected_crs"]),
        origin_easting_m=float(cs["origin_easting_m"]),
        origin_northing_m=float(cs["origin_northing_m"]),
        origin_longitude=float(cs["origin_longitude"]),
        origin_latitude=float(cs["origin_latitude"]),
    )


def _ensure_ne_layer(theme: str, name: str) -> Path:
    shp = BOUNDARIES_DIR / f"{name}.shp"
    if shp.is_file():
        return shp
    url = f"{NE_BASE}/{theme}/{name}.zip"
    log.info("Downloading Natural Earth %s", url)
    BOUNDARIES_DIR.mkdir(parents=True, exist_ok=True)
    with urlopen(url, timeout=120) as resp:
        data = resp.read()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        zf.extractall(BOUNDARIES_DIR)
    if not shp.is_file():
        raise FileNotFoundError(f"Expected {shp} after extracting {url}")
    return shp


def _read_ne(theme: str, name: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(_ensure_ne_layer(theme, name))
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def _col(row, *names: str) -> Optional[str]:
    for n in names:
        if n in row and row[n] not in (None, ""):
            s = str(row[n]).strip()
            if s and s.lower() not in {"nan", "none", "null"}:
                return s
    return None


def _highway_class(row) -> Optional[str]:
    raw = _col(row, "type", "TYPE", "featurecla", "FEATURECLA")
    if not raw:
        return "primary"
    mapped = ROAD_TO_HIGHWAY.get(raw.lower())
    if mapped is None and raw.lower() in ROAD_TO_HIGHWAY:
        return None
    if mapped:
        return mapped
    # US interstates often labeled Interstate / Federal
    low = raw.lower()
    if "interstate" in low or "motorway" in low:
        return "motorway"
    if "federal" in low or "trunk" in low or "us highway" in low:
        return "trunk"
    if "state" in low:
        return "primary"
    if "secondary" in low:
        return "primary"
    return "secondary"


def _place_kind(row) -> str:
    feat = (_col(row, "FEATURECLA", "featurecla") or "").lower()
    pop = row.get("POP_MAX")
    if pop is None:
        pop = row.get("pop_max")
    try:
        pop_n = float(pop) if pop is not None and pop == pop else 0.0
    except (TypeError, ValueError):
        pop_n = 0.0
    if "capital" in feat or pop_n >= 50000:
        return "city"
    return "town"


def _iter_line_parts(geom):
    if geom is None or geom.is_empty:
        return
    t = geom.geom_type
    if t == "LineString":
        yield geom
    elif t == "MultiLineString":
        yield from geom.geoms
    elif t == "GeometryCollection":
        for part in geom.geoms:
            yield from _iter_line_parts(part)


def _iter_poly_parts(geom):
    if geom is None or geom.is_empty:
        return
    t = geom.geom_type
    if t == "Polygon":
        yield geom
    elif t == "MultiPolygon":
        yield from geom.geoms
    elif t == "GeometryCollection":
        for part in geom.geoms:
            yield from _iter_poly_parts(part)


def _clip_to_local(geom, clip_proj, to_proj, local: LocalCRS, simplify_m: float):
    if geom is None or geom.is_empty:
        return None
    g = geom_to_projected(geom, to_proj)
    if not g.is_valid:
        g = g.buffer(0)
    try:
        g = g.intersection(clip_proj)
    except Exception:
        g = g.buffer(0).intersection(clip_proj)
    if g is None or g.is_empty:
        return None
    if simplify_m > 0:
        g = g.simplify(simplify_m, preserve_topology=True)
    if g.is_empty:
        return None
    return geom_to_local(g, local)


def bake_ne_context_into_scene(
    out: Path,
    *,
    admin_wgs,
    local: LocalCRS,
    region_id: str,
    cache_dir: Path | None = None,
    bbox_wgs: tuple[float, float, float, float] | None = None,
) -> dict[str, int]:
    del cache_dir, bbox_wgs, region_id
    to_proj, _to_wgs = make_transformers(local.projected_crs)
    clip_wgs = unwrap_dateline_west(admin_wgs)
    clip_proj = geom_to_projected(clip_wgs, to_proj)
    if not clip_proj.is_valid:
        clip_proj = clip_proj.buffer(0)
    clip_lines = clip_proj.buffer(300.0)
    minx, miny, maxx, maxy = clip_wgs.bounds
    unwrap_features = minx < -180.0

    def _subset(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if not unwrap_features:
            return gdf.cx[minx:maxx, miny:maxy]
        west = gdf.cx[-180.0:maxx, miny:maxy]
        east = gdf.cx[(minx + 360.0):180.0, miny:maxy]
        parts = [p for p in (west, east) if p is not None and not p.empty]
        if not parts:
            return gdf.iloc[0:0]
        out = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=gdf.crs)
        out["geometry"] = [shift_positive_lons_west(g) for g in out.geometry]
        return out

    roads = _subset(_read_ne(*NE_LAYERS["roads"]))
    highway_feats: list[dict] = []
    for _, row in roads.iterrows():
        hw = _highway_class(row)
        if not hw:
            continue
        loc = _clip_to_local(row.geometry, clip_lines, to_proj, local, SIMPLIFY_M)
        if loc is None:
            continue
        for part in _iter_line_parts(loc):
            # length in local meters ≈ projected meters
            if part.length < MIN_HIGHWAY_M:
                continue
            highway_feats.append(
                {
                    "type": "Feature",
                    "geometry": mapping(part),
                    "properties": {
                        "highway": hw,
                        "name": _col(row, "name", "NAME", "name_en"),
                        "_len": float(part.length),
                    },
                }
            )
    highway_feats.sort(key=lambda f: -f["properties"]["_len"])
    highway_feats = highway_feats[:MAX_HIGHWAYS]
    for f in highway_feats:
        f["properties"].pop("_len", None)

    rivers = _subset(_read_ne(*NE_LAYERS["rivers"]))
    river_feats: list[dict] = []
    for _, row in rivers.iterrows():
        loc = _clip_to_local(row.geometry, clip_lines, to_proj, local, SIMPLIFY_M)
        if loc is None:
            continue
        for part in _iter_line_parts(loc):
            if part.length < MIN_RIVER_M:
                continue
            river_feats.append(
                {
                    "type": "Feature",
                    "geometry": mapping(part),
                    "properties": {
                        "kind": "river",
                        "name": _col(row, "name", "NAME", "name_en"),
                        "_len": float(part.length),
                    },
                }
            )
    river_feats.sort(key=lambda f: -f["properties"]["_len"])
    river_feats = river_feats[:MAX_RIVERS]
    for f in river_feats:
        f["properties"].pop("_len", None)

    lakes = _subset(_read_ne(*NE_LAYERS["lakes"]))
    lake_feats: list[dict] = []
    for _, row in lakes.iterrows():
        loc = _clip_to_local(row.geometry, clip_proj, to_proj, local, SIMPLIFY_M)
        if loc is None:
            continue
        for part in _iter_poly_parts(loc):
            if part.area < MIN_LAKE_M2:
                continue
            lake_feats.append(
                {
                    "type": "Feature",
                    "geometry": mapping(part),
                    "properties": {
                        "kind": "lake",
                        "name": _col(row, "name", "NAME", "name_en"),
                        "_area": float(part.area),
                    },
                }
            )
    lake_feats.sort(key=lambda f: -f["properties"]["_area"])
    lake_feats = lake_feats[:MAX_LAKES]
    for f in lake_feats:
        f["properties"].pop("_area", None)

    places = _subset(_read_ne(*NE_LAYERS["places"]))
    place_feats: list[dict] = []
    for _, row in places.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        pt = geom if geom.geom_type == "Point" else geom.centroid
        if not clip_wgs.covers(pt) and not clip_wgs.intersects(pt):
            continue
        name = _col(row, "NAME", "name", "NAMEASCII", "nameascii")
        if not name:
            continue
        pop = row.get("POP_MAX")
        if pop is None:
            pop = row.get("pop_max")
        try:
            pop_n = float(pop) if pop is not None and pop == pop else 0.0
        except (TypeError, ValueError):
            pop_n = 0.0
        gproj = geom_to_projected(pt, to_proj)
        loc = geom_to_local(gproj, local)
        place_feats.append(
            {
                "type": "Feature",
                "geometry": mapping(loc),
                "properties": {
                    "name": name,
                    "place": _place_kind(row),
                    "lon": float(pt.x),
                    "lat": float(pt.y),
                    "_pop": pop_n,
                },
            }
        )
    place_feats.sort(key=lambda f: -f["properties"]["_pop"])
    place_feats = place_feats[:MAX_PLACES]
    for f in place_feats:
        f["properties"].pop("_pop", None)

    vec = out / "vectors"
    write_local_geojson(vec / "highways.geojson", highway_feats, local, "highways")
    write_local_geojson(vec / "water.geojson", river_feats + lake_feats, local, "water")
    write_local_geojson(vec / "places.geojson", place_feats, local, "places")
    counts = {
        "highways": len(highway_feats),
        "water": len(river_feats) + len(lake_feats),
        "places": len(place_feats),
    }
    log.info(
        "Region NE context %s: highways=%s water=%s (rivers=%s lakes=%s) places=%s",
        out.name,
        counts["highways"],
        counts["water"],
        len(river_feats),
        len(lake_feats),
        counts["places"],
    )
    return counts


# Compat alias used by region_clay
bake_osm_into_scene = bake_ne_context_into_scene
