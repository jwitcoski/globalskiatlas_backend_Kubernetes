#!/usr/bin/env python3
"""One resort: live Overpass, existing pipeline scripts, patch region + combined parquet.

Env: ACTION (update|add|delete|batch), WINTER_SPORTS_ID, REGION, NAME, COUNTRY, STATE,
LAT, LON, JOB_ID, S3_BUCKET, BOUNDARIES, OVERPASS_URL.

ACTION=batch first asks Overpass for named landuse=winter_sports ways and
relations. Any id missing from combined/ski_areas.parquet, the delete skip list,
and the job inbox is queued as an add. DISCOVER_MAX (default 8) caps how many
new resorts one night starts. A state that is not its own extract, such as
Shizuoka inside Chubu, is filed under the Geofabrik region that contains it.
A country with no extract is added when Geofabrik publishes one. One resort
that fails is recorded and removed from the queue so the rest of the batch runs.

Set DATA_ROOT to a local output directory (the Docker host volume) to read and
write that catalog with no S3, DynamoDB, or CloudFront calls.
"""
from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
TABLES = (
    "ski_areas.parquet",
    "lifts.parquet",
    "pistes.parquet",
    "osm_near_winter_sports.parquet",
    "ski_areas_analyzed.parquet",
    "ski_areas_1000ft_buffer.parquet",
    "ski_area_contours.parquet",
    "ski_area_elevation_points.parquet",
    "ski_areas_elevation.parquet",
)
ID_COLUMNS = ("winter_sports_id", "osm_way_id", "osm_id", "osm_relation_id")
RADIUS_M = 305


def _match(df: pd.DataFrame, wid: str) -> pd.Series:
    wid = str(wid)
    mask = pd.Series(False, index=df.index)
    for col in ID_COLUMNS:
        if col in df.columns:
            mask = mask | (df[col].astype(str) == wid)
    return mask


def replace_resort_rows(existing: pd.DataFrame | None, incoming: pd.DataFrame | None, wid: str) -> pd.DataFrame:
    """Drop wid from existing and append incoming rows for that id (or all incoming rows)."""
    if existing is None or len(existing) == 0:
        base = incoming
        if base is None:
            return pd.DataFrame()
        hit = _match(base, wid)
        return (base.loc[hit] if hit.any() else base).reset_index(drop=True)
    kept = existing.loc[~_match(existing, wid)]
    if incoming is None or len(incoming) == 0:
        return kept.reset_index(drop=True)
    hit = _match(incoming, wid)
    add = incoming.loc[hit] if hit.any() else incoming
    if "osm_uid" in kept.columns and "osm_uid" in add.columns:
        kept = kept.loc[~kept["osm_uid"].isin(add["osm_uid"].dropna())]
    return pd.concat([kept, add], ignore_index=True)


def overpass(query: str, url: str | None = None) -> dict:
    endpoints = [url] if url else []
    endpoints.append(os.environ.get("OVERPASS_URL") or "https://overpass-api.de/api/interpreter")
    endpoints.append("https://overpass.kumi.systems/api/interpreter")
    endpoints.append("https://overpass.openstreetmap.fr/api/interpreter")
    seen = []
    last = None
    for endpoint in endpoints:
        if not endpoint or endpoint in seen:
            continue
        seen.append(endpoint)
        data = urllib.parse.urlencode({"data": query}).encode()
        req = urllib.request.Request(
            endpoint,
            data=data,
            headers={"User-Agent": "globalskiatlas-one-resort/1.0", "Accept": "application/json"},
        )
        for attempt in range(1, 4):
            try:
                with urllib.request.urlopen(req, timeout=180) as resp:
                    return json.loads(resp.read().decode())
            except Exception as exc:
                last = exc
                print(f"overpass {endpoint} attempt {attempt}/3: {exc}", flush=True)
                if attempt < 3:
                    time.sleep(20)
    raise last


def _coords(el: dict) -> list[tuple[float, float]]:
    geom = el.get("geometry") or []
    return [(p["lon"], p["lat"]) for p in geom if "lon" in p and "lat" in p]


def element_feature(el: dict) -> dict | None:
    tags = dict(el.get("tags") or {})
    et = el.get("type")
    if et == "node" and "lon" in el and "lat" in el:
        geom = {"type": "Point", "coordinates": [el["lon"], el["lat"]]}
    else:
        coords = _coords(el)
        if len(coords) < 2:
            return None
        if coords[0] == coords[-1] and len(coords) >= 4:
            geom = {"type": "Polygon", "coordinates": [coords]}
        else:
            geom = {"type": "LineString", "coordinates": coords}
    tags["osm_id"] = el.get("id")
    if et == "way":
        tags["osm_way_id"] = el.get("id")
    elif et == "relation":
        tags["osm_relation_id"] = el.get("id")
    tags["osm_type"] = et
    return {"type": "Feature", "geometry": geom, "properties": tags}


def _write_fc(path: Path, features: list) -> None:
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8")


def _bbox(el: dict) -> tuple[float, float, float, float] | None:
    b = el.get("bounds")
    if b:
        return b["minlon"], b["minlat"], b["maxlon"], b["maxlat"]
    coords = _coords(el)
    if not coords and "lon" in el:
        coords = [(el["lon"], el["lat"])]
    if not coords:
        return None
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return min(lons), min(lats), max(lons), max(lats)


def _expand(bbox, meters: float) -> tuple[float, float, float, float]:
    minlon, minlat, maxlon, maxlat = bbox
    lat = (minlat + maxlat) / 2
    dlat = meters / 111320.0
    dlon = meters / (111320.0 * max(0.2, math.cos(math.radians(lat))))
    return minlon - dlon, minlat - dlat, maxlon + dlon, maxlat + dlat


def fetch_winter_sports(wid: str) -> dict | None:
    data = overpass(f"[out:json][timeout:90];(way({wid});relation({wid}););out geom;")
    for el in data.get("elements") or []:
        tags = el.get("tags") or {}
        if tags.get("landuse") == "winter_sports" or tags.get("leisure") == "ski_resort":
            return el
    els = data.get("elements") or []
    return els[0] if els else None


_COUNTRY_REGION_SLUG = {
    "united states of america": "us",
    "united states": "us",
    "israel": "israel-and-palestine",
    "palestine": "israel-and-palestine",
}


def _hyphen(text: str) -> str:
    folded = unicodedata.normalize("NFKD", text or "")
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "-", folded.lower()).strip("-")


def _regions_module():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "regions_list", REPO / "scripts" / "list_regions_for_pipeline.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _region_paths() -> list[str]:
    return [row[0] for row in _regions_module().load_region_rows()]


def _region_rows() -> list[tuple[str, str]]:
    return [(row[0], row[1]) for row in _regions_module().load_region_rows()]


# Natural Earth often names a French department. Pipeline regions are the older regions.
_FRANCE_DEPT = {
    "ain": "rhone-alpes", "ardeche": "rhone-alpes", "drome": "rhone-alpes", "isere": "rhone-alpes",
    "loire": "rhone-alpes", "rhone": "rhone-alpes", "savoie": "rhone-alpes", "haute-savoie": "rhone-alpes",
    "bas-rhin": "alsace", "haut-rhin": "alsace",
    "dordogne": "aquitaine", "gironde": "aquitaine", "landes": "aquitaine", "lot-et-garonne": "aquitaine",
    "pyrenees-atlantiques": "aquitaine",
    "allier": "auvergne", "cantal": "auvergne", "haute-loire": "auvergne", "puy-de-dome": "auvergne",
    "calvados": "basse-normandie", "manche": "basse-normandie", "orne": "basse-normandie",
    "cote-d-or": "bourgogne", "nievre": "bourgogne", "saone-et-loire": "bourgogne", "yonne": "bourgogne",
    "cotes-d-armor": "bretagne", "finistere": "bretagne", "ille-et-vilaine": "bretagne", "morbihan": "bretagne",
    "cher": "centre", "eure-et-loir": "centre", "indre": "centre", "indre-et-loire": "centre",
    "loir-et-cher": "centre", "loiret": "centre",
    "ardennes": "champagne-ardenne", "aube": "champagne-ardenne", "marne": "champagne-ardenne",
    "haute-marne": "champagne-ardenne",
    "corse-du-sud": "corse", "haute-corse": "corse",
    "doubs": "franche-comte", "jura": "franche-comte", "haute-saone": "franche-comte",
    "territoire-de-belfort": "franche-comte",
    "eure": "haute-normandie", "seine-maritime": "haute-normandie",
    "paris": "ile-de-france", "seine-et-marne": "ile-de-france", "yvelines": "ile-de-france",
    "essonne": "ile-de-france", "hauts-de-seine": "ile-de-france", "seine-saint-denis": "ile-de-france",
    "val-de-marne": "ile-de-france", "val-d-oise": "ile-de-france",
    "aude": "languedoc-roussillon", "gard": "languedoc-roussillon", "herault": "languedoc-roussillon",
    "lozere": "languedoc-roussillon", "pyrenees-orientales": "languedoc-roussillon",
    "correze": "limousin", "creuse": "limousin", "haute-vienne": "limousin",
    "meurthe-et-moselle": "lorraine", "meuse": "lorraine", "moselle": "lorraine", "vosges": "lorraine",
    "ariege": "midi-pyrenees", "aveyron": "midi-pyrenees", "haute-garonne": "midi-pyrenees",
    "gers": "midi-pyrenees", "lot": "midi-pyrenees", "hautes-pyrenees": "midi-pyrenees",
    "tarn": "midi-pyrenees", "tarn-et-garonne": "midi-pyrenees",
    "nord": "nord-pas-de-calais", "pas-de-calais": "nord-pas-de-calais",
    "loire-atlantique": "pays-de-la-loire", "maine-et-loire": "pays-de-la-loire",
    "mayenne": "pays-de-la-loire", "sarthe": "pays-de-la-loire", "vendee": "pays-de-la-loire",
    "aisne": "picardie", "oise": "picardie", "somme": "picardie",
    "charente": "poitou-charentes", "charente-maritime": "poitou-charentes",
    "deux-sevres": "poitou-charentes", "vienne": "poitou-charentes",
    "alpes-de-haute-provence": "provence-alpes-cote-d-azur", "hautes-alpes": "provence-alpes-cote-d-azur",
    "alpes-maritimes": "provence-alpes-cote-d-azur", "bouches-du-rhone": "provence-alpes-cote-d-azur",
    "var": "provence-alpes-cote-d-azur", "vaucluse": "provence-alpes-cote-d-azur",
}

# Natural Earth names prefectures. Pipeline regions are the Geofabrik extracts.
_JAPAN_PREF = {
    "hokkaido": "hokkaido",
    "aomori": "tohoku", "iwate": "tohoku", "miyagi": "tohoku", "akita": "tohoku",
    "yamagata": "tohoku", "fukushima": "tohoku",
    "ibaraki": "kanto", "tochigi": "kanto", "gunma": "kanto", "gumma": "kanto",
    "saitama": "kanto", "chiba": "kanto", "tokyo": "kanto", "kanagawa": "kanto",
    "niigata": "chubu", "toyama": "chubu", "ishikawa": "chubu", "fukui": "chubu",
    "yamanashi": "chubu", "nagano": "chubu", "gifu": "chubu", "shizuoka": "chubu", "aichi": "chubu",
    "mie": "kansai", "shiga": "kansai", "kyoto": "kansai", "osaka": "kansai",
    "hyogo": "kansai", "nara": "kansai", "wakayama": "kansai",
    "tottori": "chugoku", "shimane": "chugoku", "okayama": "chugoku",
    "hiroshima": "chugoku", "yamaguchi": "chugoku",
    "tokushima": "shikoku", "kagawa": "shikoku", "ehime": "shikoku", "kochi": "shikoku",
    "fukuoka": "kyushu", "saga": "kyushu", "nagasaki": "kyushu", "kumamoto": "kyushu",
    "oita": "kyushu", "miyazaki": "kyushu", "kagoshima": "kyushu", "okinawa": "kyushu",
}

# Countries left out of config/regions.yaml on purpose.
_EXCLUDED_COUNTRY_SLUGS = {
    "vietnam", "cambodia", "indonesia", "laos", "thailand", "philippines",
    "malaysia", "singapore", "brunei", "bangladesh", "east-timor", "timor-leste",
    "malta", "monaco",
}
_FULL_CONTINENTS = {"africa", "south-america", "australia-oceania"}
_CONTINENT_SLUG = {
    "asia": "asia",
    "europe": "europe",
    "africa": "africa",
    "north america": "north-america",
    "south america": "south-america",
    "oceania": "australia-oceania",
    "australia": "australia-oceania",
}


def _country_slug(country: str) -> str:
    return _COUNTRY_REGION_SLUG.get((country or "").lower(), _hyphen(country))


def _state_slug(country_slug: str, state: str) -> str:
    slug = _hyphen(state)
    if country_slug == "france":
        return _FRANCE_DEPT.get(slug, slug)
    if country_slug == "japan":
        for suffix in ("-prefecture", "-ken", "-fu", "-do", "-to"):
            if slug.endswith(suffix):
                slug = slug[: -len(suffix)]
        return _JAPAN_PREF.get(slug, slug)
    return slug


def region_for_place(country: str, state: str) -> str:
    """Match Natural Earth country and state names to a pipeline region path."""
    paths = _region_paths()
    country_slug = _country_slug(country)
    state_slug = _state_slug(country_slug, state)
    if state_slug:
        hits = [
            path for path in paths
            if path.endswith("/" + state_slug) and country_slug in path.split("/")
        ]
        if len(hits) == 1:
            return hits[0]
        if len(hits) > 1:
            raise SystemExit(f"several regions for {country} / {state}: {hits}")
    hits = [path for path in paths if path.split("/")[-1] == country_slug]
    if len(hits) == 1:
        return hits[0]
    if not hits:
        raise SystemExit(f"no region for {country} / {state}")
    raise SystemExit(f"several regions for {country}: {hits}")


def _parse_poly(text: str) -> list[tuple[list[tuple[float, float]], bool]]:
    """Geofabrik .poly rings as (lon, lat) with a hole flag."""
    rings = []
    current: list[tuple[float, float]] = []
    hole = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line == "END":
            if len(current) >= 3:
                rings.append((current, hole))
            current = []
            hole = False
            continue
        if line.startswith("!"):
            hole = True
            line = line[1:].strip()
            if not line:
                continue
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            current.append((float(parts[0]), float(parts[1])))
        except ValueError:
            continue
    if len(current) >= 3:
        rings.append((current, hole))
    return rings


def _point_in_ring(lon: float, lat: float, ring: list[tuple[float, float]]) -> bool:
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > lat) != (yj > lat):
            cross = (xj - xi) * (lat - yi) / (yj - yi) + xi
            if lon < cross:
                inside = not inside
        j = i
    return inside


def _rings_contain(lon: float, lat: float, rings: list[tuple[list[tuple[float, float]], bool]]) -> bool:
    outer = False
    hole = False
    for ring, is_hole in rings:
        if not _point_in_ring(lon, lat, ring):
            continue
        if is_hole:
            hole = True
        else:
            outer = True
    return outer and not hole


def _ring_area(ring: list[tuple[float, float]]) -> float:
    area = 0.0
    j = len(ring) - 1
    for i in range(len(ring)):
        area += (ring[j][0] + ring[i][0]) * (ring[j][1] - ring[i][1])
        j = i
    return abs(area) / 2


_POLY_CACHE: dict[str, list[tuple[list[tuple[float, float]], bool]] | None] = {}


def _poly_url(pbf_url: str) -> str | None:
    if "download.geofabrik.de" not in (pbf_url or ""):
        return None
    if not pbf_url.endswith("-latest.osm.pbf"):
        return None
    return pbf_url[: -len("-latest.osm.pbf")] + ".poly"


def _load_poly(pbf_url: str) -> list[tuple[list[tuple[float, float]], bool]] | None:
    url = _poly_url(pbf_url)
    if not url:
        return None
    if url in _POLY_CACHE:
        return _POLY_CACHE[url]
    cache_dir = Path(os.environ.get("DATA_ROOT") or "/tmp") / "geofabrik-polys"
    cache_file = cache_dir / (re.sub(r"[^a-z0-9]+", "-", url.lower()).strip("-") + ".poly")
    text = ""
    if cache_file.is_file():
        text = cache_file.read_text(encoding="utf-8")
    else:
        req = urllib.request.Request(url, headers={"User-Agent": "globalskiatlas-one-resort/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                text = resp.read().decode("utf-8", "replace")
        except Exception as exc:
            print("poly", url, exc, flush=True)
            _POLY_CACHE[url] = None
            return None
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(text, encoding="utf-8")
        except OSError:
            pass
    rings = _parse_poly(text)
    _POLY_CACHE[url] = rings or None
    return _POLY_CACHE[url]


def region_containing(country: str, lat: float, lon: float) -> str | None:
    """Pick the existing Geofabrik extract whose outline contains this resort."""
    country_slug = _country_slug(country)
    if not country_slug:
        return None
    hits = []
    for path, pbf_url in _region_rows():
        if country_slug not in path.split("/"):
            continue
        rings = _load_poly(pbf_url)
        if not rings or not _rings_contain(lon, lat, rings):
            continue
        area = sum(_ring_area(ring) for ring, is_hole in rings if not is_hole)
        hits.append((area, path))
    if not hits:
        return None
    hits.sort()
    return hits[0][1]


def _url_exists(url: str) -> bool:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "globalskiatlas-one-resort/1.0", "Range": "bytes=0-0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status in (200, 206)
    except Exception:
        return False


def _continent_slug(lat: float, lon: float, boundaries: Path) -> str | None:
    shp = boundaries / "ne_10m_admin_0_countries.shp"
    if not shp.is_file():
        return None
    try:
        import geopandas as gpd
        from shapely.geometry import Point
    except ImportError:
        return None
    try:
        gdf = gpd.read_file(shp)
        if not gdf.crs:
            gdf.set_crs("EPSG:4326", inplace=True)
        gdf = gdf.to_crs("EPSG:4326")
        joined = gpd.sjoin(
            gpd.GeoDataFrame([{"geometry": Point(lon, lat)}], crs="EPSG:4326"),
            gdf,
            how="left",
            predicate="within",
        )
        idx = joined["index_right"].iloc[0]
        if idx is None or (isinstance(idx, float) and math.isnan(idx)):
            return None
        name = str(gdf.loc[idx].get("CONTINENT") or "").lower()
        return _CONTINENT_SLUG.get(name)
    except Exception as exc:
        print("continent lookup", exc, flush=True)
        return None


def _extra_region_files() -> list[Path]:
    paths = [REPO / "config" / "extra-regions.yaml"]
    root = os.environ.get("DATA_ROOT")
    if root:
        paths.append(Path(root) / "extra-regions.yaml")
    return paths


def _append_extra_region(region: str, pbf_url: str) -> None:
    import yaml

    item = {"region": region, "pbf_url": pbf_url}
    wrote = False
    for path in _extra_region_files():
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            current = yaml.safe_load(path.read_text(encoding="utf-8")) if path.is_file() else []
            current = current or []
            if isinstance(current, dict):
                current = current.get("regions") or []
            if any(str(row.get("region")) == region for row in current):
                wrote = True
                continue
            current.append(item)
            path.write_text(yaml.safe_dump(current, sort_keys=False), encoding="utf-8")
            wrote = True
            print("added pipeline region", region, pbf_url, flush=True)
        except OSError as exc:
            print("could not write", path, exc, flush=True)
    if not wrote:
        raise SystemExit(f"could not store new region {region}")


def add_country_region(country: str, lat: float, lon: float, boundaries: Path) -> str | None:
    """Add a Geofabrik country extract when the pipeline has no region for it."""
    slug = _country_slug(country)
    if not slug or slug in _EXCLUDED_COUNTRY_SLUGS:
        return None
    paths = _region_paths()
    if any(slug in path.split("/") for path in paths):
        return None
    continent = _continent_slug(lat, lon, boundaries)
    if continent in _FULL_CONTINENTS and continent in paths:
        print("region", continent, "full continent for", country, flush=True)
        return continent
    tries = []
    if continent:
        tries.append(continent)
    if continent == "north-america":
        tries.append("central-america")
    for name in ("europe", "asia", "north-america", "south-america", "africa", "australia-oceania", "central-america"):
        if name not in tries:
            tries.append(name)
    for name in tries:
        url = f"https://download.geofabrik.de/{name}/{slug}-latest.osm.pbf"
        if not _url_exists(url):
            continue
        region = f"{name}/{slug}"
        _append_extra_region(region, url)
        return region
    return None


def region_for_resort(country: str, state: str, lat: float, lon: float, boundaries: Path) -> str:
    """Region path for a newly found resort, adding a country extract when needed."""
    try:
        return region_for_place(country, state)
    except SystemExit as exc:
        reason = exc
    found = region_containing(country, lat, lon)
    if found:
        print("region from extract outline", found, flush=True)
        return found
    added = add_country_region(country, lat, lon, boundaries)
    if added:
        return added
    raise SystemExit(reason)


def _centroid(el: dict) -> tuple[float, float]:
    pts = _coords(el)
    if pts:
        return sum(p[1] for p in pts) / len(pts), sum(p[0] for p in pts) / len(pts)
    bounds = el.get("bounds") or {}
    if "minlat" in bounds and "minlon" in bounds:
        return (bounds["minlat"] + bounds["maxlat"]) / 2, (bounds["minlon"] + bounds["maxlon"]) / 2
    if "lat" in el and "lon" in el:
        return float(el["lat"]), float(el["lon"])
    raise SystemExit("winter sports element has no location")


def search_by_name(name: str, state: str, country: str, lat: str, lon: str) -> list[dict]:
    safe = name.replace('"', "")
    if lat and lon:
        clause = f'(around:40000,{lat},{lon})'
        area = ""
    else:
        admin = state or country
        if not admin:
            raise SystemExit("add needs state, country, or lat/lon")
        area = f'area["name"="{admin}"]->.searchArea;'
        clause = "(area.searchArea)"
    query = (
        f'[out:json][timeout:90];{area}'
        f'(way["landuse"="winter_sports"]["name"~"{safe}",i]{clause};'
        f'relation["landuse"="winter_sports"]["name"~"{safe}",i]{clause};);out ids;'
    )
    return [el for el in (overpass(query).get("elements") or []) if el.get("type") in ("way", "relation")]


def fetch_nearby(el: dict, wid: str) -> list[dict]:
    bbox = _expand(_bbox(el), RADIUS_M)
    s, w, n, e = bbox[1], bbox[0], bbox[3], bbox[2]
    data = overpass(f"[out:json][timeout:180];(node({s},{w},{n},{e});way({s},{w},{n},{e});relation({s},{w},{n},{e}););out geom;")
    name = (el.get("tags") or {}).get("name") or ""
    out = []
    for item in data.get("elements") or []:
        item = dict(item)
        item["winter_sports_id"] = int(wid) if str(wid).isdigit() else wid
        item["winter_sports_type"] = el.get("type") or "way"
        item["winter_sports_name"] = name
        out.append(item)
    return out


def write_extracts(work: Path, resort: dict, nearby: list[dict]) -> None:
    feat = element_feature(resort)
    if not feat:
        raise SystemExit("winter sports element has no geometry")
    _write_fc(work / "ski_areas.geojson", [feat])
    lifts, pistes = [], []
    for el in nearby:
        tags = el.get("tags") or {}
        f = element_feature(el)
        if not f:
            continue
        if tags.get("aerialway"):
            lifts.append(f)
        if tags.get("piste:type"):
            pistes.append(f)
    _write_fc(work / "lifts.geojson", lifts)
    _write_fc(work / "pistes.geojson", pistes)
    (work / "osm_near_winter_sports.json").write_text(
        json.dumps({"elements": nearby}), encoding="utf-8"
    )


def _run(cmd: list[str]) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True, cwd=REPO)


def build_local(work: Path, wid: str, region: str, boundaries: str) -> None:
    py = sys.executable
    _run([py, "scripts/enrich_geojson_properties.py", "all", "-d", str(work), "-b", boundaries])
    _run([
        py, "analyze_ski_areas.py", str(work / "ski_areas.geojson"),
        str(work / "osm_near_winter_sports.json"),
        "-o", str(work / "ski_areas_analyzed.csv"),
        "-b", boundaries, "-r", region,
    ])
    _run([py, "convert_to_geoparquet.py", "osm", "-i", str(work / "osm_near_winter_sports.json"), "-o", str(work / "osm_near_winter_sports.parquet")])
    _run([py, "convert_to_geoparquet.py", "all", "-d", str(work)])
    _run([py, "scripts/ski_area_1000ft_buffer.py", "-d", str(work)])
    _run([
        py, "scripts/translate_resort_names.py",
        "-i", str(work / "ski_areas_analyzed.parquet"),
        "-o", str(work / "ski_areas_analyzed.parquet"),
        "--cache", str(work / "name_translations.json"),
    ])
    _run([
        py, "scripts/ski_area_elevation_contours.py",
        "-i", str(work / "ski_areas.parquet"), "-o", str(work),
        "--cache-dir", str(work / "cache"), "--save-dem", "--ids-file", _ids_file(work, wid),
    ])
    csv = work / "ski_areas_analyzed.csv"
    pd.read_parquet(work / "ski_areas_analyzed.parquet").to_csv(csv, index=False)


def _ids_file(work: Path, wid: str) -> str:
    path = work / "ids.json"
    path.write_text(json.dumps({"candidates": [{"winter_sports_id": wid}]}), encoding="utf-8")
    return str(path)


def _read_table(path: Path) -> pd.DataFrame | None:
    if not path.is_file():
        return None
    if path.suffix == ".csv":
        return pd.read_csv(path)
    try:
        import geopandas as gpd
        return gpd.read_parquet(path)
    except Exception:
        return pd.read_parquet(path)


def _write_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".csv":
        df.to_csv(path, index=False)
        return
    out = df.copy()
    for col in out.columns:
        if col == "geometry" or out[col].dtype != object:
            continue
        out[col] = out[col].map(lambda x: "" if pd.isna(x) else str(x))
    out.to_parquet(path, index=False)


def _local_root() -> Path | None:
    raw = os.environ.get("DATA_ROOT", "").strip()
    return Path(raw) if raw else None


def _s3():
    import boto3
    return boto3.client("s3")


def _store_bytes(s3, bucket: str, key: str, body: bytes, content_type: str = "application/json") -> None:
    root = _local_root()
    if root:
        dest = root / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
        return
    if s3:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def _store_file(s3, bucket: str, key: str, src: Path) -> None:
    root = _local_root()
    if root:
        dest = root / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        return
    if s3:
        s3.upload_file(str(src), bucket, key)


def _remove_key(s3, bucket: str, key: str) -> None:
    root = _local_root()
    if root:
        (root / key).unlink(missing_ok=True)
        return
    if s3:
        s3.delete_object(Bucket=bucket, Key=key)


def _read_json_key(s3, bucket: str, key: str) -> dict | None:
    root = _local_root()
    if root:
        path = root / key
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    if not s3:
        return None
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
    except Exception:
        return None


def _inbox_keys(s3, bucket: str, folder: str = "inbox") -> list[str]:
    root = _local_root()
    if root:
        inbox = root / "resort-jobs" / folder
        if not inbox.is_dir():
            return []
        return [f"resort-jobs/{folder}/{path.name}" for path in sorted(inbox.glob("*.json"))]
    keys = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": f"resort-jobs/{folder}/"}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        keys.extend(obj["Key"] for obj in page.get("Contents") or [] if obj["Key"].endswith(".json"))
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    return keys


def _download(s3, bucket: str, key: str, dest: Path) -> bool:
    from botocore.exceptions import ClientError

    dest.parent.mkdir(parents=True, exist_ok=True)
    root = _local_root()
    if root:
        src = root / key
        if not src.is_file():
            return False
        shutil.copy2(src, dest)
        return True
    if s3 is None:
        return False
    try:
        s3.download_file(bucket, key, str(dest))
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def _upload_dot_catalog(s3, bucket: str, parquet_path: Path) -> None:
    """Main-map dots. The site used to read a MapTiler copy that this job never wrote."""
    df = pd.read_parquet(parquet_path)
    if "centroid_lon" not in df.columns or "centroid_lat" not in df.columns:
        return
    ok = df["centroid_lon"].notna() & df["centroid_lat"].notna()
    df = df.loc[ok]
    features = []
    for row in df.to_dict(orient="records"):
        lon = float(row.pop("centroid_lon"))
        lat = float(row.pop("centroid_lat"))
        props = {}
        for key, value in row.items():
            if value is None or (isinstance(value, float) and value != value):
                continue
            if hasattr(value, "item"):
                value = value.item()
            props[key] = value
        props["centroid_lon"] = lon
        props["centroid_lat"] = lat
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
        })
    body = json.dumps({
        "type": "FeatureCollection",
        "name": "ski_areas_analyzed",
        "features": features,
    }).encode()
    s3.put_object(
        Bucket=bucket,
        Key="combined/ski_areas_analyzed.geojson",
        Body=body,
        ContentType="application/geo+json",
        CacheControl="public, max-age=60",
    )


def patch_prefix(s3, bucket: str, prefix: str, work: Path, wid: str, publish: bool) -> None:
    scratch = work / "remote" / prefix.replace("/", "_")
    for name in TABLES:
        local = work / name
        incoming = _read_table(local) if local.is_file() else None
        if incoming is None and publish:
            continue
        remote = scratch / name
        key = f"{prefix.strip('/')}/{name}"
        had = _download(s3, bucket, key, remote)
        existing = _read_table(remote) if had else None
        if publish and existing is None and prefix.strip("/") == "combined":
            raise SystemExit(f"refusing to replace s3://{bucket}/{key} with a single resort")
        merged = replace_resort_rows(existing, incoming if publish else None, wid)
        out = scratch / f"out_{name}"
        _write_table(merged, out)
        _store_file(s3, bucket, key, out)
    csv_name = "ski_areas_analyzed.csv"
    local_csv = work / csv_name
    if publish and local_csv.is_file():
        remote = scratch / csv_name
        key = f"{prefix.strip('/')}/{csv_name}"
        had = _download(s3, bucket, key, remote)
        merged = replace_resort_rows(_read_table(remote) if had else None, pd.read_csv(local_csv), wid)
        out = scratch / f"out_{csv_name}"
        _write_table(merged, out)
        _store_file(s3, bucket, key, out)
    if prefix.strip("/") == "combined" and s3 and not _local_root():
        merged_analyzed = scratch / "out_ski_areas_analyzed.parquet"
        if merged_analyzed.is_file():
            _upload_dot_catalog(s3, bucket, merged_analyzed)
    dem = work / "dems" / prefix / f"{wid}.tif"
    if not dem.is_file():
        dem = next((work / "dems").rglob(f"{wid}.tif"), None) if (work / "dems").is_dir() else None
    if publish and dem and dem.is_file():
        rel = dem.relative_to(work).as_posix()
        _store_file(s3, bucket, f"{prefix.strip('/')}/{rel}", dem)


def write_run_status(s3, bucket: str) -> None:
    """One file to check: resort-jobs/status.json."""
    root = _local_root()
    if root is None or s3:
        return
    jobs = root / "resort-jobs"
    jobs.mkdir(parents=True, exist_ok=True)
    recent = []
    counts = {"succeeded": 0, "failed": 0, "running": 0, "queued": 0}
    for path in jobs.glob("*.json"):
        if path.name in ("status.json", "region-cursor.json") or path.name.startswith("lock-"):
            continue
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        status = str(body.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        recent.append({
            "jobId": body.get("jobId") or path.stem,
            "status": status,
            "message": body.get("message") or "",
            "winter_sports_id": body.get("winter_sports_id") or "",
            "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(path.stat().st_mtime)),
        })
    recent.sort(key=lambda row: row["updated"], reverse=True)
    cursor = {}
    cursor_path = jobs / "region-cursor.json"
    if cursor_path.is_file():
        try:
            cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            cursor = {}
    paths = _region_paths()
    index = int(cursor.get("index") or 0)
    summary = {
        "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "lastRegion": cursor.get("region") or "",
        "nextRegion": paths[index % len(paths)] if paths else "",
        "regionIndex": index,
        "regionCount": len(paths),
        "counts": counts,
        "running": [row for row in recent if row["status"] == "running"][:5],
        "recent": recent[:20],
    }
    (jobs / "status.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("status", jobs / "status.json", flush=True)


def _put_job(s3, bucket: str, job_id: str, status: str, message: str, extra: dict | None = None) -> None:
    body = {"jobId": job_id, "status": status, "message": message}
    if extra:
        body.update(extra)
    if os.environ.get("WINTER_SPORTS_ID") and "winter_sports_id" not in body:
        body["winter_sports_id"] = os.environ.get("WINTER_SPORTS_ID")
    raw = json.dumps(body).encode()
    print(body, flush=True)
    if job_id and (s3 or _local_root()):
        _store_bytes(s3, bucket, f"resort-jobs/{job_id}.json", raw)
        write_run_status(s3, bucket)


def _slug(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return slug or "resort"


def _ensure_resort_yaml(wid: str, region: str, work: Path) -> Path:
    """Keep an existing resort file. Otherwise write the standard thresholds."""
    existing = _resort_yaml(wid)
    if existing:
        return existing
    name = os.environ.get("NAME") or wid
    state = os.environ.get("STATE") or ""
    country = os.environ.get("COUNTRY") or ""
    analyzed = work / "ski_areas_analyzed.parquet"
    if analyzed.is_file():
        frame = pd.read_parquet(analyzed)
        row = _row_for(frame, wid)
        if row:
            name = str(row.get("english_name") or row.get("name") or name)
            state = str(row.get("state") or state)
            country = str(row.get("country") or country)
            region = str(row.get("region") or region)
    resort_id = f"{_slug(name)}_{_slug(country)}"
    path = REPO / "config" / "resorts" / f"{resort_id}.yaml"
    if path.is_file():
        return path
    where = ", ".join(part for part in (state, country) if part)
    path.write_text(
        "\n".join([
            f"# {name} — OSM {wid}. Written by the resort job.",
            f"resort_id: {resort_id}",
            f"display_name: {name}",
            f'winter_sports_id: "{wid}"',
            f"region: {region}",
            f"state: {state}",
            f"country: {country}",
            f"approximate_location_name: {where or name}",
            "game_style: classic_arcade",
            "seed: 20260928",
            "status: prototype",
            "target_crs: auto_utm",
            "terrain_tile_size_m: 256",
            "terrain_mesh_resolution_m: 4",
            "heightfield_resolution_m: 2",
            "collision_heightfield_resolution_m: 4",
            "route_min_vertical_drop_m: 25",
            "route_max_uphill_fraction: 0.10",
            "route_min_length_m: 75",
            "piste_corridor_default_half_width_m: 18",
            "piste_corridor_min_half_width_m: 12",
            "piste_corridor_max_half_width_m: 35",
            "scene_bounds_buffer_m: 300",
            "route_sample_spacing_m: 8",
            "route_connect_endpoint_m: 25",
            "steep_hazard_degrees: 45",
            "building_buffer_m: 8",
            "water_buffer_m: 6",
            "road_buffer_m: 6",
            "cliff_buffer_m: 10",
            "terrain_boundary_buffer_m: 8",
            "highway_hazard_types:",
            "  - motorway",
            "  - trunk",
            "  - primary",
            "  - secondary",
            "  - tertiary",
            "  - residential",
            "  - service",
            "  - unclassified",
            "",
        ]),
        encoding="utf-8",
    )
    print("wrote", path.name, flush=True)
    return path


def _resort_yaml(wid: str) -> Path | None:
    root = REPO / "config" / "resorts"
    for path in root.glob("*.yaml"):
        text = path.read_text(encoding="utf-8")
        if f'"{wid}"' in text or f"winter_sports_id: {wid}" in text:
            return path
    return None


def _geom_changed(s3, bucket: str, region: str, wid: str, work: Path) -> bool:
    # ponytail: compare ski_areas bounds only; upgrade if a vertex-level edit must rebake clay
    new = _read_table(work / "ski_areas.parquet")
    if new is None or "geometry" not in getattr(new, "columns", []):
        return False
    old_path = work / "old_ski.geoparquet"
    if not _download(s3, bucket, f"{region}/ski_areas.parquet", old_path):
        return True
    old = _read_table(old_path)
    if old is None:
        return True
    def bounds(df):
        hit = df.loc[_match(df, wid)]
        if len(hit) == 0 or "geometry" not in hit:
            return None
        g = hit.geometry.iloc[0]
        return tuple(round(x, 5) for x in g.bounds)
    return bounds(old) != bounds(new)


def _append_skip(s3, bucket: str, wid: str, work: Path) -> None:
    key = "combined/resort_skip.json"
    dest = work / "resort_skip.json"
    ids: list[str] = []
    if _download(s3, bucket, key, dest):
        ids = [str(x) for x in json.loads(dest.read_text(encoding="utf-8")).get("winter_sports_ids") or []]
    if str(wid) not in ids:
        ids.append(str(wid))
    body = json.dumps({"winter_sports_ids": ids}).encode()
    dest.write_bytes(body)
    if s3 or _local_root():
        _store_bytes(s3, bucket, key, body)


def _drop_skip(s3, bucket: str, wid: str, work: Path) -> None:
    key = "combined/resort_skip.json"
    dest = work / "resort_skip.json"
    if not _download(s3, bucket, key, dest):
        return
    ids = [str(x) for x in json.loads(dest.read_text(encoding="utf-8")).get("winter_sports_ids") or [] if str(x) != str(wid)]
    body = json.dumps({"winter_sports_ids": ids}).encode()
    _store_bytes(s3, bucket, key, body)


def _delete_prefix(s3, bucket: str, prefix: str) -> None:
    if not s3:
        return
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents") or []]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")


def stage_scene_inputs(work: Path, region: str, wid: str) -> None:
    """game_export reads data-root/combined or data-root/<region>, not the work root."""
    dest = work / "combined"
    dest.mkdir(parents=True, exist_ok=True)
    for name in TABLES:
        src = work / name
        if src.is_file():
            shutil.copy2(src, dest / name)
    dem_root = work / "dems"
    dem = next(dem_root.rglob(f"{wid}.tif"), None) if dem_root.is_dir() else None
    if dem is None:
        return
    parts = [p for p in region.split("/") if p]
    dem_dest = dest / "dems" / Path(*parts) / f"{wid}.tif"
    dem_dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(dem, dem_dest)


def _state_dots() -> None:
    state = os.environ.get("STATE", "")
    if not state or _local_root():
        return
    country = os.environ.get("COUNTRY") or "United States of America"
    from atlas.map_gen.wiki_page_id import wiki_state_page_id

    page_id = wiki_state_page_id(state, country)
    scene = REPO / "output" / "clay_scenes" / "regions" / page_id
    if not (scene / "scene-manifest.json").is_file():
        bucket = os.environ.get("S3_BUCKET", "globalskiatlas-backend-k8s-output")
        prefix = f"clay_scenes/regions/{page_id}/"
        s3 = _s3()
        scene.mkdir(parents=True, exist_ok=True)
        token = None
        found = False
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            for obj in page.get("Contents") or []:
                found = True
                rel = obj["Key"][len(prefix):]
                dest = scene / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, obj["Key"], str(dest))
            if not page.get("IsTruncated"):
                break
            token = page.get("NextContinuationToken")
        if not found:
            print("state dots: no existing scene", page_id, flush=True)
            return
    _run([
        sys.executable, "scripts/bake_region_clay.py", "--osm-only",
        "--state", state, "--country", country, "--upload",
    ])


def _analyzed_frame(work: Path, kind: str) -> pd.DataFrame | None:
    path = work / "remote" / "combined" / f"{kind}ski_areas_analyzed.parquet"
    if kind == "out":
        path = work / "remote" / "combined" / "out_ski_areas_analyzed.parquet"
    elif kind == "before":
        path = work / "remote" / "combined" / "ski_areas_analyzed.parquet"
    if not path.is_file():
        return None
    return pd.read_parquet(path)


def _row_for(frame: pd.DataFrame | None, wid: str) -> dict | None:
    if frame is None or "winter_sports_id" not in frame.columns:
        return None
    hit = frame[frame["winter_sports_id"].astype(str) == str(wid)]
    if hit.empty:
        return None
    row = hit.iloc[0].to_dict()
    return {k: (None if isinstance(v, float) and v != v else (v.item() if hasattr(v, "item") else v)) for k, v in row.items()}


def _publish_wiki(s3, bucket: str, action: str, wid: str, work: Path) -> None:
    if os.environ.get("SKIP_WIKI") == "1" or s3 is None:
        return
    from decimal import Decimal

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "resort_copy",
        REPO / "scripts" / "generate_resort_copy_bedrock.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    wiki_page_id_from_row = mod.wiki_page_id_from_row

    source = _analyzed_frame(work, "before" if action == "delete" else "out")
    row = _row_for(source, wid)
    if row is None and action == "delete":
        state = os.environ.get("STATE", "")
        slug = re.sub(r"[^a-z0-9]+", "-", state.lower()).strip("-")
        page_id = f"{wid}-{slug}" if slug else ""
        if not page_id:
            print("wiki: no analyzed row", wid, flush=True)
            return
        ddb = __import__("boto3").client("dynamodb")
        table = os.environ.get("WIKI_TABLE", "atlas-WikiPages")
        ddb.delete_item(TableName=table, Key={"pageId": {"S": page_id}})
        print("wiki deleted", page_id, flush=True)
        return
    if row is None:
        print("wiki: no analyzed row", wid, flush=True)
        return
    page_id = wiki_page_id_from_row(row)
    table = os.environ.get("WIKI_TABLE", "atlas-WikiPages")
    ddb = __import__("boto3").client("dynamodb")
    if action == "delete":
        ddb.delete_item(TableName=table, Key={"pageId": {"S": page_id}})
        print("wiki deleted", page_id, flush=True)
        return
    lat = row.get("centroid_lat")
    lon = row.get("centroid_lon")
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    if action == "update":
        values = {
            ":u": {"S": now},
            ":w": {"S": str(wid)},
            ":d": {"S": str(row.get("downhill_trails") or "")},
            ":l": {"S": str(row.get("total_lifts") or "")},
        }
        expr = "SET updatedAt = :u, winterSportsId = :w, downhillTrails = :d, totalLifts = :l"
        if lat is not None and lon is not None:
            values[":lat"] = {"N": str(Decimal(str(lat)))}
            values[":lon"] = {"N": str(Decimal(str(lon)))}
            expr += ", centroidLat = :lat, centroidLon = :lon"
        ddb.update_item(
            TableName=table,
            Key={"pageId": {"S": page_id}},
            UpdateExpression=expr,
            ExpressionAttributeValues=values,
        )
        print("wiki updated", page_id, flush=True)
        return
    content = ""
    wiki_path = work / "wiki.json"
    if wiki_path.is_file():
        payload = json.loads(wiki_path.read_text(encoding="utf-8"))
        content = str(payload.get("final") or payload.get("contentMarkdown") or "")
        items = payload.get("items") or []
        if not content and items:
            content = str(items[0].get("contentMarkdown") or items[0].get("final") or "")
    item = {
        "pageId": {"S": page_id},
        "title": {"S": str(row.get("english_name") or row.get("name") or wid)},
        "content": {"S": content},
        "winterSportsId": {"S": str(wid)},
        "winterSportsType": {"S": str(row.get("winter_sports_type") or "way")},
        "country": {"S": str(row.get("country") or "")},
        "state": {"S": str(row.get("state") or "")},
        "region": {"S": str(row.get("region") or "")},
        "pageType": {"S": "resort"},
        "status": {"S": "published"},
        "createdAt": {"S": now},
        "updatedAt": {"S": now},
        "downhillTrails": {"S": str(row.get("downhill_trails") or "")},
        "totalLifts": {"S": str(row.get("total_lifts") or "")},
    }
    if lat is not None and lon is not None:
        item["centroidLat"] = {"N": str(Decimal(str(lat)))}
        item["centroidLon"] = {"N": str(Decimal(str(lon)))}
    ddb.put_item(TableName=table, Item=item)
    print("wiki put", page_id, flush=True)


def _publish_pmtiles(s3, bucket: str, work: Path) -> None:
    if os.environ.get("SKIP_PMTILES") == "1" or s3 is None:
        return
    src = work / "remote" / "combined"
    tile_in = work / "tile_input"
    tile_in.mkdir(parents=True, exist_ok=True)
    for name in TABLES:
        patched = src / f"out_{name}"
        if patched.is_file():
            shutil.copy2(patched, tile_in / name)
    if not (tile_in / "ski_areas_analyzed.parquet").is_file():
        raise SystemExit("pmtiles: combined analyzed parquet was not patched")
    out = work / "pmtiles"
    _run([
        sys.executable, "scripts/build_pmtiles.py",
        "--input-dir", str(tile_in),
        "--staging-dir", str(work / "pmtiles_staging"),
        "--output-dir", str(out),
        "--java-heap", os.environ.get("JAVA_HEAP", "6g"),
    ])
    for name in ("ski_overview.pmtiles", "ski_resort_detail.pmtiles"):
        s3.upload_file(
            str(out / name),
            bucket,
            f"pmtiles/{name}",
            ExtraArgs={"ContentType": "application/vnd.pmtiles", "CacheControl": "public, max-age=60"},
        )
        print("uploaded", name, flush=True)


def _merge_game_catalog(s3, bucket: str, local_catalog: Path) -> None:
    if s3 is None:
        return
    from game_export.catalog import merge_catalog_resorts

    local = json.loads(local_catalog.read_text(encoding="utf-8"))
    incoming = local.get("resorts") or []
    key = "game_scenes/catalog.json"
    remote: dict = {}
    remote_path = local_catalog.parent / "remote_catalog.json"
    if _download(s3, bucket, key, remote_path):
        remote = json.loads(remote_path.read_text(encoding="utf-8"))
    merged = merge_catalog_resorts(remote.get("resorts") or [], incoming)
    body = json.dumps({**remote, "resorts": merged}).encode()
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", CacheControl="public, max-age=60")
    print("game catalog", len(merged), flush=True)


def _invalidate_site(s3, wid: str) -> None:
    dist = os.environ.get("CLOUDFRONT_DISTRIBUTION_ID", "E3BDMTLYF8G4VB")
    if os.environ.get("SKIP_INVALIDATE") == "1" or not dist:
        return
    paths = ["/clay_scenes/catalog.json", "/scripts/map-config.js"]
    yaml = _resort_yaml(wid)
    if yaml:
        paths.append(f"/clay_scenes/{yaml.stem}/*")
    __import__("boto3").client("cloudfront").create_invalidation(
        DistributionId=dist,
        InvalidationBatch={
            "Paths": {"Quantity": len(paths), "Items": paths},
            "CallerReference": f"resort-{wid}-{os.environ.get('JOB_ID') or 'local'}-{int(__import__('time').time())}",
        },
    )
    print("invalidated", paths, flush=True)


def ids_in_frame(df: pd.DataFrame | None) -> set[str]:
    found: set[str] = set()
    if df is None or len(df) == 0:
        return found
    for col in ID_COLUMNS:
        if col in df.columns:
            found.update(x for x in df[col].dropna().astype(str) if x and x != "nan")
    return found


def new_named_ids(osm_ids: list[str], known: set[str], skipped: set[str], queued: set[str], limit: int) -> list[str]:
    """Named OSM winter_sports ids that this catalog has never ingested."""
    fresh = []
    for wid in osm_ids:
        wid = str(wid)
        if wid in known or wid in skipped or wid in queued or wid in fresh:
            continue
        fresh.append(wid)
        if len(fresh) >= limit:
            break
    return fresh


def _named_winter_sports_ids() -> list[str]:
    query = (
        "[out:json][timeout:180];"
        '(way["landuse"="winter_sports"]["name"];'
        'relation["landuse"="winter_sports"]["name"];);'
        "out ids;"
    )
    ids = []
    for el in overpass(query).get("elements") or []:
        if el.get("type") in ("way", "relation") and el.get("id") is not None:
            ids.append(str(el["id"]))
    return ids


def _queue_add(s3, bucket: str, wid: str) -> str:
    job_id = uuid.uuid4().hex
    queued = {"jobId": job_id, "status": "queued", "message": "add", "winter_sports_id": wid}
    _store_bytes(s3, bucket, f"resort-jobs/{job_id}.json", json.dumps(queued).encode())
    _store_bytes(s3, bucket, f"resort-jobs/lock-{wid}.json", json.dumps({"jobId": job_id}).encode())
    request = {
        "action": "add",
        "winter_sports_id": wid,
        "region": "",
        "name": "",
        "country": "",
        "state": "",
        "lat": "",
        "lon": "",
        "jobId": job_id,
    }
    _store_bytes(s3, bucket, f"resort-jobs/inbox/{job_id}.json", json.dumps(request).encode())
    print("queued add", wid, job_id, flush=True)
    return job_id


def discover_named_resorts(s3, bucket: str, work: Path) -> int:
    """Queue add jobs for named landuse=winter_sports that are not in the catalog."""
    if os.environ.get("SKIP_DISCOVER") == "1":
        return 0
    limit = int(os.environ.get("DISCOVER_MAX", "8"))
    known_path = work / "known_ski_areas.parquet"
    known: set[str] = set()
    if not _download(s3, bucket, "combined/ski_areas.parquet", known_path):
        print("discover: combined/ski_areas.parquet is missing, not queueing", flush=True)
        return 0
    known = ids_in_frame(_read_table(known_path))
    skip_path = work / "resort_skip.json"
    skipped: set[str] = set()
    if _download(s3, bucket, "combined/resort_skip.json", skip_path):
        skipped = {str(x) for x in json.loads(skip_path.read_text(encoding="utf-8")).get("winter_sports_ids") or []}
    queued: set[str] = set()
    for key in _inbox_keys(s3, bucket):
        body = _read_json_key(s3, bucket, key) or {}
        if body.get("winter_sports_id"):
            queued.add(str(body["winter_sports_id"]))
    osm_ids = _named_winter_sports_ids()
    fresh = new_named_ids(osm_ids, known, skipped, queued, limit)
    print(f"discover: {len(osm_ids)} named in OSM, {len(fresh)} new (cap {limit})", flush=True)
    for wid in fresh:
        _queue_add(s3, bucket, wid)
    return len(fresh)


def _resort_ids(frame: pd.DataFrame, region: str) -> list[str]:
    hit = frame.loc[frame["region"].astype(str) == region]
    ids: list[str] = []
    seen: set[str] = set()
    for col in ID_COLUMNS:
        if col not in hit.columns:
            continue
        for raw in hit[col].dropna().astype(str):
            if raw and raw != "nan" and raw not in seen:
                seen.add(raw)
                ids.append(raw)
    return ids


def sweep_region(s3, bucket: str, work: Path) -> int:
    """Queue an update for every resort in the next state or province.

    Empty regions are skipped in the same pass. The cursor then moves to the
    following region, so one pass covers one populated region.
    """
    paths = _region_paths()
    if not paths:
        print("sweep: no regions", flush=True)
        return 0
    cursor = _read_json_key(s3, bucket, "resort-jobs/region-cursor.json") or {}
    start = int(cursor.get("index") or 0) % len(paths)
    known_path = work / "sweep_ski_areas.parquet"
    if not _download(s3, bucket, "combined/ski_areas.parquet", known_path):
        print("sweep: combined/ski_areas.parquet is missing, not queueing", flush=True)
        return 0
    frame = _read_table(known_path)
    if frame is None or "region" not in frame.columns:
        print("sweep: ski areas have no region column", flush=True)
        return 0
    index = start
    ids: list[str] = []
    region = paths[index]
    for _ in range(len(paths)):
        region = paths[index]
        ids = _resort_ids(frame, region)
        if ids:
            break
        print("sweep: skip empty", region, flush=True)
        index = (index + 1) % len(paths)
    else:
        print("sweep: no resorts in any region", flush=True)
        return 0
    for wid in ids:
        _store_bytes(s3, bucket, f"resort-jobs/region-inbox/sweep-{region.replace('/', '-')}-{wid}.json", json.dumps({
            "action": "update",
            "winter_sports_id": wid,
            "region": region,
            "name": "",
            "country": "",
            "state": "",
            "lat": "",
            "lon": "",
            "jobId": f"sweep-{wid}",
        }).encode())
    nxt = (index + 1) % len(paths)
    _store_bytes(s3, bucket, "resort-jobs/region-cursor.json", json.dumps({"index": nxt, "region": region}).encode())
    print(f"sweep: queued {len(ids)} in {region}; next {paths[nxt]}", flush=True)
    write_run_status(s3, bucket)
    return len(ids)


def run_batch() -> int:
    """Find new named resorts, apply every queued resort, then rebuild the shared tiles once."""
    bucket = os.environ.get("S3_BUCKET", "globalskiatlas-backend-k8s-output")
    s3 = None if _local_root() else _s3()
    work = Path(os.environ.get("WORK_DIR", "/tmp/one_resort"))
    work.mkdir(parents=True, exist_ok=True)
    folder = "region-inbox" if os.environ.get("ACTION") == "sweep" else "inbox"
    if folder == "inbox":
        discover_named_resorts(s3, bucket, work / "discover")
    keys = _inbox_keys(s3, bucket, folder)
    if not keys:
        print("batch: nothing queued", flush=True)
        return 0
    os.environ["SKIP_PMTILES"] = "1"
    os.environ["SKIP_INVALIDATE"] = "1"
    last_work = None
    stems: list[str] = []
    for key in keys:
        body = _read_json_key(s3, bucket, key) or {}
        os.environ["ACTION"] = body.get("action") or "update"
        os.environ["WINTER_SPORTS_ID"] = str(body.get("winter_sports_id") or "")
        os.environ["REGION"] = str(body.get("region") or "")
        os.environ["NAME"] = str(body.get("name") or "")
        os.environ["COUNTRY"] = str(body.get("country") or "")
        os.environ["STATE"] = str(body.get("state") or "")
        os.environ["LAT"] = str(body.get("lat") or "")
        os.environ["LON"] = str(body.get("lon") or "")
        os.environ["JOB_ID"] = str(body.get("jobId") or "")
        os.environ["WORK_DIR"] = str(Path("/tmp/one_resort") / os.environ["JOB_ID"])
        try:
            code = main()
        except Exception as exc:
            print(f"batch: {key} failed ({exc}); continuing", flush=True)
            code = 1
        if code in (0, 1, 3):
            _remove_key(s3, bucket, key)
            if code == 0:
                last_work = Path(os.environ["WORK_DIR"])
                yaml = _resort_yaml(os.environ["WINTER_SPORTS_ID"])
                if yaml:
                    stems.append(yaml.stem)
            elif code == 3:
                print("batch: skipped unmapped", key, flush=True)
            else:
                print("batch: failed, continuing", key, flush=True)
        else:
            print("batch: left queued", key, flush=True)
    os.environ.pop("SKIP_PMTILES", None)
    os.environ.pop("SKIP_INVALIDATE", None)
    if last_work and s3:
        _publish_pmtiles(s3, bucket, last_work)
        os.environ["JOB_ID"] = "batch"
        paths = ["/clay_scenes/catalog.json", "/scripts/map-config.js"]
        paths.extend(f"/clay_scenes/{stem}/*" for stem in stems)
        dist = os.environ.get("CLOUDFRONT_DISTRIBUTION_ID", "E3BDMTLYF8G4VB")
        if dist:
            __import__("boto3").client("cloudfront").create_invalidation(
                DistributionId=dist,
                InvalidationBatch={
                    "Paths": {"Quantity": len(paths), "Items": paths},
                    "CallerReference": f"resort-batch-{int(time.time())}",
                },
            )
            print("invalidated", paths, flush=True)
    return 0


def main() -> int:
    action = os.environ.get("ACTION", "update")
    if action == "sweep":
        os.environ["SKIP_DISCOVER"] = "1"
        bucket = os.environ.get("S3_BUCKET", "globalskiatlas-backend-k8s-output")
        s3 = None if _local_root() else _s3()
        work = Path(os.environ.get("WORK_DIR", "/tmp/one_resort"))
        work.mkdir(parents=True, exist_ok=True)
        sweep_region(s3, bucket, work / "sweep")
        return run_batch()
    if action == "batch":
        return run_batch()
    wid = os.environ.get("WINTER_SPORTS_ID", "")
    region = os.environ.get("REGION", "")
    job_id = os.environ.get("JOB_ID", "")
    bucket = os.environ.get("S3_BUCKET", "globalskiatlas-backend-k8s-output")
    boundaries = os.environ.get("BOUNDARIES", "/boundaries")
    work = Path(os.environ.get("WORK_DIR", "/tmp/one_resort"))
    work.mkdir(parents=True, exist_ok=True)
    s3 = None if _local_root() or os.environ.get("SKIP_S3") == "1" else _s3()
    try:
        _put_job(s3, bucket, job_id, "running", action)
        resort = None
        if action == "add":
            if not wid:
                _put_job(s3, bucket, job_id, "failed", "winter_sports_id is required")
                return 2
            resort = fetch_winter_sports(wid)
            if not resort:
                _put_job(s3, bucket, job_id, "failed", "not in OSM")
                return 2
            if not os.environ.get("NAME"):
                os.environ["NAME"] = str((resort.get("tags") or {}).get("name") or wid)
            if not region:
                from analyze_ski_areas import _lookup_country_state_from_boundaries

                lat, lon = _centroid(resort)
                country, state = _lookup_country_state_from_boundaries(lat, lon, Path(boundaries))
                os.environ["COUNTRY"] = str(country or "")
                os.environ["STATE"] = str(state or "")
                os.environ["LAT"] = str(lat)
                os.environ["LON"] = str(lon)
                try:
                    region = region_for_resort(
                        os.environ["COUNTRY"], os.environ["STATE"], lat, lon, Path(boundaries)
                    )
                except SystemExit as exc:
                    _put_job(s3, bucket, job_id, "failed", str(exc))
                    return 3
                print("region", region, flush=True)
        if not wid or not region:
            _put_job(s3, bucket, job_id, "failed", "winter_sports_id and region are required")
            return 2
        if action == "delete":
            for prefix in (region, "combined"):
                patch_prefix(s3, bucket, prefix, work, wid, publish=False)
            _append_skip(s3, bucket, wid, work)
            yaml = _resort_yaml(wid)
            if yaml and s3:
                _delete_prefix(s3, bucket, f"game_scenes/{yaml.stem}/")
                _delete_prefix(s3, bucket, f"clay_scenes/{yaml.stem}/")
            if s3:
                s3.delete_object(Bucket=bucket, Key=f"{region}/dems/{wid}.tif")
            _state_dots()
            _publish_wiki(s3, bucket, "delete", wid, work)
            _publish_pmtiles(s3, bucket, work)
            _invalidate_site(s3, wid)
            _put_job(s3, bucket, job_id, "succeeded", f"deleted {wid}")
            return 0
        if resort is None:
            resort = fetch_winter_sports(wid)
        if not resort:
            _put_job(s3, bucket, job_id, "failed", "not in OSM")
            return 2
        write_extracts(work, resort, fetch_nearby(resort, wid))
        build_local(work, wid, region, boundaries)
        for prefix in (region, "combined"):
            patch_prefix(s3, bucket, prefix, work, wid, publish=True)
        _drop_skip(s3, bucket, wid, work)
        yaml = _ensure_resort_yaml(wid, region, work)
        stage_scene_inputs(work, region, wid)
        _run([
            sys.executable, "-m", "game_export", "--resort", yaml.stem,
            "--data-root", str(work), "--fetch-skadi", "--force",
        ])
        scenes = work / "game_scenes"
        if (scenes / "catalog.json").is_file() and s3 and not _local_root():
            _run([sys.executable, "scripts/upload_game_scenes.py", "--scenes-dir", str(scenes), "--bucket", bucket])
            _merge_game_catalog(s3, bucket, scenes / "catalog.json")
        elif _local_root() and scenes.is_dir():
            shutil.copytree(scenes, _local_root() / "game_scenes", dirs_exist_ok=True)
        _run([
            sys.executable, "-m", "game_export", "--clay-scene", "--resort", yaml.stem,
            "--data-root", str(work), "--fetch-skadi", "--force",
        ])
        clay = work / "clay_scenes"
        if s3 and not _local_root():
            _run([
                sys.executable, "scripts/upload_clay_scenes.py",
                "--scenes-dir", str(clay),
                "--only", yaml.stem, "--bucket", bucket,
            ])
        elif _local_root() and clay.is_dir():
            shutil.copytree(clay, _local_root() / "clay_scenes", dirs_exist_ok=True)
        if action == "add" and not _local_root():
            wiki_path = work / "wiki.json"
            _run([
                sys.executable, "scripts/generate_resort_copy_bedrock.py",
                "-i", str(work / "ski_areas_analyzed.parquet"),
                "--name", os.environ.get("NAME") or wid,
                "--out-json", str(wiki_path),
            ])
            if s3 and job_id and wiki_path.is_file():
                s3.upload_file(str(wiki_path), bucket, f"resort-jobs/{job_id}/wiki.json")
            _state_dots()
        _publish_wiki(s3, bucket, action, wid, work)
        _publish_pmtiles(s3, bucket, work)
        _invalidate_site(s3, wid)
        _put_job(s3, bucket, job_id, "succeeded", f"{action} {wid}")
        return 0
    except Exception as exc:
        _put_job(s3, bucket, job_id, "failed", str(exc))
        return 1
    finally:
        if wid and (s3 or _local_root()):
            _remove_key(s3, bucket, f"resort-jobs/lock-{wid}.json")


if __name__ == "__main__":
    if os.environ.get("WRITE_STATUS") == "1":
        write_run_status(None, os.environ.get("S3_BUCKET", ""))
        raise SystemExit(0)
    if os.environ.get("RUN_CHECK") == "1":
        existing = pd.DataFrame({"winter_sports_id": ["1", "45096232"], "name": ["Other", "Old"]})
        incoming = pd.DataFrame({"winter_sports_id": ["45096232"], "name": ["Montage"]})
        out = replace_resort_rows(existing, incoming, "45096232")
        assert list(out["name"]) == ["Other", "Montage"], out
        gone = replace_resort_rows(existing, None, "45096232")
        assert list(gone["winter_sports_id"]) == ["1"], gone
        lifts = pd.DataFrame({"osm_uid": ["way/1", "way/2"], "name": ["Keep", "Old"]})
        again = replace_resort_rows(lifts, pd.DataFrame({"osm_uid": ["way/2"], "name": ["New"]}), "45096232")
        assert list(again["name"]) == ["Keep", "New"], again
        assert new_named_ids(["9", "1", "9", "2"], {"1"}, {"2"}, set(), 8) == ["9"]
        assert new_named_ids(["3", "4"], set(), set(), set(), 1) == ["3"]
        assert region_for_place("Japan", "Shizuoka") == "asia/japan/chubu"
        assert region_for_place("Japan", "Nagano") == "asia/japan/chubu"
        assert region_for_place("Japan", "Hyōgo") == "asia/japan/kansai"
        assert region_for_place("Japan", "Hokkaido") == "asia/japan/hokkaido"
        assert region_for_place("France", "Savoie") == "europe/france/rhone-alpes"
        box = _parse_poly("box\n1\n0 0\n4 0\n4 4\n0 4\nEND\n!hole\n1 1\n2 1\n2 2\n1 2\nEND\nEND\n")
        assert _rings_contain(3, 3, box)
        assert not _rings_contain(1.5, 1.5, box)
        assert not _rings_contain(9, 9, box)
        print("check ok")
        raise SystemExit(0)
    raise SystemExit(main())
