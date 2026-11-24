#!/usr/bin/env python3
"""One resort: live Overpass, existing pipeline scripts, patch region + combined parquet.

Env: ACTION (update|add|delete|batch), WINTER_SPORTS_ID, REGION, NAME, COUNTRY, STATE,
LAT, LON, JOB_ID, S3_BUCKET, BOUNDARIES, OVERPASS_URL, OVERPASS_TIMEOUT, MAPLARK_API_KEY,
MAPLARK_URL, MAPLARK_MAX_AROUND_M.

ACTION=batch first asks Overpass for named landuse=winter_sports ways and
relations. Any id missing from combined/ski_areas.parquet, the delete skip list,
and the job inbox is queued as an add. DISCOVER_MAX (default 8) caps how many
new resorts one night starts. A state that is not its own extract, such as
Shizuoka inside Chubu, is filed under the Geofabrik region that contains it.
A country with no extract is added when Geofabrik publishes one. One resort
that fails is recorded and removed from the queue so the rest of the batch runs.

When every public Overpass mirror fails (timeout, 502, etc.), the same query is
retried once against MapLark's Overpass-compatible interpreter if MAPLARK_API_KEY
is set. MapLark needs a spatial anchor for tag filters, so planet-wide discovery
still depends on Overpass.

Set DATA_ROOT to a local output directory (the Docker host volume) to read and
write that catalog locally. When a region finishes, the region parquet and DEM
files are uploaded to S3, and each resort is written to DynamoDB atlas-WikiPages.
Every resort gets a new AI essay, replacing the article already on the page.
SKIP_REGION_S3=1 skips the upload. SKIP_REGION_DDB=1 skips DynamoDB.
SKIP_ESSAY=1 skips the essay. Harvested facts and amenities stay on
atlas-ResortFacts and atlas-ResortAmenities.

The playable game reads ``game_play/<id>/`` (world.json, detail.u16, backdrop.u16),
not a ``game_scenes`` cake. This job still exports a scene as scratch, bakes the
Hemu Ski files from it, then deletes the cake. Baked files stay on GAME_PLAY_DIR
and are uploaded to ``game_play/`` when AWS credentials are available.
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
_LAST_OSM_SOURCE = ""


def _osm_host_label(endpoint: str) -> str:
    host = (urllib.parse.urlparse(endpoint).hostname or endpoint or "").lower()
    if "maplark.com" in host:
        return "MapLark"
    if host == "overpass-api.de":
        return "Overpass (de)"
    if "kumi.systems" in host:
        return "Overpass (kumi)"
    if "openstreetmap.fr" in host:
        return "Overpass (fr)"
    if host:
        return f"Overpass ({host})"
    return "Overpass"


def _note_osm_source(label: str) -> None:
    global _LAST_OSM_SOURCE
    _LAST_OSM_SOURCE = (label or "").strip()


def _osm_http_timeout() -> float:
    """Seconds to wait on one Overpass/MapLark attempt before trying the next host."""
    try:
        return max(5.0, float(os.environ.get("OVERPASS_TIMEOUT") or "30"))
    except ValueError:
        return 30.0


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


def _maplark_adapt_query(query: str) -> str:
    """Rewrite Overpass QL bits MapLark's interpreter subset rejects."""
    # Case-insensitive regex (,i) is unsupported; keep the pattern as-is.
    adapted = re.sub(r'(~"[^"]*"),i\b', r"\1", query)
    try:
        max_around = int(os.environ.get("MAPLARK_MAX_AROUND_M") or "22000")
    except ValueError:
        max_around = 22000
    max_around = max(1, max_around)

    def _clamp_around(match: re.Match[str]) -> str:
        radius = min(int(match.group(1)), max_around)
        return f"(around:{radius},{match.group(2)})"

    return re.sub(r"\(around:(\d+),([^)]+)\)", _clamp_around, adapted)


def _maplark_overpass(query: str) -> dict:
    key = (os.environ.get("MAPLARK_API_KEY") or os.environ.get("MAPLARK_KEY") or "").strip()
    if not key:
        raise RuntimeError("MAPLARK_API_KEY is not set")
    endpoint = (os.environ.get("MAPLARK_URL") or "https://api.maplark.com/api/interpreter").strip()
    adapted = _maplark_adapt_query(query)
    if adapted != query:
        print(f"maplark adapted query for supported Overpass subset", flush=True)
    data = urllib.parse.urlencode({"data": adapted}).encode()
    req = urllib.request.Request(
        endpoint,
        data=data,
        headers={
            "User-Agent": "globalskiatlas-one-resort/1.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {key}",
        },
    )
    with urllib.request.urlopen(req, timeout=_osm_http_timeout()) as resp:
        payload = json.loads(resp.read().decode())
    _note_osm_source(_osm_host_label(endpoint))
    return payload


def overpass(query: str, url: str | None = None) -> dict:
    """Try each Overpass host once, then MapLark. No retries, no sleep between hosts."""
    endpoints = [url] if url else []
    endpoints.append(os.environ.get("OVERPASS_URL") or "https://overpass-api.de/api/interpreter")
    endpoints.append("https://overpass.kumi.systems/api/interpreter")
    endpoints.append("https://overpass.openstreetmap.fr/api/interpreter")
    timeout = _osm_http_timeout()
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
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode())
            _note_osm_source(_osm_host_label(endpoint))
            return payload
        except Exception as exc:
            last = exc
            print(f"overpass {endpoint}: {exc}", flush=True)
    key = (os.environ.get("MAPLARK_API_KEY") or os.environ.get("MAPLARK_KEY") or "").strip()
    if key:
        print("overpass exhausted; trying MapLark interpreter", flush=True)
        try:
            return _maplark_overpass(query)
        except Exception as exc:
            last = exc
            print(f"maplark interpreter failed: {exc}", flush=True)
    elif last is not None:
        print("overpass exhausted; set MAPLARK_API_KEY to enable MapLark fallback", flush=True)
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
    _write_analyzed_csv(work)


def _write_analyzed_csv(work: Path) -> None:
    pd.read_parquet(work / "ski_areas_analyzed.parquet").to_csv(
        work / "ski_areas_analyzed.csv", index=False
    )


def _downhill_trail_count(work: Path) -> int:
    """Downhill piste count from the OSM analysis. Elevation waits on this."""
    path = work / "ski_areas_analyzed.parquet"
    if not path.is_file():
        raise SystemExit("ski area analysis missing")
    frame = pd.read_parquet(path)
    if frame.empty or "downhill_trails" not in frame.columns:
        raise SystemExit("ski area analysis has no downhill_trails")
    value = frame.iloc[0]["downhill_trails"]
    try:
        if pd.isna(value):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _write_osm_hull_dem(work: Path, wid: str, region: str) -> Path:
    """One Mapterhorn mosaic covering the ski area plus its pistes and lifts.

    Game and clay exports read this file instead of sampling those tiles again.
    """
    import geopandas as gpd
    from shapely.ops import unary_union

    from game_export.inputs import _mosaic_from_skadi_cache, _write_temp_dem, osm_feature_hull

    def _load(name: str):
        path = work / name
        if not path.is_file():
            return None
        frame = gpd.read_parquet(path)
        if "winter_sports_id" in frame.columns:
            frame = frame[frame["winter_sports_id"].astype(str) == str(wid)]
        return frame

    pistes = _load("pistes.parquet")
    lifts = _load("lifts.parquet")
    ski = _load("ski_areas.parquet")
    coverage = osm_feature_hull(pistes, lifts)
    polygon = None
    if ski is not None and not ski.empty:
        polygon = ski.geometry.iloc[0]
    if coverage is not None and polygon is not None and not polygon.is_empty:
        coverage = unary_union([coverage, polygon]).convex_hull
    elif coverage is None:
        coverage = polygon
    if coverage is None or coverage.is_empty:
        raise FileNotFoundError(f"no OSM coverage geometry for {wid}")
    crop, west, south, east, north = _mosaic_from_skadi_cache(coverage, work / "cache", fetch=True)
    dest = work / "dems" / "scene" / f"{wid}.tif"
    _write_temp_dem(crop, west, south, east, north, dest)
    # Same file game_export.resolve_dem looks for before mosaicking again.
    hull_cache = work / "cache" / "game_export_dems" / Path(*[p for p in region.split("/") if p]) / f"{wid}-osmhull.tif"
    hull_cache.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(dest, hull_cache)
    print("scene dem: wrote one Mapterhorn mosaic", dest, "and", hull_cache, flush=True)
    return dest


def _fetch_elevation(work: Path, wid: str, region: str) -> None:
    _run([
        sys.executable, "scripts/ski_area_elevation_contours.py",
        "-i", str(work / "ski_areas.parquet"), "-o", str(work),
        "--cache-dir", str(work / "cache"), "--save-dem", "--ids-file", _ids_file(work, wid),
    ])
    _write_analyzed_csv(work)
    try:
        _write_osm_hull_dem(work, wid, region)
    except Exception as exc:
        print("scene dem: mosaic failed, scene export may fetch skadi", exc, flush=True)


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
    if not dem.is_file() and (work / "dems").is_dir():
        dem = next(
            (path for path in (work / "dems").rglob(f"{wid}.tif") if "scene" not in path.parts),
            None,
        )
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
    osm_sources = {}
    for path in jobs.glob("*.json"):
        if path.name in ("status.json", "region-cursor.json") or path.name.startswith("lock-"):
            continue
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        status = str(body.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        src = str(body.get("osm_source") or "").strip() or "No source recorded"
        src_counts = osm_sources.setdefault(
            src, {"succeeded": 0, "failed": 0, "running": 0, "queued": 0}
        )
        src_counts[status] = src_counts.get(status, 0) + 1
        recent.append({
            "jobId": body.get("jobId") or path.stem,
            "status": status,
            "message": body.get("message") or "",
            "winter_sports_id": body.get("winter_sports_id") or "",
            "region": body.get("region") or "",
            "osm_source": body.get("osm_source") or "",
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
    running = [row for row in recent if row["status"] == "running"][:5]
    processing = next((row.get("region") for row in running if row.get("region")), "")
    summary = {
        "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "lastRegion": processing or cursor.get("region") or "",
        "queuedThrough": cursor.get("region") or "",
        "nextRegion": paths[index % len(paths)] if paths else "",
        "regionIndex": index,
        "regionCount": len(paths),
        "counts": counts,
        "osm_sources": osm_sources,
        "running": running,
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
    if os.environ.get("REGION") and "region" not in body:
        body["region"] = os.environ.get("REGION")
    if _LAST_OSM_SOURCE and "osm_source" not in body:
        body["osm_source"] = _LAST_OSM_SOURCE
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
    scene_dem = work / "dems" / "scene" / f"{wid}.tif"
    dem_root = work / "dems"
    if scene_dem.is_file():
        dem = scene_dem
    elif dem_root.is_dir():
        dem = next((path for path in dem_root.rglob(f"{wid}.tif") if "scene" not in path.parts), None)
    else:
        dem = None
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


_WIKI_STRINGS = (
    ("winterSportsId", "winter_sports_id"),
    ("winterSportsType", "winter_sports_type"),
    ("englishName", "english_name"),
    ("country", "country"),
    ("state", "state"),
    ("region", "region"),
    ("totalLifts", "total_lifts"),
    ("downhillTrails", "downhill_trails"),
    ("trailsNovice", "trails_novice"),
    ("trailsEasy", "trails_easy"),
    ("trailsIntermediate", "trails_intermediate"),
    ("trailsAdvanced", "trails_advanced"),
    ("trailsExpert", "trails_expert"),
    ("trailsFreeride", "trails_freeride"),
    ("trailsExtreme", "trails_extreme"),
    ("gladedTerrain", "gladed_terrain"),
    ("snowPark", "snow_park"),
    ("sleddingTubing", "sledding_tubing"),
    ("nightSkiing", "night_skiing"),
    ("snowmaking", "snowmaking"),
    ("liftTypes", "lift_types"),
    ("resortType", "resort_type"),
    ("website", "website"),
    ("operator", "operator"),
    ("openingHours", "opening_hours"),
    ("phone", "phone"),
    ("elevationSource", "elevation_source"),
)
_WIKI_NUMBERS = (
    ("centroidLat", "centroid_lat"),
    ("centroidLon", "centroid_lon"),
    ("totalAreaHa", "total_area_ha"),
    ("totalAreaAcres", "total_area_acres"),
    ("skiableTerrainHa", "skiable_terrain_ha"),
    ("skiableTerrainAcres", "skiable_terrain_acres"),
    ("longestLiftMi", "longest_lift_mi"),
    ("longestTrailMi", "longest_trail_mi"),
    ("avgTrailMi", "avg_trail_mi"),
    ("totalTrailMi", "total_trail_mi"),
    ("litPistes", "lit_pistes"),
    ("litLifts", "lit_lifts"),
    ("skiNorthAngle", "ski_north_angle"),
    ("verticalDropFt", "vertical_drop_ft"),
    ("highElevationM", "elevation_high_m"),
    ("lowElevationM", "elevation_low_m"),
)


def _wiki_plain(value):
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            return None
    if isinstance(value, float) and value != value:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return value


def _wiki_slug(value) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^a-z0-9-]", "", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


def wiki_page_id_from_analyzed(row: dict) -> str:
    """pageId used by wiki-ingest-parquet.js. The local name wins over english_name."""
    name = None
    for key in ("name", "Name", "resort_name", "title", "english_name"):
        name = _wiki_plain(row.get(key))
        if name is not None:
            break
    state = None
    for key in ("state", "State", "addr:state", "province", "addr:province", "state_province", "region"):
        state = _wiki_plain(row.get(key))
        if state is not None:
            break
    country = None
    for key in ("country", "Country", "addr:country", "country_name"):
        country = _wiki_plain(row.get(key))
        if country is not None:
            break
    name_slug = _wiki_slug(name)
    if state is not None:
        return f"{name_slug}-{_wiki_slug(state)}"
    if country is not None:
        return f"{name_slug}-{_wiki_slug(country)}"
    return name_slug


def _wiki_size(row: dict) -> str:
    kind = str(row.get("resort_type") or "").strip().lower()
    if kind == "not a downhill ski resort":
        return "unknown"
    trails = _wiki_plain(row.get("downhill_trails"))
    lifts = _wiki_plain(row.get("total_lifts"))
    acres = _wiki_plain(row.get("skiable_terrain_acres"))
    if acres is None:
        ha = _wiki_plain(row.get("skiable_terrain_ha"))
        if ha is not None:
            acres = float(ha) * 2.471
    try:
        trails_n = float(trails) if trails is not None else None
        lifts_n = float(lifts) if lifts is not None else None
        acres_n = float(acres) if acres is not None else None
    except (TypeError, ValueError):
        return "unknown"
    if trails_n is None or lifts_n is None or trails_n < 0 or lifts_n < 0:
        return "unknown"
    area = acres_n if acres_n is not None and acres_n >= 0 else 0.0
    if trails_n >= 200 or area >= 10000:
        return "mega_resort"
    if trails_n >= 100 or area >= 5000:
        return "multiple_mountains"
    if trails_n >= 50 or area >= 1000:
        return "ski_mountain"
    return "small_hill"


def _wiki_string_attr(value) -> dict:
    if isinstance(value, float) and value.is_integer():
        return {"S": str(int(value))}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"S": str(value)}
    return {"S": str(value).strip()}


def _wiki_number_attr(value) -> dict:
    from decimal import Decimal

    number = Decimal(str(value))
    if number == number.to_integral():
        return {"N": str(number.to_integral())}
    return {"N": format(number, "f")}


def wiki_stat_attributes(row: dict) -> dict[str, dict]:
    """OSM and DEM fields for atlas-WikiPages. Article content is not included."""
    attrs: dict[str, dict] = {}
    for dest, src in _WIKI_STRINGS:
        value = _wiki_plain(row.get(src))
        if value is not None:
            attrs[dest] = _wiki_string_attr(value)
    for dest, src in _WIKI_NUMBERS:
        value = _wiki_plain(row.get(src))
        if dest == "highElevationM" and value is None:
            value = _wiki_plain(row.get("high_elevation_m"))
        if dest == "lowElevationM" and value is None:
            value = _wiki_plain(row.get("low_elevation_m"))
        if value is None:
            continue
        try:
            attrs[dest] = _wiki_number_attr(value)
        except Exception:
            continue
    high = _wiki_plain(row.get("elevation_high_m")) or _wiki_plain(row.get("high_elevation_m"))
    low = _wiki_plain(row.get("elevation_low_m")) or _wiki_plain(row.get("low_elevation_m"))
    drop = _wiki_plain(row.get("vertical_drop_m"))
    if drop is None and high is not None and low is not None:
        try:
            if float(high) >= float(low):
                drop = float(high) - float(low)
        except (TypeError, ValueError):
            drop = None
    if drop is not None:
        try:
            if float(drop) >= 0:
                attrs["verticalDropM"] = _wiki_number_attr(drop)
        except (TypeError, ValueError):
            pass
    attrs["resortSizeCategory"] = {"S": _wiki_size(row)}
    return attrs


def wiki_stat_update(page_id: str, row: dict, now: str, content: str | None = None) -> dict:
    """Update one wiki page. Pass content to store a new essay."""
    title = str(_wiki_plain(row.get("english_name")) or _wiki_plain(row.get("name")) or page_id)
    names = {"#status": "status"}
    values = {
        ":now": {"S": now},
        ":title": {"S": title},
        ":resort": {"S": "resort"},
        ":published": {"S": "published"},
    }
    if content:
        values[":essay"] = {"S": content}
        content_set = "content = :essay"
    else:
        values[":stub"] = {"S": f"# {title}\n\n*Add a description for this resort.*"}
        content_set = "content = if_not_exists(content, :stub)"
    sets = [
        "updatedAt = :now",
        "title = if_not_exists(title, :title)",
        content_set,
        "createdAt = if_not_exists(createdAt, :now)",
        "pageType = if_not_exists(pageType, :resort)",
        "#status = if_not_exists(#status, :published)",
    ]
    for index, (key, attr) in enumerate(wiki_stat_attributes(row).items()):
        names[f"#a{index}"] = key
        values[f":a{index}"] = attr
        sets.append(f"#a{index} = :a{index}")
    return {
        "Key": {"pageId": {"S": page_id}},
        "UpdateExpression": "SET " + ", ".join(sets),
        "ExpressionAttributeNames": names,
        "ExpressionAttributeValues": values,
    }


def _write_wiki_stats(row: dict, content: str | None = None) -> None:
    """Write one analyzed resort onto atlas-WikiPages."""
    if os.environ.get("SKIP_REGION_DDB") == "1":
        return
    page_id = wiki_page_id_from_analyzed(row)
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).replace(microsecond=0).isoformat()
    table = os.environ.get("WIKI_TABLE", "atlas-WikiPages")
    request = wiki_stat_update(page_id, row, now, content=content)
    client = __import__("boto3").client("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    client.update_item(TableName=table, **request)
    print("wiki stats", table, page_id, "essay" if content else "stats", flush=True)


def _mirror_s3(bucket: str, key: str, src: Path) -> None:
    """Upload one finished homeserver file to the output bucket."""
    if os.environ.get("SKIP_REGION_S3") == "1" or not src.is_file():
        return
    try:
        _s3().upload_file(str(src), bucket, key)
        print("s3", key, flush=True)
    except Exception as exc:
        print(f"s3 upload failed {key}: {exc}", flush=True)


def _essay_markdown(payload: dict, row: dict) -> str:
    final = str(payload.get("final") or "").strip()
    if not final:
        return ""
    if final.startswith("# "):
        return final
    name = str(_wiki_plain(row.get("english_name")) or _wiki_plain(row.get("name")) or "Resort")
    return f"# {name}\n\n{final}"


def _write_resort_essay(analyzed: Path, row: dict, wid: str, dest: Path) -> str:
    """One Bedrock essay for this resort. The search name is the id so punctuation cannot miss."""
    frame = pd.read_parquet(analyzed)
    hit = frame
    if "winter_sports_id" in frame.columns:
        hit = frame[frame["winter_sports_id"].astype(str) == str(wid)]
    if hit.empty:
        hit = frame.head(1)
    one = hit.head(1).copy()
    label = str(_wiki_plain(row.get("english_name")) or _wiki_plain(row.get("name")) or "")
    if "english_name" not in one.columns:
        one["english_name"] = label
    elif not str(one["english_name"].iloc[0] or "").strip():
        one.loc[one.index[:1], "english_name"] = label
    token = f"wid{wid}"
    if "name" not in one.columns:
        one["name"] = token
    else:
        one.loc[one.index[:1], "name"] = token
    row_path = dest.with_suffix(".row.parquet")
    row_path.parent.mkdir(parents=True, exist_ok=True)
    one.to_parquet(row_path, index=False)
    _run([
        sys.executable, "scripts/generate_resort_copy_bedrock.py",
        "-i", str(row_path),
        "--name", token,
        "--tier", "auto",
        "--out-json", str(dest),
    ])
    payload = json.loads(dest.read_text(encoding="utf-8"))
    return _essay_markdown(payload, row)


def _publish_held_wiki(path: Path, wid: str, region: str, bucket: str) -> None:
    analyzed = path / "ski_areas_analyzed.parquet"
    if not analyzed.is_file():
        print("publish: no analyzed parquet for wiki", wid, flush=True)
        return
    frame = pd.read_parquet(analyzed)
    row = _row_for(frame, wid)
    if row is None and len(frame.index) == 1:
        row = _row_for(frame.assign(winter_sports_id=str(wid)), wid)
    if row is None:
        print("publish: no analyzed row for wiki", wid, flush=True)
        return
    content = None
    if os.environ.get("SKIP_ESSAY") != "1":
        essay_path = path / "wiki.json"
        try:
            content = _write_resort_essay(analyzed, row, wid, essay_path) or None
        except Exception as exc:
            print(f"publish: essay failed {wid}: {exc}", flush=True)
            content = None
        if content and essay_path.is_file():
            _mirror_s3(bucket, f"{region.strip('/')}/wiki/{wid}.json", essay_path)
    try:
        _write_wiki_stats(row, content)
    except Exception as exc:
        print(f"publish: wiki stats failed {wid}: {exc}", flush=True)


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
    if action == "update":
        try:
            _write_wiki_stats(row)
        except Exception as exc:
            print(f"wiki stats failed {wid}: {exc}", flush=True)
        return
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
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


def _latest_scene_dir(scenes: Path, resort_id: str) -> Path | None:
    root = scenes / resort_id
    if not root.is_dir():
        return None
    versions = sorted(
        (p for p in root.iterdir() if p.is_dir() and (p / "scene-manifest.json").is_file()),
        key=lambda p: p.stat().st_mtime,
    )
    return versions[-1] if versions else None


def _merge_worlds_file(path: Path, entry: dict) -> None:
    worlds = {"resorts": []}
    if path.is_file():
        worlds = json.loads(path.read_text(encoding="utf-8"))
    resorts = [r for r in worlds.get("resorts") or [] if r.get("id") != entry["id"]]
    resorts.append(entry)
    resorts.sort(key=lambda w: str(w.get("name") or "").lower())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"resorts": resorts}, ensure_ascii=False), encoding="utf-8")


def _upload_game_play(s3, bucket: str, resort_id: str, src: Path, entry: dict) -> None:
    types = {".json": "application/json; charset=utf-8", ".u16": "application/octet-stream"}
    for f in sorted(p for p in src.rglob("*") if p.is_file()):
        key = f"game_play/{resort_id}/{f.relative_to(src).as_posix()}"
        s3.upload_file(
            str(f), bucket, key,
            ExtraArgs={
                "ContentType": types.get(f.suffix, "application/octet-stream"),
                "CacheControl": "public, max-age=3600",
            },
        )
    key = "game_play/worlds.json"
    worlds: dict = {"resorts": []}
    try:
        worlds = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
    except Exception:
        worlds = {"resorts": []}
    resorts = [r for r in worlds.get("resorts") or [] if r.get("id") != resort_id]
    resorts.append(entry)
    resorts.sort(key=lambda w: str(w.get("name") or "").lower())
    s3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps({"resorts": resorts}, ensure_ascii=False).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )
    print("uploaded game_play", resort_id, flush=True)


def _publish_game_play(s3, bucket: str, work: Path, resort_id: str) -> None:
    """Bake one scene cake into Hemu Ski files and drop the cake."""
    from game_export.game_play import convert_scene

    scene_dir = _latest_scene_dir(work / "game_scenes", resort_id)
    if scene_dir is None:
        raise SystemExit(f"game play: no scene cake for {resort_id}")
    catalog_name = None
    catalog_path = work / "game_scenes" / "catalog.json"
    if catalog_path.is_file():
        for row in json.loads(catalog_path.read_text(encoding="utf-8")).get("resorts") or []:
            if row.get("id") == resort_id:
                catalog_name = row.get("name")
                break
    out = work / "game_play_out" / resort_id
    if out.exists():
        shutil.rmtree(out)
    entry = convert_scene(
        scene_dir, out, resort_id,
        catalog_name=catalog_name,
        tile_cache=work / "cache" / "game_play_tiles",
    )
    play_root = Path(os.environ.get("GAME_PLAY_DIR", "/tmp/one_resort/game_play"))
    dest = play_root / resort_id
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(out, dest)
    _merge_worlds_file(play_root / "worlds.json", entry)
    cake = work / "game_scenes" / resort_id
    if cake.is_dir():
        shutil.rmtree(cake)
    print("game play", resort_id, entry.get("runs"), "runs", flush=True)
    client = s3
    if client is None and os.environ.get("SKIP_GAME_PLAY_S3") != "1":
        try:
            client = _s3()
        except Exception as exc:
            print("game play: staying local, no s3 client", exc, flush=True)
            client = None
    if client is not None and os.environ.get("SKIP_GAME_PLAY_S3") != "1":
        _upload_game_play(client, bucket, resort_id, dest, entry)


def _drop_game_play(s3, bucket: str, resort_id: str) -> None:
    play_root = Path(os.environ.get("GAME_PLAY_DIR", "/tmp/one_resort/game_play"))
    dest = play_root / resort_id
    if dest.is_dir():
        shutil.rmtree(dest)
    worlds_path = play_root / "worlds.json"
    if worlds_path.is_file():
        worlds = json.loads(worlds_path.read_text(encoding="utf-8"))
        worlds["resorts"] = [r for r in worlds.get("resorts") or [] if r.get("id") != resort_id]
        worlds_path.write_text(json.dumps(worlds, ensure_ascii=False), encoding="utf-8")
    if s3 is None:
        return
    _delete_prefix(s3, bucket, f"game_play/{resort_id}/")
    try:
        worlds = json.loads(s3.get_object(Bucket=bucket, Key="game_play/worlds.json")["Body"].read().decode())
    except Exception:
        return
    worlds["resorts"] = [r for r in worlds.get("resorts") or [] if r.get("id") != resort_id]
    s3.put_object(
        Bucket=bucket, Key="game_play/worlds.json",
        Body=json.dumps(worlds, ensure_ascii=False).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )


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


def _disk_full(exc) -> bool:
    if isinstance(exc, OSError) and exc.errno == 28:
        return True
    return "No space left on device" in str(exc)


def _clear_resort_work() -> None:
    """Drop scratch files before another region starts.

    Published parquet already lives on the data volume. Baked ``game_play`` files
    stay on the scratch disk because that volume is the one with room. A new
    region is queued only after this returns.
    """
    keep = {"game_play"}
    scratch = Path("/tmp/one_resort")
    if scratch.is_dir():
        for child in scratch.iterdir():
            if child.name in keep:
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
    else:
        scratch.mkdir(parents=True, exist_ok=True)
    for path in (Path("/app/cache/game_export_dems"), Path("/app/cache/skadi")):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
    print("sweep: deleted region work files", flush=True)


_RUNNER_LOCK_FD = None


def _hold_runner_lock() -> None:
    """Block until this process owns the shared pipeline lock.

    The nightly add and the region sweep both call this. The lock is released
    when the process exits, including when it is killed.
    """
    global _RUNNER_LOCK_FD
    root = _local_root()
    if root is None or _RUNNER_LOCK_FD is not None:
        return
    import fcntl

    path = root / "resort-jobs" / "runner.flock"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = open(path, "a+", encoding="utf-8")
    print("sweep: waiting for runner lock", flush=True)
    fcntl.flock(fd, fcntl.LOCK_EX)
    fd.seek(0)
    fd.truncate()
    fd.write(str(os.getpid()))
    fd.flush()
    _RUNNER_LOCK_FD = fd
    print("sweep: runner lock acquired", flush=True)


def _publish_hold_dir() -> Path | None:
    root = _local_root()
    if root is None:
        return None
    return root / "resort-jobs" / "region-publish"


def _hold_resort_publish(work: Path, wid: str, region: str) -> None:
    """Keep this resort's tables until the region is written once."""
    root = _publish_hold_dir()
    if root is None:
        return
    dest = root / str(wid)
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    for name in TABLES:
        src = work / name
        if src.is_file():
            shutil.copy2(src, dest / name)
    csv_path = work / "ski_areas_analyzed.csv"
    if csv_path.is_file():
        shutil.copy2(csv_path, dest / csv_path.name)
    dem_root = work / "dems"
    if dem_root.is_dir():
        for dem in dem_root.rglob(f"{wid}.tif"):
            if "scene" in dem.parts:
                continue
            rel = dem.relative_to(work)
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dem, target)
    (dest / "meta.json").write_text(
        json.dumps({"wid": str(wid), "region": region}),
        encoding="utf-8",
    )
    print("publish: held", wid, region, flush=True)


def _arrow_without_wid(table, wid: str):
    """Drop rows for this resort without turning geometries into Shapely objects."""
    import pyarrow as pa
    import pyarrow.compute as pc

    try:
        wid_int = int(str(wid))
    except ValueError:
        wid_int = None
    mask = None
    for col in ID_COLUMNS:
        if col not in table.column_names:
            continue
        column = table[col]
        if pa.types.is_integer(column.type) and wid_int is not None:
            part = pc.equal(column, wid_int)
        else:
            part = pc.equal(pc.cast(column, pa.string()), str(wid))
        part = pc.fill_null(part, False)
        mask = part if mask is None else pc.or_(mask, part)
    if mask is None:
        return table
    return table.filter(pc.invert(mask))


def _cast_to_field(column, field):
    """Cast one resort column onto the national type.

    OSM uses yes/no in fields that the national file stored as numbers.
    A strict cast of 'yes' to double aborts the whole region publish.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    if column.type == field.type:
        return column
    if pa.types.is_null(column.type) or pa.types.is_null(field.type):
        return pa.nulls(len(column), type=field.type)
    try:
        return pc.cast(column, field.type, safe=False)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        if not (pa.types.is_floating(field.type) or pa.types.is_integer(field.type)):
            raise
    text = pc.utf8_lower(pc.cast(column, pa.string(), safe=False))
    text = pc.if_else(pc.is_in(text, value_set=pa.array(["yes", "true", "y"])), "1", text)
    text = pc.if_else(pc.is_in(text, value_set=pa.array(["no", "false", "n"])), "0", text)
    text = pc.if_else(pc.equal(text, ""), None, text)
    numeric = pc.match_substring_regex(text, r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")
    text = pc.if_else(pc.fill_null(numeric, False), text, None)
    return pc.cast(text, field.type, safe=False)


def _align_arrow_table(table, schema):
    import pyarrow as pa

    columns = []
    for field in schema:
        if field.name in table.column_names:
            columns.append(_cast_to_field(table[field.name], field))
        else:
            columns.append(pa.nulls(table.num_rows, type=field.type))
    return pa.Table.from_arrays(columns, schema=schema)


def _schema_for_merge(source_schema, incoming):
    """Widen all-null national columns so a later resort can fill them.

    Parquet stores a column that has never held a value as the null type.
    Casting a real string onto that type aborts the publish.
    """
    import pyarrow as pa

    fields = []
    for field in source_schema:
        if pa.types.is_null(field.type) and field.name in incoming.schema.names:
            incoming_field = incoming.schema.field(field.name)
            if not pa.types.is_null(incoming_field.type):
                fields.append(
                    pa.field(field.name, incoming_field.type, nullable=True, metadata=field.metadata)
                )
                continue
        fields.append(field)
    return pa.schema(fields, metadata=source_schema.metadata)


def _parquet_readable(path: Path) -> bool:
    """False when a parquet file was cut off before its footer was written."""
    import pyarrow.parquet as pq

    try:
        pq.ParquetFile(path)
    except Exception as exc:
        print(f"publish: unreadable parquet {path}: {exc}", flush=True)
        return False
    return True


def _merge_parquet_light(existing: Path, incoming: Path, wid: str, dest: Path) -> None:
    """Replace one resort's rows without loading the whole GeoParquet.

    The national nearby-OSM table is about 5 million rows. Reading it in one
    shot expands past the memory limit and the process is killed. One row
    group at a time stays near 2 GB.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    source = pq.ParquetFile(existing)
    incoming_table = pq.read_table(incoming)
    schema = _schema_for_merge(source.schema_arrow, incoming_table)
    added = _align_arrow_table(incoming_table, schema)
    uids = None
    if "osm_uid" in schema.names and "osm_uid" in added.column_names:
        uids = added["osm_uid"].drop_null()
        if len(uids) == 0:
            uids = None
    dest.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(dest, schema)
    try:
        for index in range(source.num_row_groups):
            group = _align_arrow_table(_arrow_without_wid(source.read_row_group(index), wid), schema)
            if uids is not None and "osm_uid" in group.column_names and group.num_rows:
                hit = pc.fill_null(pc.is_in(group["osm_uid"], value_set=uids), False)
                group = group.filter(pc.invert(hit))
            if group.num_rows:
                writer.write_table(group)
        if added.num_rows:
            writer.write_table(added)
    finally:
        writer.close()


def _patch_prefix_light(s3, bucket: str, prefix: str, work: Path, wid: str, mirror_s3: bool = False) -> None:
    """Merge one resort into a published prefix using Arrow, not GeoPandas."""
    scratch = work / "_publish"
    for name in TABLES:
        local = work / name
        if not local.is_file():
            continue
        key = f"{prefix.strip('/')}/{name}"
        remote = scratch / "in" / name
        had = _download(s3, bucket, key, remote)
        out = scratch / "out" / name
        if had and not _parquet_readable(remote):
            print(f"publish: leaving broken {key}, continuing", flush=True)
            continue
        if local.is_file() and not _parquet_readable(local):
            print(f"publish: skip bad incoming {key}, continuing", flush=True)
            continue
        if not had:
            if prefix.strip("/") == "combined":
                raise SystemExit(f"refusing to replace {key} with a single resort")
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local, out)
        else:
            _merge_parquet_light(remote, local, wid, out)
        _store_file(s3, bucket, key, out)
        if mirror_s3:
            _mirror_s3(bucket, key, out)
    csv_name = "ski_areas_analyzed.csv"
    local_csv = work / csv_name
    if local_csv.is_file():
        key = f"{prefix.strip('/')}/{csv_name}"
        remote = scratch / "in" / csv_name
        had = _download(s3, bucket, key, remote)
        merged = replace_resort_rows(_read_table(remote) if had else None, pd.read_csv(local_csv), wid)
        out = scratch / "out" / csv_name
        _write_table(merged, out)
        _store_file(s3, bucket, key, out)
        if mirror_s3:
            _mirror_s3(bucket, key, out)
    dem_root = work / "dems"
    if dem_root.is_dir():
        for dem in dem_root.rglob(f"{wid}.tif"):
            if "scene" in dem.parts:
                continue
            rel = dem.relative_to(work).as_posix()
            dem_key = f"{prefix.strip('/')}/{rel}"
            _store_file(s3, bucket, dem_key, dem)
            if mirror_s3:
                _mirror_s3(bucket, dem_key, dem)
    shutil.rmtree(scratch, ignore_errors=True)


def _flush_region_publish(s3, bucket: str) -> None:
    """Write held resorts one at a time.

    Each resort is removed from the hold after it lands, so a killed pass
    continues with whoever is left. The national files stay in Arrow so the
    geometry column is not expanded into Shapely objects.
    """
    import gc

    root = _publish_hold_dir()
    if root is None or not root.is_dir():
        return
    items = []
    for path in sorted(p for p in root.iterdir() if p.is_dir()):
        meta_path = path / "meta.json"
        if not meta_path.is_file():
            continue
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        region = str(meta.get("region") or "")
        wid = str(meta.get("wid") or path.name)
        if region and wid:
            items.append((wid, region, path))
    if not items:
        return
    publish_combined = os.environ.get("PUBLISH_COMBINED") == "1" or not _local_root()
    print(f"publish: writing {len(items)} resorts one at a time", flush=True)
    if not publish_combined:
        print("publish: leaving combined for a later merge", flush=True)
    for index, (wid, region, path) in enumerate(items, start=1):
        print(f"publish: {index}/{len(items)} {wid} {region}", flush=True)
        prefixes = [region]
        if publish_combined:
            prefixes.append("combined")
        for prefix in prefixes:
            _patch_prefix_light(s3, bucket, prefix, path, wid, mirror_s3=True)
        _publish_held_wiki(path, wid, region, bucket)
        shutil.rmtree(path, ignore_errors=True)
        gc.collect()
    shutil.rmtree(root, ignore_errors=True)
    print("publish: region tables written", flush=True)


def _publish_resort_tables(s3, bucket: str, work: Path, wid: str, region: str) -> None:
    if os.environ.get("HOLD_PUBLISH") == "1" and _local_root() and os.environ.get("ACTION") != "delete":
        _hold_resort_publish(work, wid, region)
        return
    for prefix in (region, "combined"):
        patch_prefix(s3, bucket, prefix, work, wid, publish=True)


def run_batch() -> int:
    """Find new named resorts, apply every queued resort, then rebuild the shared tiles once."""
    bucket = os.environ.get("S3_BUCKET", "globalskiatlas-backend-k8s-output")
    s3 = None if _local_root() else _s3()
    work = Path(os.environ.get("WORK_DIR", "/tmp/one_resort"))
    work.mkdir(parents=True, exist_ok=True)
    folder = "region-inbox" if os.environ.get("ACTION") == "sweep" else "inbox"
    if folder == "inbox":
        discover_named_resorts(s3, bucket, work / "discover")
    _clear_resort_work()
    if _local_root():
        os.environ["HOLD_PUBLISH"] = "1"
    keys = _inbox_keys(s3, bucket, folder)
    if not keys:
        _flush_region_publish(s3, bucket)
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
        work_dir = Path(os.environ.get("WORK_DIR") or "")
        stop_for_disk = False
        try:
            code = main()
        except (Exception, SystemExit) as exc:
            detail = exc.code if isinstance(exc, SystemExit) else exc
            if _disk_full(detail):
                _put_job(s3, bucket, os.environ.get("JOB_ID", ""), "queued", "disk full, will retry")
                stop_for_disk = True
                code = 28
            else:
                print(f"batch: {key} failed ({detail}); continuing", flush=True)
                code = 1
        if code == 28:
            stop_for_disk = True
            print(f"batch: disk full, left queued {key}", flush=True)
        elif code in (0, 1, 2, 3):
            _remove_key(s3, bucket, key)
            if code == 0:
                if last_work and last_work != work_dir:
                    shutil.rmtree(last_work, ignore_errors=True)
                last_work = work_dir
                work_dir = Path()
                yaml = _resort_yaml(os.environ["WINTER_SPORTS_ID"])
                if yaml:
                    stems.append(yaml.stem)
            elif code == 3:
                print("batch: skipped unmapped", key, flush=True)
            else:
                print("batch: failed, continuing", key, flush=True)
        else:
            print("batch: left queued", key, flush=True)
        if work_dir.is_dir() and work_dir.parent == Path("/tmp/one_resort"):
            shutil.rmtree(work_dir, ignore_errors=True)
        if stop_for_disk:
            break
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
    _flush_region_publish(s3, bucket)
    _clear_resort_work()
    return 0


def main() -> int:
    action = os.environ.get("ACTION", "update")
    if action == "sweep":
        os.environ["SKIP_DISCOVER"] = "1"
        _hold_runner_lock()
        bucket = os.environ.get("S3_BUCKET", "globalskiatlas-backend-k8s-output")
        s3 = None if _local_root() else _s3()
        _clear_resort_work()
        work = Path(os.environ.get("WORK_DIR", "/tmp/one_resort"))
        work.mkdir(parents=True, exist_ok=True)
        if _inbox_keys(s3, bucket, "region-inbox"):
            print("sweep: inbox still has resorts, not queueing another region", flush=True)
        else:
            _flush_region_publish(s3, bucket)
            sweep_region(s3, bucket, work / "sweep")
        code = run_batch()
        _clear_resort_work()
        print("sweep: region finished, work files deleted, next region starts now", flush=True)
        return code
    if action == "batch":
        _hold_runner_lock()
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
        _note_osm_source("")
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
            if yaml:
                _drop_game_play(s3, bucket, yaml.stem)
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
        if _downhill_trail_count(work) < 1:
            _publish_resort_tables(s3, bucket, work, wid, region)
            _drop_skip(s3, bucket, wid, work)
            print("skip elevation, no downhill pistes", wid, flush=True)
            _put_job(s3, bucket, job_id, "failed", "no downhill pistes")
            return 3
        _fetch_elevation(work, wid, region)
        _publish_resort_tables(s3, bucket, work, wid, region)
        _drop_skip(s3, bucket, wid, work)
        yaml = _ensure_resort_yaml(wid, region, work)
        stage_scene_inputs(work, region, wid)
        _run([
            sys.executable, "-m", "game_export", "--resort", yaml.stem,
            "--data-root", str(work), "--cache-dir", str(work / "cache"),
            "--fetch-skadi", "--force",
        ])
        _publish_game_play(s3, bucket, work, yaml.stem)
        _run([
            sys.executable, "-m", "game_export", "--clay-scene", "--resort", yaml.stem,
            "--data-root", str(work), "--cache-dir", str(work / "cache"),
            "--fetch-skadi", "--force",
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
    except SystemExit as exc:
        if _disk_full(exc):
            _put_job(s3, bucket, job_id, "queued", "disk full, will retry")
            return 28
        _put_job(s3, bucket, job_id, "failed", str(exc.code or exc))
        return 1
    except Exception as exc:
        if _disk_full(exc):
            _put_job(s3, bucket, job_id, "queued", "disk full, will retry")
            return 28
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
