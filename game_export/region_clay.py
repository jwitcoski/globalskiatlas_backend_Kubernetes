"""Wiki admin-region clay islands (state / country), not playable game scenes."""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import numpy as np
from shapely.geometry import box, mapping, shape
from shapely.ops import unary_union
from shapely.prepared import prep

from atlas.book_gen.resort_category import is_not_downhill, resort_size_category
from atlas.map_gen.regional_overview import (
    OverviewUnit,
    _admin_match,
    _boundary_for_unit,
    _country_col,
    _downhill_mask,
    _load_boundaries,
    _name_col,
    _parquet_to_gdf,
    _state_col,
    discover_units,
)
from atlas.map_gen.wiki_page_id import wiki_country_page_id, wiki_page_id_from_row, wiki_state_page_id
from game_export import jsonutil
from game_export.config import REPO_ROOT
from game_export.coords import LocalCRS, geom_to_local, geom_to_projected, make_transformers, utm_crs_from_lonlat
from game_export.region_dem import (
    MAX_SKADI_TILES,
    country_crs_from_lonlat,
    mosaic_terrarium_for_boundary,
    shift_positive_lons_west,
    unwrap_boundary_gdf,
    unwrap_dateline_west,
)
from game_export.glb import write_terrain_glb
from game_export.region_context import bake_ne_context_into_scene, _local_from_manifest
from game_export.s3_inputs import default_s3_bucket, fetch_first_s3_object, make_s3_client
from game_export.vectors import write_local_geojson

log = logging.getLogger("game_export")

REGION_OUT_ROOT = "clay_scenes/regions"
SCHEMA_VERSION = "1.0"
SCENE_SCHEMA_VERSION = "0.1.0-region"
SCENE_KIND = "wiki_region"
DEFAULT_MESH_RESOLUTION_M = 900.0
MAX_MESH_BYTES = 6_291_456  # 6 MiB GLB; whole scene should stay under ~10 MB
MAX_MESH_VERTICES = 80_000
COUNTRY_MAX_MESH_VERTICES = 120_000
MAX_DEM_CELLS = 200_000
COUNTRY_MAX_DEM_CELLS = 700_000
MAX_WARP_CELLS = 160_000
COUNTRY_MAX_WARP_CELLS = 500_000
MAX_SPACING_M = 25_000.0
COUNTRY_MAX_SPACING_M = 12_000.0
COUNTRY_MAX_EXAGGERATE = 96.0
LAND_ELEV_FLOOR_M = -120.0  # keep Death Valley; drop Terrarium bathymetry
HERO_SPAN = 100.0
TARGET_RELIEF_FRAC = 0.10
FOOTPRINT_SIMPLIFY_M = 250.0
BOUNDARY_SIMPLIFY_M = 400.0
MAX_FOOTPRINTS = 80
VECTOR_KEYS = {
    "admin_boundary": "vectors/admin-boundary.geojson",
    "resorts": "vectors/resorts.geojson",
    "ski_area_footprints": "vectors/ski-area-footprints.geojson",
    "highways": "vectors/highways.geojson",
    "water": "vectors/water.geojson",
    "places": "vectors/places.geojson",
}
COUNTRY_VECTOR_KEYS = {
    **VECTOR_KEYS,
    "admin_1": "vectors/admin-1.geojson",
}
ADMIN1_SIMPLIFY_DEG = 0.08
ADMIN1_SIMPLIFY_M = 8000.0
REQUIRED_SCENE_FILES = (
    "scene-manifest.json",
    "terrain/terrain-mesh.glb",
    *VECTOR_KEYS.values(),
)

REGION_ATTRIBUTION_MD = """# Attribution

Decorative wiki 3D overview of an administrative region. Not a navigation or safety product.

## Administrative boundary

Natural Earth 10m admin 0 / admin 1 (same source as wiki 2D regional overview maps).

- https://www.naturalearthdata.com/

## Ski areas

OpenStreetMap winter-sports inventory processed by Global Ski Atlas.

© OpenStreetMap contributors — ODbL 1.0
https://www.openstreetmap.org/copyright

## Elevation (DEM)

Mapzen Skadi tiles (AWS Terrain Tiles), resampled to a coarse grid and clipped to the admin polygon.

`https://elevation-tiles-prod.s3.amazonaws.com/skadi`

## Context layers (highways, water, places)

Natural Earth 10m roads, river centerlines, lakes, and populated places, clipped to the admin polygon.

- https://www.naturalearthdata.com/

Ski-area footprints and resort points remain OpenStreetMap (ODbL).

## Affiliation

Not affiliated with, endorsed by, or approved by any resort operator or tourism office.

## Safety

Do not use this overview for skiing, travel, or safety decisions.
"""


def region_id_for_state(state: str, country: str) -> str:
    return wiki_state_page_id(state, country)


def region_id_for_country(country: str) -> str:
    return wiki_country_page_id(country)


def region_scene_dir(out_root: Path, region_id: str) -> Path:
    return out_root / REGION_OUT_ROOT / region_id


def region_scene_is_ready(out_root: Path, region_id: str) -> bool:
    """True when a prior --skip-existing bake can leave this scene alone.

    Requires the mesh plus all vector layers (Natural Earth context may be an
    empty FeatureCollection for tiny admin units).
    """
    scene = region_scene_dir(out_root, region_id)
    return all((scene / rel).is_file() for rel in REQUIRED_SCENE_FILES)


def list_country_units(
    data_root: Path,
    cache_dir: Path,
    *,
    from_s3: bool = True,
    s3_bucket: Optional[str] = None,
) -> list[OverviewUnit]:
    """Every country with downhill resorts in the analyzed parquet."""
    parquet = _resolve_analyzed_parquet(
        data_root, cache_dir, from_s3=from_s3, s3_bucket=s3_bucket or default_s3_bucket()
    )
    gdf = _parquet_to_gdf(parquet)
    units = [u for u in discover_units(gdf) if u.kind == "country"]
    units.sort(key=lambda u: (u.country, -u.resort_count))
    return units


def list_admin1_units(
    data_root: Path,
    cache_dir: Path,
    *,
    from_s3: bool = True,
    s3_bucket: Optional[str] = None,
    country: Optional[str] = None,
) -> list[OverviewUnit]:
    """Every downhill state/province/territory in the analyzed parquet (not countries)."""
    parquet = _resolve_analyzed_parquet(
        data_root, cache_dir, from_s3=from_s3, s3_bucket=s3_bucket or default_s3_bucket()
    )
    gdf = _parquet_to_gdf(parquet)
    units = [u for u in discover_units(gdf) if u.kind == "state" and u.state]
    if country:
        cf = country.strip().casefold()
        units = [u for u in units if u.country.strip().casefold() == cf]
    units.sort(key=lambda u: (u.country, u.state or "", -u.resort_count))
    return units


def _size_tier(row: dict[str, Any]) -> str:
    cat = resort_size_category(row)
    if cat in {"mega_resort", "multiple_mountains"}:
        return "large"
    if cat == "ski_mountain":
        return "medium"
    return "small"


def _suggested_height_exaggerate(span_m: float, relief_m: float, *, max_ex: float = 36.0) -> float:
    relief = max(float(relief_m), 1.0)
    raw = (float(span_m) * TARGET_RELIEF_FRAC) / relief
    return float(max(6.0, min(float(max_ex), round(raw, 1))))


def _resolve_analyzed_parquet(data_root: Path, cache_dir: Path, *, from_s3: bool, s3_bucket: str) -> Path:
    local = data_root / "combined" / "ski_areas_analyzed.parquet"
    if local.is_file():
        return local
    combined = data_root / "ski_areas_analyzed.parquet"
    if combined.is_file():
        return combined
    repo_local = REPO_ROOT / "output" / "combined" / "ski_areas_analyzed.parquet"
    if repo_local.is_file():
        return repo_local
    if from_s3:
        s3 = make_s3_client()
        hit = fetch_first_s3_object(
            s3,
            s3_bucket,
            ["combined/ski_areas_analyzed.parquet"],
            cache_dir,
        )
        if hit is not None:
            return hit
    raise FileNotFoundError(
        "ski_areas_analyzed.parquet not found under data-root/combined or on S3 combined/"
    )


def _resolve_ski_polygons(data_root: Path, cache_dir: Path, *, from_s3: bool, s3_bucket: str) -> Optional[Path]:
    for p in (
        data_root / "combined" / "ski_areas.parquet",
        REPO_ROOT / "output" / "combined" / "ski_areas.parquet",
    ):
        if p.is_file():
            return p
    if from_s3:
        s3 = make_s3_client()
        return fetch_first_s3_object(s3, s3_bucket, ["combined/ski_areas.parquet"], cache_dir)
    return None


def _clay_catalog_by_ws() -> dict[str, str]:
    path = REPO_ROOT / "config" / "clay_scenes" / "catalog.json"
    if not path.is_file():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for row in raw.get("resorts") or []:
        wid = str(row.get("winter_sports_id") or "").strip()
        rid = str(row.get("id") or "").strip()
        if wid and rid:
            out[wid] = rid
    return out


def _unit_from_args(*, page_id: Optional[str], state: Optional[str], country: Optional[str]) -> OverviewUnit:
    if page_id:
        pid = page_id.strip()
        if pid.startswith("state-"):
            rest = pid[len("state-") :]
            # state-{state-slug}-{country-slug}; country slugs can contain hyphens
            # Known wiki form: state-west-virginia-united-states-of-america
            if not country or not state:
                if rest.endswith("united-states-of-america"):
                    country = country or "United States of America"
                    state = state or rest[: -len("united-states-of-america")].strip("-").replace("-", " ").title()
                else:
                    raise ValueError(
                        "Pass --state and --country with --page-id unless the country is United States of America"
                    )
            return OverviewUnit(
                kind="state",
                country=country,
                state=state,
                country_slug="",
                state_slug="",
                resort_count=0,
            )
        if pid.startswith("country-"):
            if not country:
                country = pid[len("country-") :].replace("-", " ")
            return OverviewUnit(
                kind="country",
                country=country,
                state=None,
                country_slug="",
                state_slug="",
                resort_count=0,
            )
        raise ValueError(f"Unsupported pageId prefix: {pid}")
    if state and country:
        return OverviewUnit(
            kind="state",
            country=country,
            state=state,
            country_slug="",
            state_slug="",
            resort_count=0,
        )
    if country and not state:
        return OverviewUnit(
            kind="country",
            country=country,
            state=None,
            country_slug="",
            state_slug="",
            resort_count=0,
        )
    raise ValueError("Need --state and --country, or --page-id")


def _mosaic_skadi_for_boundary(
    boundary_wgs: gpd.GeoDataFrame,
    cache_dir: Path,
    target_res_m: float,
    pad_deg: float = 0.05,
):
    """Fetch Skadi tiles one at a time and resample onto a coarse WGS84 grid.

    Never mosaics full 1-arc-second statewide arrays (that OOM'd BC/Ontario).
    """
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds
    from rasterio.warp import Resampling, reproject

    from atlas.map_gen.overview_dem import HGT_NO_DATA
    from scripts.ski_area_elevation_contours import _skadi_tile_name, fetch_skadi_tile, tiles_for_bbox

    geom = unary_union(list(boundary_wgs.geometry))
    minx, miny, maxx, maxy = geom.bounds
    minx -= pad_deg
    miny -= pad_deg
    maxx += pad_deg
    maxy += pad_deg
    clat = (miny + maxy) / 2.0
    lat_m = 111_320.0
    lon_m = max(1.0, 111_320.0 * math.cos(math.radians(clat)))
    res_m = max(float(target_res_m), 250.0)
    res_deg_x = res_m / lon_m
    res_deg_y = res_m / lat_m
    width = max(2, int(math.ceil((maxx - minx) / res_deg_x)))
    height = max(2, int(math.ceil((maxy - miny) / res_deg_y)))
    while width * height > MAX_DEM_CELLS:
        res_deg_x *= 1.25
        res_deg_y *= 1.25
        width = max(2, int(math.ceil((maxx - minx) / res_deg_x)))
        height = max(2, int(math.ceil((maxy - miny) / res_deg_y)))

    log.info(
        "Skadi coarse mosaic %s×%s cells (~%.0fm) bbox=[%.2f,%.2f,%.2f,%.2f]",
        width,
        height,
        res_m,
        minx,
        miny,
        maxx,
        maxy,
    )
    dst = np.full((height, width), np.nan, dtype=np.float32)
    dst_transform = from_bounds(minx, miny, maxx, maxy, width, height)
    dst_crs = CRS.from_epsg(4326)
    tile_coords = tiles_for_bbox(miny, minx, maxy, maxx)
    prepared = prep(geom.buffer(0.02))
    tile_coords = [
        (lat_sw, lon_sw)
        for lat_sw, lon_sw in tile_coords
        if prepared.intersects(box(float(lon_sw), float(lat_sw), float(lon_sw + 1), float(lat_sw + 1)))
    ]
    got = 0
    for lat_sw, lon_sw in tile_coords:
        arr = fetch_skadi_tile(lat_sw, lon_sw, cache_dir, keep_cache=False)
        if arr is None:
            continue
        try:
            side = int(arr.shape[0])
            src = arr.astype(np.float32, copy=True)
            src[arr == HGT_NO_DATA] = np.nan
            src_transform = from_bounds(
                float(lon_sw), float(lat_sw), float(lon_sw + 1), float(lat_sw + 1), side, side
            )
            tmp = np.full((height, width), np.nan, dtype=np.float32)
            reproject(
                source=src,
                destination=tmp,
                src_transform=src_transform,
                src_crs=dst_crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                resampling=Resampling.average,
                src_nodata=np.nan,
                dst_nodata=np.nan,
            )
            hit = np.isfinite(tmp)
            dst[hit] = tmp[hit]
            got += 1
        finally:
            del arr
            name = _skadi_tile_name(lat_sw, lon_sw)
            ns = "N" if lat_sw >= 0 else "S"
            hgt = cache_dir / "skadi" / f"{ns}{abs(lat_sw)}" / f"{name}.hgt"
            hgt.unlink(missing_ok=True)
    if got == 0 or not np.any(np.isfinite(dst)):
        raise RuntimeError(f"No Skadi tiles for bbox {miny},{minx},{maxy},{maxx}")
    log.info("Skadi mosaic used %s/%s tiles (source HGT not kept on disk)", got, len(tile_coords))
    skadi_root = cache_dir / "skadi"
    if skadi_root.is_dir() and not any(skadi_root.rglob("*.hgt")):
        import shutil

        shutil.rmtree(skadi_root, ignore_errors=True)
    return dst, float(minx), float(miny), float(maxx), float(maxy), np.nan


def _warp_clip_dem(
    dem: np.ndarray,
    west: float,
    south: float,
    east: float,
    north: float,
    boundary_wgs: gpd.GeoDataFrame,
    projected: str,
    res_m: float,
    nodata: float,
    max_warp_cells: int = MAX_WARP_CELLS,
):
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds
    from rasterio.warp import Resampling, reproject
    from rasterio import features

    h, w = dem.shape
    src_transform = from_bounds(west, south, east, north, w, h)
    src_crs = CRS.from_epsg(4326)
    dst_crs = CRS.from_user_input(projected)
    boundary_proj = boundary_wgs.to_crs(projected)
    minx, miny, maxx, maxy = boundary_proj.total_bounds
    pad = res_m * 2
    minx -= pad
    miny -= pad
    maxx += pad
    maxy += pad
    width = max(2, int(math.ceil((maxx - minx) / res_m)))
    height = max(2, int(math.ceil((maxy - miny) / res_m)))
    used_res = float(res_m)
    while width * height > max_warp_cells:
        used_res *= 1.25
        width = max(2, int(math.ceil((maxx - minx) / used_res)))
        height = max(2, int(math.ceil((maxy - miny) / used_res)))
    dst_transform = from_bounds(minx, miny, maxx, maxy, width, height)
    dst = np.full((height, width), np.nan, dtype=np.float32)
    src = np.where(np.isfinite(dem), dem, np.nan).astype(np.float32, copy=False)
    reproject(
        source=src,
        destination=dst,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        resampling=Resampling.bilinear,
        src_nodata=np.nan,
        dst_nodata=np.nan,
    )
    geom = unary_union(list(boundary_proj.geometry))
    if BOUNDARY_SIMPLIFY_M > 0:
        geom = geom.simplify(BOUNDARY_SIMPLIFY_M, preserve_topology=True)
    inside = features.geometry_mask(
        [mapping(geom)],
        out_shape=(height, width),
        transform=dst_transform,
        invert=True,
    )
    dst[~inside] = np.nan
    cell_m = float(abs(dst_transform.a))
    return dst, dst_transform, geom, boundary_proj, cell_m


def _build_compact_mesh(elev: np.ndarray, transform, local: LocalCRS, cell_m: float) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict]:
    from game_export.terrain import _derivatives, _grid_xy

    rows, cols = elev.shape
    mx, my = _grid_xy(transform, rows, cols)
    valid = np.isfinite(elev)
    dzdx, dzdy = _derivatives(np.where(valid, elev, np.nan), cell_m)
    gx = -dzdx
    gy = np.ones_like(elev, dtype=np.float64)
    gz = dzdy
    nlen = np.sqrt(gx * gx + gy * gy + gz * gz)
    nlen = np.where(nlen > 1e-9, nlen, 1.0)
    gx = gx / nlen
    gy = gy / nlen
    gz = gz / nlen

    tmask = valid[:-1, :-1] & valid[:-1, 1:] & valid[1:, :-1] & valid[1:, 1:]
    ii = np.arange(rows * cols, dtype=np.uint32).reshape(rows, cols)
    i00 = ii[:-1, :-1][tmask]
    i10 = ii[:-1, 1:][tmask]
    i01 = ii[1:, :-1][tmask]
    i11 = ii[1:, 1:][tmask]
    raw_idx = np.stack([i00, i10, i11, i00, i11, i01], axis=1).reshape(-1).astype(np.uint32)
    used = np.unique(raw_idx)
    remap = np.full(rows * cols, -1, dtype=np.int64)
    remap[used] = np.arange(used.size, dtype=np.int64)
    idx = remap[raw_idx].astype(np.uint32)

    xg = (mx - local.origin_easting_m).reshape(-1)[used]
    zg = (-(my - local.origin_northing_m)).reshape(-1)[used]
    yg = elev.reshape(-1)[used]
    pos = np.stack([xg, yg, zg], axis=-1).astype(np.float32)
    nrm = np.stack(
        [
            np.nan_to_num(gx, nan=0.0).reshape(-1)[used],
            np.nan_to_num(gy, nan=1.0).reshape(-1)[used],
            np.nan_to_num(gz, nan=0.0).reshape(-1)[used],
        ],
        axis=-1,
    ).astype(np.float32)
    meta = {
        "vertex_count": int(pos.shape[0]),
        "triangle_count": int(idx.size // 3),
        "grid_rows": int(rows),
        "grid_cols": int(cols),
    }
    return pos, nrm, idx, meta


def _pick_resolution_m(geom_proj, start_m: float, *, max_spacing_m: float = MAX_SPACING_M) -> float:
    area = float(geom_proj.area)
    if area <= 0:
        return max(start_m, 500.0)
    spacing = math.sqrt(area / 55000.0)
    return float(max(250.0, min(max_spacing_m, max(start_m, spacing))))


def _resorts_in_region(gdf: gpd.GeoDataFrame, unit: OverviewUnit, admin_wgs) -> gpd.GeoDataFrame:
    country_c = _country_col(gdf)
    state_c = _state_col(gdf)
    mask = _downhill_mask(gdf)
    sub = gdf[mask].copy()
    country_ok = sub[country_c].astype(str).str.strip().str.casefold() == unit.country.strip().casefold()
    sub = sub[country_ok]
    if unit.kind == "state" and unit.state and state_c:
        state_ok = sub[state_c].astype(str).str.strip().str.casefold() == unit.state.strip().casefold()
        sub = sub[state_ok]
    if sub.empty:
        return sub
    poly = unwrap_dateline_west(unary_union(list(admin_wgs.geometry)))
    unwrap = poly.bounds[0] < -180
    cents = []
    for _, row in sub.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        pt = geom if geom.geom_type == "Point" else geom.centroid
        if unwrap and pt.x > 0:
            from shapely.geometry import Point

            pt = Point(pt.x - 360.0, pt.y)
        if not pt.within(poly) and not pt.intersects(poly):
            # keep state+country matches even if centroid sits just outside the NE outline
            if unit.kind == "state":
                cents.append(row)
            continue
        cents.append(row)
    if not cents:
        return sub.iloc[0:0]
    return gpd.GeoDataFrame(cents, crs=gdf.crs)


def _ne_states_in_country(
    states: gpd.GeoDataFrame, countries: gpd.GeoDataFrame, country: str
) -> gpd.GeoDataFrame:
    cfold = country.strip().casefold()
    hit = states.iloc[0:0]
    for col in ("admin", "ADMIN", "geonunit"):
        if col not in states.columns:
            continue
        cand = states[states[col].astype(str).str.strip().str.casefold() == cfold]
        if not cand.empty:
            hit = cand
            break
    if hit.empty and "iso_a2" in states.columns:
        cmatch = _admin_match(countries, "ADMIN", country)
        if cmatch.empty:
            cmatch = _admin_match(countries, "NAME", country)
        if not cmatch.empty and "iso_a2" in cmatch.columns:
            iso = str(cmatch.iloc[0]["iso_a2"]).strip()
            hit = states[states["iso_a2"].astype(str).str.strip() == iso]
    return hit


def bake_admin1_click_layer(
    out: Path,
    *,
    unit: OverviewUnit,
    ski_gdf: gpd.GeoDataFrame,
    countries: gpd.GeoDataFrame,
    states: gpd.GeoDataFrame,
    local: LocalCRS,
    admin_geom,
    out_root: Path,
) -> list[dict[str, Any]]:
    """Country-only: downhill admin-1 polygons with wiki pageId for click-through."""
    dest = out / "vectors" / "admin-1.geojson"
    if unit.kind != "country":
        return []
    children = [
        u
        for u in discover_units(ski_gdf)
        if u.kind == "state"
        and u.state
        and u.country.strip().casefold() == unit.country.strip().casefold()
    ]
    ne = _ne_states_in_country(states, countries, unit.country)
    unwrap_children = float(admin_geom.bounds[0]) < -180.0
    to_proj, _to_wgs = make_transformers(local.projected_crs)
    features: list[dict] = []
    catalog_rows: list[dict[str, Any]] = []
    for child in children:
        page_id = region_id_for_state(child.state or "", child.country)
        rowg = _admin_match(ne, "name", child.state or "")
        if rowg.empty:
            rowg = _admin_match(ne, "NAME", child.state or "")
        if rowg.empty:
            log.warning("No Natural Earth admin-1 polygon for %s", page_id)
            continue
        geom = unary_union(list(rowg.geometry))
        geom = unwrap_dateline_west(geom)
        if unwrap_children:
            geom = shift_positive_lons_west(geom)
        if ADMIN1_SIMPLIFY_DEG > 0:
            geom = geom.simplify(ADMIN1_SIMPLIFY_DEG, preserve_topology=True)
        if geom is None or geom.is_empty:
            continue
        gproj = geom_to_projected(geom, to_proj)
        if not gproj.is_valid:
            gproj = gproj.buffer(0)
        gproj = gproj.simplify(ADMIN1_SIMPLIFY_M, preserve_topology=True)
        gloc = geom_to_local(gproj, local)
        ready = region_scene_is_ready(out_root, page_id)
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(gloc),
                "properties": {
                    "kind": "admin1",
                    "pageId": page_id,
                    "pageType": "state",
                    "title": child.state,
                    "state": child.state,
                    "country": child.country,
                    "resort_count": int(child.resort_count),
                    "has_region_scene": ready,
                    "scene": f"clay_scenes/regions/{page_id}/scene-manifest.json",
                },
            }
        )
        catalog_rows.append(
            {
                "id": page_id,
                "pageId": page_id,
                "title": child.state,
                "resort_count": int(child.resort_count),
                "ready": ready,
            }
        )
    write_local_geojson(dest, features, local, "admin-1")
    log.info("Country admin-1 click targets %s: %s polygons", unit.country, len(features))
    return catalog_rows


def _lonlat_of_row(row) -> tuple[Optional[float], Optional[float]]:
    geom = row.geometry
    if geom is not None and not geom.is_empty:
        pt = geom if geom.geom_type == "Point" else geom.centroid
        return float(pt.x), float(pt.y)
    for lon_k, lat_k in (("centroid_lon", "centroid_lat"), ("lon", "lat")):
        if lon_k in row and lat_k in row and row[lon_k] == row[lon_k] and row[lat_k] == row[lat_k]:
            return float(row[lon_k]), float(row[lat_k])
    return None, None


def _write_region_attribution(out: Path) -> None:
    att = out / "attribution"
    att.mkdir(parents=True, exist_ok=True)
    (att / "ATTRIBUTION.md").write_text(REGION_ATTRIBUTION_MD, encoding="utf-8")
    (att / "sources.json").write_text(
        jsonutil.dumps(
            {
                "admin_boundary": {
                    "provider": "Natural Earth 10m admin 0 / admin 1",
                    "url": "https://www.naturalearthdata.com/",
                    "note": "Same polygons as wiki 2D regional overview maps.",
                },
                "natural_earth_context": {
                    "provider": "Natural Earth 10m",
                    "url": "https://www.naturalearthdata.com/",
                    "license": "public domain",
                    "layers": ["highways", "water", "places"],
                    "files": [
                        "ne_10m_roads",
                        "ne_10m_rivers_lake_centerlines",
                        "ne_10m_lakes",
                        "ne_10m_populated_places",
                    ],
                    "note": "Clipped to the admin polygon; same local-meter CRS as resorts.geojson.",
                },
                "osm": {
                    "attribution": "© OpenStreetMap contributors",
                    "license": "ODbL 1.0",
                    "url": "https://www.openstreetmap.org/copyright",
                    "layers": ["resorts", "ski_area_footprints"],
                },
                "dem": {
                    "provider": "Mapzen Skadi / AWS Terrain Tiles",
                    "url": "https://elevation-tiles-prod.s3.amazonaws.com/skadi",
                },
            }
        ),
        encoding="utf-8",
    )


def _catalog_path() -> Path:
    return REPO_ROOT / "config" / "clay_scenes" / "regions" / "catalog.json"


def upsert_region_catalog(entry: dict[str, Any]) -> Path:
    path = _catalog_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        catalog = json.loads(path.read_text(encoding="utf-8"))
    else:
        catalog = {
            "schema_version": SCHEMA_VERSION,
            "description": "Clay 3D admin-region scenes for wiki 3D Map (state/country/continent).",
            "path_contract": {
                "asset_root": "clay_scenes/regions/{region_id}/",
                "catalog": "clay_scenes/regions/catalog.json",
                "wiki_join_key": "pageId",
                "by_page": "clay_scenes/regions/by-page/{pageId}.json",
                "region_id_rule": (
                    "region_id == wiki pageId. States: state-{state-slug}-{country-slug}. "
                    "Countries: country-{country-slug}. Slugs: lowercase, hyphenated, [a-z0-9-]."
                ),
            },
            "regions": [],
        }
    regions = [r for r in (catalog.get("regions") or []) if r.get("id") != entry["id"]]
    regions.append(entry)
    regions.sort(key=lambda r: str(r.get("id") or ""))
    catalog["schema_version"] = SCHEMA_VERSION
    catalog["regions"] = regions
    path.write_text(json.dumps(catalog, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return path


def write_by_page_stub(out_root: Path, entry: dict[str, Any]) -> None:
    dest = out_root / "by-page" / f"{entry['pageId']}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        jsonutil.dumps(
            {
                "id": entry["id"],
                "pageId": entry["pageId"],
                "ready": bool(entry.get("ready")),
            }
        ),
        encoding="utf-8",
    )


def export_region_scene(
    *,
    state: Optional[str] = None,
    country: Optional[str] = None,
    page_id: Optional[str] = None,
    data_root: Path,
    cache_dir: Path,
    out_root: Path,
    from_s3: bool = True,
    s3_bucket: Optional[str] = None,
    mesh_resolution_m: Optional[float] = None,
    force: bool = False,
) -> Path:
    s3_bucket = s3_bucket or default_s3_bucket()
    unit = _unit_from_args(page_id=page_id, state=state, country=country)
    if unit.kind == "state":
        region_id = region_id_for_state(unit.state or "", unit.country)
        title = unit.state or region_id
    else:
        region_id = region_id_for_country(unit.country)
        title = unit.country
    page_type = unit.kind

    out = out_root / REGION_OUT_ROOT / region_id
    if out.exists() and not force and (out / "terrain" / "terrain-mesh.glb").is_file():
        log.info("Region clay already exists: %s — refreshing Natural Earth context vectors", out)
        return refresh_region_osm_vectors(
            state=state,
            country=country,
            page_id=page_id,
            cache_dir=cache_dir,
            out_root=out_root,
        )
    if out.exists() and force:
        import shutil

        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    countries, states = _load_boundaries()
    boundary_wgs = _boundary_for_unit(unit, countries, states)
    boundary_wgs = unwrap_boundary_gdf(boundary_wgs)
    admin_geom = unwrap_dateline_west(unary_union(list(boundary_wgs.geometry)))
    minx, miny, maxx, maxy = admin_geom.bounds
    clon = (minx + maxx) / 2.0
    clat = (miny + maxy) / 2.0
    if unit.kind == "country":
        projected = country_crs_from_lonlat(clon, clat)
    else:
        projected = utm_crs_from_lonlat(clon, clat)
    to_proj, to_wgs = make_transformers(projected)

    analyzed_path = _resolve_analyzed_parquet(data_root, cache_dir, from_s3=from_s3, s3_bucket=s3_bucket)
    ski_gdf = _parquet_to_gdf(analyzed_path)
    resorts_gdf = _resorts_in_region(ski_gdf, unit, boundary_wgs)
    log.info("%s downhill resorts in %s", len(resorts_gdf), region_id)

    geom_proj = geom_to_projected(admin_geom, to_proj)
    start_res = float(mesh_resolution_m) if mesh_resolution_m is not None else DEFAULT_MESH_RESOLUTION_M
    country = unit.kind == "country"
    max_spacing = COUNTRY_MAX_SPACING_M if country else MAX_SPACING_M
    max_verts = COUNTRY_MAX_MESH_VERTICES if country else MAX_MESH_VERTICES
    max_warp = COUNTRY_MAX_WARP_CELLS if country else MAX_WARP_CELLS
    if country and mesh_resolution_m is None:
        start_res = max(start_res, 5000.0)
    res = _pick_resolution_m(geom_proj, start_res, max_spacing_m=max_spacing)
    use_terrarium = country
    if not use_terrarium:
        from scripts.ski_area_elevation_contours import tiles_for_bbox

        b = admin_geom.bounds
        n_tiles = len(tiles_for_bbox(b[1], b[0], b[3], b[2]))
        use_terrarium = n_tiles > MAX_SKADI_TILES
    if use_terrarium:
        dem, west, south, east, north, nodata = mosaic_terrarium_for_boundary(
            boundary_wgs,
            cache_dir,
            target_res_m=res,
            max_cells=COUNTRY_MAX_DEM_CELLS if country else MAX_DEM_CELLS,
        )
    else:
        dem, west, south, east, north, nodata = _mosaic_skadi_for_boundary(
            boundary_wgs, cache_dir, target_res_m=res
        )

    centroid = geom_proj.centroid
    olon, olat = to_wgs.transform(centroid.x, centroid.y)
    local = LocalCRS(
        source_crs="EPSG:4326",
        projected_crs=projected,
        origin_easting_m=float(centroid.x),
        origin_northing_m=float(centroid.y),
        origin_longitude=float(olon),
        origin_latitude=float(olat),
    )

    terrain_dir = out / "terrain"
    terrain_dir.mkdir(parents=True, exist_ok=True)
    mesh_bytes = 0
    glb_info: dict[str, Any] = {}
    compact_meta: dict[str, Any] = {}
    zmin = zmax = 0.0
    elev = None
    transform = None

    while True:
        try:
            elev, transform, geom_clip, _bp, cell_m = _warp_clip_dem(
                dem, west, south, east, north, boundary_wgs, projected, res, nodata,
                max_warp_cells=max_warp,
            )
        except MemoryError:
            log.warning("Warp OOM at %sm — coarsening", res)
            next_res = round(res * 1.5, 1)
            if next_res <= res or next_res > max_spacing:
                raise
            res = next_res
            continue
        res = float(cell_m)
        if country:
            elev = elev.copy()
            elev[np.isfinite(elev) & (elev < LAND_ELEV_FLOOR_M)] = np.nan
        pos, nrm, idx, compact_meta = _build_compact_mesh(elev, transform, local, res)
        too_heavy = compact_meta["vertex_count"] > max_verts
        if not too_heavy:
            glb_info = write_terrain_glb(terrain_dir / "terrain-mesh.glb", pos, nrm, idx)
            mesh_bytes = int(glb_info.get("byte_size") or (terrain_dir / "terrain-mesh.glb").stat().st_size)
            too_heavy = mesh_bytes > MAX_MESH_BYTES
        if not too_heavy:
            break
        log.warning(
            "Region mesh over budget at %sm (verts=%s) — coarsening",
            res,
            f"{compact_meta['vertex_count']:,}",
        )
        next_res = round(res * 1.35, 1)
        if next_res <= res or next_res > max_spacing:
            if compact_meta["vertex_count"] > max_verts:
                glb_info = write_terrain_glb(terrain_dir / "terrain-mesh.glb", pos, nrm, idx)
                mesh_bytes = int(glb_info.get("byte_size") or (terrain_dir / "terrain-mesh.glb").stat().st_size)
            break
        res = next_res

    valid_elev = elev[np.isfinite(elev)]
    zmin = float(np.nanmin(valid_elev))
    zmax = float(np.nanmax(valid_elev))
    span_m = max(geom_proj.bounds[2] - geom_proj.bounds[0], geom_proj.bounds[3] - geom_proj.bounds[1])
    height_exaggerate = _suggested_height_exaggerate(
        span_m,
        zmax - zmin,
        max_ex=COUNTRY_MAX_EXAGGERATE if country else 36.0,
    )

    terrain_meta = {
        "kind": SCENE_KIND,
        "region_id": region_id,
        "source_crs": "EPSG:4326",
        "projected_crs": projected,
        "work_resolution_m": res,
        "height_exaggerate": height_exaggerate,
        "clip": "admin_polygon",
        "mesh": {
            "file": "terrain-mesh.glb",
            **glb_info,
            "vertex_spacing_m": res,
            "coordinate_space": "game (X east, Y elevation, Z negative north)",
            **compact_meta,
        },
        "local_crs": local.to_dict(),
        "elevation_min_m": zmin,
        "elevation_max_m": zmax,
    }
    (terrain_dir / "terrain-metadata.json").write_text(jsonutil.dumps(terrain_meta), encoding="utf-8")

    local_admin = geom_to_local(geom_clip, local)
    admin_features = [
        {
            "type": "Feature",
            "geometry": mapping(local_admin),
            "properties": {"kind": "admin_boundary", "pageId": region_id, "title": title},
        }
    ]
    write_local_geojson(out / "vectors" / "admin-boundary.geojson", admin_features, local, "admin-boundary")

    clay_by_ws = _clay_catalog_by_ws()
    name_c = _name_col(resorts_gdf) if not resorts_gdf.empty else "name"
    resort_features = []
    for _, row in resorts_gdf.iterrows():
        lon, lat = _lonlat_of_row(row)
        if lon is None or lat is None:
            continue
        if minx < -180 and lon > 0:
            lon -= 360.0
        east, north = to_proj.transform(lon, lat)
        local_e = east - local.origin_easting_m
        local_n = north - local.origin_northing_m
        d = row.to_dict()
        wid = str(d.get("winter_sports_id") or d.get("osm_id") or "").strip()
        name = str(d.get(name_c) or d.get("english_name") or d.get("name") or "Unknown").strip()
        wiki_pid = wiki_page_id_from_row(
            {
                "name": name,
                "english_name": d.get("english_name") or name,
                "state": d.get("state") or d.get("State") or unit.state,
                "country": d.get("country") or d.get("Country") or unit.country,
            }
        )
        clay_id = clay_by_ws.get(wid)
        trails = d.get("downhill_trails")
        try:
            trails_n = int(float(trails)) if trails is not None and trails == trails else None
        except (TypeError, ValueError):
            trails_n = None
        resort_features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(local_e), float(local_n)]},
                "properties": {
                    "name": name,
                    "english_name": str(d.get("english_name") or name),
                    "winter_sports_id": wid or None,
                    "wiki_pageId": wiki_pid,
                    "state": unit.state if unit.kind == "state" else (d.get("state") or d.get("State")),
                    "country": unit.country,
                    "lon": float(lon),
                    "lat": float(lat),
                    "size_tier": _size_tier(d),
                    "downhill_trails": trails_n,
                    "has_clay_scene": bool(clay_id),
                    "clay_scene_id": clay_id,
                },
            }
        )
    write_local_geojson(out / "vectors" / "resorts.geojson", resort_features, local, "resorts")

    footprints: list[dict] = []
    poly_path = _resolve_ski_polygons(data_root, cache_dir, from_s3=from_s3, s3_bucket=s3_bucket)
    if unit.kind != "country" and poly_path is not None and resort_features:
        try:
            polys = gpd.read_parquet(poly_path)
            if polys.crs is None:
                polys = polys.set_crs("EPSG:4326")
            elif polys.crs.to_epsg() != 4326:
                polys = polys.to_crs("EPSG:4326")
            id_col = next((c for c in ("winter_sports_id", "osm_way_id", "osm_id") if c in polys.columns), None)
            wanted = {str(f["properties"]["winter_sports_id"]) for f in resort_features if f["properties"].get("winter_sports_id")}
            n = 0
            if id_col:
                for _, row in polys.iterrows():
                    wid = str(row.get(id_col) or "").strip()
                    if wid not in wanted:
                        continue
                    geom = row.geometry
                    if geom is None or geom.is_empty or geom.geom_type == "Point":
                        continue
                    gproj = geom_to_projected(geom, to_proj)
                    gproj = gproj.simplify(FOOTPRINT_SIMPLIFY_M, preserve_topology=True)
                    gloc = geom_to_local(gproj, local)
                    footprints.append(
                        {
                            "type": "Feature",
                            "geometry": mapping(gloc),
                            "properties": {"winter_sports_id": wid, "kind": "ski_area_footprint"},
                        }
                    )
                    n += 1
                    if n >= MAX_FOOTPRINTS:
                        break
        except Exception as exc:
            log.warning("Skipping ski-area footprints: %s", exc)
    write_local_geojson(out / "vectors" / "ski-area-footprints.geojson", footprints, local, "ski-area-footprints")

    pad = 0.02
    bbox_wgs = (float(minx) - pad, float(miny) - pad, float(maxx) + pad, float(maxy) + pad)
    bake_ne_context_into_scene(
        out,
        admin_wgs=admin_geom,
        local=local,
        region_id=region_id,
        cache_dir=cache_dir,
        bbox_wgs=bbox_wgs,
    )
    admin1_rows: list[dict[str, Any]] = []
    vector_keys = dict(VECTOR_KEYS)
    if unit.kind == "country":
        admin1_rows = bake_admin1_click_layer(
            out,
            unit=unit,
            ski_gdf=ski_gdf,
            countries=countries,
            states=states,
            local=local,
            admin_geom=admin_geom,
            out_root=out_root,
        )
        vector_keys = dict(COUNTRY_VECTOR_KEYS)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = {
        "scene_schema_version": SCENE_SCHEMA_VERSION,
        "scene_kind": SCENE_KIND,
        "region_id": region_id,
        "pageId": region_id,
        "pageType": page_type,
        "title": title,
        "state": unit.state,
        "country": unit.country,
        "display_name": title,
        "build_timestamp_utc": now,
        "coordinate_system": local.to_dict(),
        "terrain": {
            "mesh": "terrain/terrain-mesh.glb",
            "mesh_metadata": "terrain/terrain-metadata.json",
            "elevation_min_m": zmin,
            "elevation_max_m": zmax,
            "vertex_spacing_m": res,
            "height_exaggerate": height_exaggerate,
        },
        "vectors": vector_keys,
        "camera": {
            "suggested_hero_span": HERO_SPAN,
            "height_exaggerate": height_exaggerate,
        },
        "attribution": {
            "osm": "© OpenStreetMap contributors",
            "dem": "Mapzen Skadi / AWS Terrain Tiles",
            "admin_boundary": "Natural Earth 10m admin 1 / admin 0",
            "highways": "Natural Earth 10m roads",
            "water": "Natural Earth 10m rivers and lakes",
            "places": "Natural Earth 10m populated places",
            **(
                {"admin_1": "Natural Earth 10m admin 1 (click targets; join on pageId)"}
                if unit.kind == "country"
                else {}
            ),
        },
        "disclaimer": "Decorative overview terrain. Not a navigation or safety product.",
    }
    (out / "scene-manifest.json").write_text(jsonutil.dumps(manifest), encoding="utf-8")
    _write_region_attribution(out)
    (out / "README.md").write_text(
        "# Wiki region clay scene\n\n"
        "Decorative admin-boundary island for the wiki 3D Map tab on state/country pages. "
        "Join key is wiki `pageId` (`region_id`). Not a playable `game_scenes` cake.\n",
        encoding="utf-8",
    )

    glb = out / "terrain" / "terrain-mesh.glb"
    ready = glb.is_file()
    entry = {
        "id": region_id,
        "pageId": region_id,
        "pageType": page_type,
        "title": title,
        "state": unit.state,
        "country": unit.country,
        "display_name": title,
        "bbox": [float(minx), float(miny), float(maxx), float(maxy)],
        "resort_count": len(resort_features),
        "ready": ready,
        "projected_crs": projected,
        "vertex_spacing_m": res,
        "height_exaggerate": height_exaggerate,
        "glb_bytes": int(glb.stat().st_size) if ready else 0,
    }
    if admin1_rows:
        entry["admin1"] = admin1_rows
    catalog_path = upsert_region_catalog(entry)
    write_by_page_stub(out_root / REGION_OUT_ROOT, entry)
    # Keep a copy of catalog next to scenes for upload
    dest_cat = out_root / REGION_OUT_ROOT / "catalog.json"
    dest_cat.write_text(catalog_path.read_text(encoding="utf-8"), encoding="utf-8")

    log.info(
        "Region clay %s: mesh=%s verts=%s resorts=%s exaggerate=%s",
        out,
        f"{mesh_bytes:,}",
        compact_meta.get("vertex_count"),
        len(resort_features),
        height_exaggerate,
    )
    return out


def refresh_region_osm_vectors(
    *,
    state: Optional[str] = None,
    country: Optional[str] = None,
    page_id: Optional[str] = None,
    cache_dir: Path,
    out_root: Path,
    data_root: Optional[Path] = None,
) -> Path:
    """Add/replace Natural Earth context vectors without rebuilding the terrain mesh."""
    unit = _unit_from_args(page_id=page_id, state=state, country=country)
    if unit.kind == "state":
        region_id = region_id_for_state(unit.state or "", unit.country)
    else:
        region_id = region_id_for_country(unit.country)
    out = out_root / REGION_OUT_ROOT / region_id
    man_path = out / "scene-manifest.json"
    if not man_path.is_file():
        raise FileNotFoundError(f"No region scene to patch: {man_path}")
    manifest = json.loads(man_path.read_text(encoding="utf-8"))
    local = _local_from_manifest(manifest)
    countries, states = _load_boundaries()
    boundary_wgs = unwrap_boundary_gdf(_boundary_for_unit(unit, countries, states))
    admin_geom = unwrap_dateline_west(unary_union(list(boundary_wgs.geometry)))
    minx, miny, maxx, maxy = admin_geom.bounds
    pad = 0.02
    bbox_wgs = (float(minx) - pad, float(miny) - pad, float(maxx) + pad, float(maxy) + pad)
    bake_ne_context_into_scene(
        out,
        admin_wgs=admin_geom,
        local=local,
        region_id=region_id,
        cache_dir=cache_dir,
        bbox_wgs=bbox_wgs,
    )
    vector_keys = dict(VECTOR_KEYS)
    if unit.kind == "country":
        ski_gdf = _parquet_to_gdf(
            _resolve_analyzed_parquet(
                data_root or REPO_ROOT / "output",
                cache_dir,
                from_s3=True,
                s3_bucket=default_s3_bucket(),
            )
        )
        bake_admin1_click_layer(
            out,
            unit=unit,
            ski_gdf=ski_gdf,
            countries=countries,
            states=states,
            local=local,
            admin_geom=admin_geom,
            out_root=out_root,
        )
        vector_keys = dict(COUNTRY_VECTOR_KEYS)
    manifest["vectors"] = vector_keys
    att = dict(manifest.get("attribution") or {})
    att.setdefault("osm", "© OpenStreetMap contributors")
    att["admin_boundary"] = "Natural Earth 10m admin 1 / admin 0"
    att["highways"] = "Natural Earth 10m roads"
    att["water"] = "Natural Earth 10m rivers and lakes"
    att["places"] = "Natural Earth 10m populated places"
    if unit.kind == "country":
        att["admin_1"] = "Natural Earth 10m admin 1 (click targets; join on pageId)"
    manifest["attribution"] = att
    man_path.write_text(jsonutil.dumps(manifest), encoding="utf-8")
    _write_region_attribution(out)
    return out
