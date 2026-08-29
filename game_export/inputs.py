"""Resolve existing pipeline OSM + DEM inputs. Never overwrite pipeline outputs."""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from shapely.geometry import Point
from shapely.ops import unary_union

from game_export.config import GameExportConfig, REPO_ROOT
from game_export.s3_inputs import (
    DEFAULT_BUCKET,
    dem_key_candidates,
    fetch_first_s3_object,
    make_s3_client,
    parquet_key_candidates,
    read_cached_etag,
)

log = logging.getLogger("game_export")

HGT_NO_DATA = -32768


@dataclass
class ResolvedInputs:
    ski_area: gpd.GeoSeries
    ski_polygon: Any
    region_dir: Path
    osm_nearby: Optional[gpd.GeoDataFrame]
    pistes: Optional[gpd.GeoDataFrame]
    lifts: Optional[gpd.GeoDataFrame]
    elevation_points: Optional[gpd.GeoDataFrame]
    dem_path: Path
    dem_source_note: str
    warnings: list[str]
    input_files: list[Path]


def _id_match(series, wid: str):
    s = series.map(lambda v: str(v).strip() if v is not None else "")
    try:
        as_int = str(int(float(wid)))
    except ValueError:
        as_int = wid
    return s.eq(wid) | s.eq(as_int) | s.map(lambda v: v.split(".")[0] == as_int)


def _region_dirs(data_root: Path, cfg: GameExportConfig) -> list[Path]:
    parts = cfg.region_dir_parts
    return [
        data_root.joinpath(*parts),
        data_root / "combined",
        Path("/data").joinpath(*parts),
        Path("/data"),
    ]


def _find_parquet(data_root: Path, cfg: GameExportConfig, name: str) -> Optional[Path]:
    for d in _region_dirs(data_root, cfg):
        p = d / name
        if p.is_file():
            return p
    return None


def _load_gdf(path: Optional[Path]) -> Optional[gpd.GeoDataFrame]:
    if path is None:
        return None
    gdf = gpd.read_parquet(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def _ski_area_parquet_paths(data_root: Path, cfg: GameExportConfig) -> list[Path]:
    seen: set[Path] = set()
    paths: list[Path] = []
    for d in _region_dirs(data_root, cfg):
        p = d / "ski_areas.parquet"
        if p.is_file() and p not in seen:
            seen.add(p)
            paths.append(p)
    return paths


def _ski_row_from_parquet(path: Path, cfg: GameExportConfig) -> Optional[gpd.GeoSeries]:
    gdf = _load_gdf(path)
    if gdf is None:
        return None
    id_col = None
    for c in ("winter_sports_id", "osm_way_id", "osm_id", "id"):
        if c in gdf.columns:
            id_col = c
            break
    if id_col is None:
        return None
    hit = gdf[_id_match(gdf[id_col], cfg.winter_sports_id)]
    if hit.empty:
        return None
    row = hit.iloc[0]
    geom = row.geometry
    if geom is None or geom.is_empty or isinstance(geom, Point):
        return None
    log.info("Using ski area from %s (%s features matched, using first)", path, len(hit))
    return row


def find_ski_area(data_root: Path, cfg: GameExportConfig) -> tuple[gpd.GeoSeries, Path]:
    paths = _ski_area_parquet_paths(data_root, cfg)
    if not paths:
        raise FileNotFoundError(
            "ski_areas.parquet not found. Expected under "
            f"{data_root / cfg.region} or {data_root / 'combined'}. "
            "Run the existing regional pipeline first, or use --from-s3."
        )
    last_detail = ""
    for path in paths:
        row = _ski_row_from_parquet(path, cfg)
        if row is not None:
            return row, path.parent
        last_detail = f"id {cfg.winter_sports_id} not in {path} (or no polygon)"
    raise RuntimeError(
        f"Ski area winter_sports_id={cfg.winter_sports_id} not found. {last_detail}"
    )


def _dem_candidates(data_root: Path, cfg: GameExportConfig) -> list[Path]:
    wid = cfg.winter_sports_id
    parts = cfg.region_dir_parts
    out = [
        data_root.joinpath(*parts, "dems", *parts, f"{wid}.tif"),
        data_root.joinpath(*parts, "dems", f"{wid}.tif"),
        data_root / "combined" / "dems" / Path(*parts) / f"{wid}.tif",
        data_root / "combined" / "dems" / f"{wid}.tif",
    ]
    return out


def _load_elevation_module():
    import importlib.util

    path = REPO_ROOT / "scripts" / "ski_area_elevation_contours.py"
    spec = importlib.util.spec_from_file_location("ski_area_elevation_contours", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _mosaic_from_skadi_cache(
    geom,
    cache_dir: Path,
    fetch: bool,
) -> tuple[np.ndarray, float, float, float, float]:
    """Reuse existing elevation helpers. fetch=False refuses network."""
    elev_mod = _load_elevation_module()

    buffered = elev_mod._buffer_geom_meters(geom, elev_mod.CONTOUR_BUFFER_METERS)
    minlon, minlat, maxlon, maxlat = buffered.bounds
    tiles = elev_mod.tiles_for_bbox(minlat, minlon, maxlat, maxlon)
    tiles_data = []
    for lat_sw, lon_sw in tiles:
        name = elev_mod._skadi_tile_name(lat_sw, lon_sw)
        ns = "N" if lat_sw >= 0 else "S"
        subdir = f"{ns}{abs(lat_sw)}"
        hgt = cache_dir / "skadi" / subdir / f"{name}.hgt"
        if not fetch and not hgt.is_file():
            continue
        if not fetch:
            arr = elev_mod._read_hgt_file(hgt, lat_sw, lon_sw)
        else:
            arr = elev_mod.fetch_skadi_tile(lat_sw, lon_sw, cache_dir)
        if arr is not None:
            tiles_data.append((lat_sw, lon_sw, arr))
    if not tiles_data:
        raise FileNotFoundError(
            "DEM GeoTIFF not found and Skadi cache has no covering tiles. "
            "Place a cropped GeoTIFF under output/<region>/dems/ or cache/skadi/, "
            "or re-run with --fetch-skadi (same Mapzen source as the existing elevation stage). "
            f"cache_dir={cache_dir}"
        )
    dem, west, south, east, north = elev_mod._mosaic_tiles(tiles_data)
    crop, cwest, csouth, ceast, cnorth = elev_mod._crop_dem_to_bbox(
        dem, west, south, east, north, minlon, minlat, maxlon, maxlat
    )
    return crop, cwest, csouth, ceast, cnorth


def _write_temp_dem(arr, west, south, east, north, path: Path) -> None:
    h, w = arr.shape
    transform = from_bounds(west, south, east, north, w, h)
    path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "width": w,
        "height": h,
        "count": 1,
        "dtype": "int16",
        "crs": CRS.from_epsg(4326),
        "transform": transform,
        "nodata": HGT_NO_DATA,
        "compress": "deflate",
        "tiled": True,
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr.astype(np.int16), 1)


def osm_feature_hull(*gdfs) -> Any:
    """Convex hull of mapped OSM geometries (pistes, lifts, nearby), not the winter_sports AOI."""
    geoms = []
    for gdf in gdfs:
        if gdf is None or getattr(gdf, "empty", True):
            continue
        for g in gdf.geometry:
            if g is None or g.is_empty:
                continue
            geoms.append(g)
    if not geoms:
        return None
    return unary_union(geoms).convex_hull


def _dem_valid_over_geom(path: Path, geom) -> bool:
    """True when the GeoTIFF covers *geom* with real elevations (not ski-AOI nodata)."""
    if geom is None or geom.is_empty or not path.is_file():
        return False
    try:
        with rasterio.open(path) as src:
            minx, miny, maxx, maxy = geom.bounds
            b = src.bounds
            pad = 0.00025
            if not (
                b.left <= minx + pad
                and b.right >= maxx - pad
                and b.bottom <= miny + pad
                and b.top >= maxy - pad
            ):
                return False
            coords: list[tuple[float, float]] = [(geom.centroid.x, geom.centroid.y)]
            ext = getattr(geom, "exterior", None)
            if ext is not None and not ext.is_empty:
                n = max(12, min(64, int(max(ext.length, 0.01) / 0.0015)))
                for i in range(n):
                    p = ext.interpolate(i / n, normalized=True)
                    coords.append((float(p.x), float(p.y)))
            nodata = src.nodata
            bad = 0
            n_ok = 0
            for val in src.sample(coords):
                n_ok += 1
                v = val[0]
                if v is None or (nodata is not None and v == nodata):
                    bad += 1
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    bad += 1
                    continue
                if not np.isfinite(fv) or fv <= HGT_NO_DATA + 1:
                    bad += 1
            return n_ok > 0 and (bad / n_ok) <= 0.08
    except Exception as exc:
        log.warning("DEM coverage check failed for %s: %s", path, exc)
        return False


def resolve_dem(
    data_root: Path,
    cfg: GameExportConfig,
    ski_polygon,
    cache_dir: Path,
    fetch_skadi: bool,
    *,
    s3=None,
    s3_bucket: Optional[str] = None,
    coverage_geom=None,
) -> tuple[Path, str]:
    cover = coverage_geom if coverage_geom is not None and not getattr(coverage_geom, "is_empty", True) else ski_polygon
    candidates = list(_dem_candidates(data_root, cfg))
    if s3 is not None:
        s3_dem = fetch_first_s3_object(
            s3,
            s3_bucket or DEFAULT_BUCKET,
            dem_key_candidates(cfg.region, cfg.winter_sports_id),
            cache_dir,
        )
        if s3_dem is not None:
            candidates.append(s3_dem)
    old_cache = cache_dir / "game_export_dems" / cfg.region / f"{cfg.winter_sports_id}.tif"
    hull_cache = cache_dir / "game_export_dems" / cfg.region / f"{cfg.winter_sports_id}-osmhull.tif"
    candidates.extend([hull_cache, old_cache])
    seen: set[Path] = set()
    for p in candidates:
        if p is None or not p.is_file() or p in seen:
            continue
        seen.add(p)
        if _dem_valid_over_geom(p, cover):
            log.info("Using DEM GeoTIFF covering OSM hull %s", p)
            return p, f"GeoTIFF covering OSM hull {p}"
        log.info("Skipping DEM that does not cover OSM hull (likely ski-AOI mask): %s", p)
    log.info("Mosaicking Skadi DEM to OSM-feature hull (fetch=%s)", fetch_skadi)
    crop, w, s, e, n = _mosaic_from_skadi_cache(cover, cache_dir, fetch=fetch_skadi)
    _write_temp_dem(crop, w, s, e, n, hull_cache)
    return hull_cache, (
        "Mapzen Skadi mosaic cropped to OSM-feature convex hull "
        f"(written only to {hull_cache}, pipeline dems/ untouched)"
    )


def filter_by_ski(gdf: Optional[gpd.GeoDataFrame], cfg: GameExportConfig, polygon) -> Optional[gpd.GeoDataFrame]:
    if gdf is None or gdf.empty:
        return gdf
    out = gdf
    if "winter_sports_id" in gdf.columns:
        m = _id_match(gdf["winter_sports_id"], cfg.winter_sports_id)
        if m.any():
            out = gdf[m].copy()
    # Spatial clip as additional filter / when statewide lifts/pistes lack ids
    try:
        buf = polygon.buffer(0.02)  # ~2 km in degrees, coarse prefilter
        out = out[out.geometry.intersects(buf)].copy()
    except Exception as exc:
        log.warning("Spatial prefilter skipped: %s", exc)
    return out


def _fetch_s3_parquet(
    s3,
    bucket: str,
    cache_dir: Path,
    cfg: GameExportConfig,
    name: str,
    *,
    allow_combined: bool,
) -> Optional[Path]:
    return fetch_first_s3_object(
        s3,
        bucket,
        parquet_key_candidates(cfg.region, name, allow_combined=allow_combined),
        cache_dir,
    )


def find_ski_area_s3(s3, bucket: str, cache_dir: Path, cfg: GameExportConfig) -> tuple[gpd.GeoSeries, Path]:
    regional = None
    try:
        regional = _fetch_s3_parquet(
            s3, bucket, cache_dir, cfg, "ski_areas.parquet", allow_combined=False
        )
    except PermissionError as exc:
        log.warning("%s", exc)
    last_detail = ""
    if regional is not None:
        row = _ski_row_from_parquet(regional, cfg)
        if row is not None:
            return row, regional.parent
        last_detail = f"id {cfg.winter_sports_id} not in {regional} (or no polygon)"
    combined = fetch_first_s3_object(s3, bucket, ["combined/ski_areas.parquet"], cache_dir)
    if combined is not None:
        row = _ski_row_from_parquet(combined, cfg)
        if row is not None:
            log.info("Using ski area from combined S3 parquet %s", combined)
            return row, combined.parent
        last_detail = f"id {cfg.winter_sports_id} not in {combined} (or no polygon)"
    if regional is None and combined is None:
        raise FileNotFoundError(
            f"ski_areas.parquet not found on s3://{bucket}/{cfg.region}/ "
            f"(or combined/). {_s3_missing_hint(bucket, cfg.region, 'ski_areas.parquet')}"
        )
    raise RuntimeError(
        f"Ski area winter_sports_id={cfg.winter_sports_id} not found. {last_detail}"
    )


def _s3_missing_hint(bucket: str, region: str, name: str) -> str:
    return (
        f"Expected s3://{bucket}/{region}/{name}. "
        "Need IAM s3:GetObject (regional prefixes are not public)."
    )


def resolve_inputs(
    data_root: Path,
    cfg: GameExportConfig,
    cache_dir: Path,
    fetch_skadi: bool,
    *,
    from_s3: bool = False,
    s3_bucket: Optional[str] = None,
    s3=None,
) -> ResolvedInputs:
    warnings: list[str] = []
    bucket = s3_bucket or DEFAULT_BUCKET
    client = None
    if from_s3:
        client = s3 or make_s3_client()
        row, region_dir = find_ski_area_s3(client, bucket, cache_dir, cfg)
        # Nearby OSM is often missing from elevation-only regional prefixes (~500 MB combined).
        osm_path = _fetch_s3_parquet(
            client, bucket, cache_dir, cfg, "osm_near_winter_sports.parquet", allow_combined=True
        )
        pistes_path = _fetch_s3_parquet(
            client, bucket, cache_dir, cfg, "pistes.parquet", allow_combined=True
        )
        lifts_path = _fetch_s3_parquet(
            client, bucket, cache_dir, cfg, "lifts.parquet", allow_combined=True
        )
        elev_path = _fetch_s3_parquet(
            client,
            bucket,
            cache_dir,
            cfg,
            "ski_area_elevation_points.parquet",
            allow_combined=True,
        )
    else:
        row, region_dir = find_ski_area(data_root, cfg)
        osm_path = _find_parquet(data_root, cfg, "osm_near_winter_sports.parquet")
        pistes_path = _find_parquet(data_root, cfg, "pistes.parquet")
        lifts_path = _find_parquet(data_root, cfg, "lifts.parquet")
        elev_path = _find_parquet(data_root, cfg, "ski_area_elevation_points.parquet")

    polygon = row.geometry
    osm = _load_gdf(osm_path)
    pistes = _load_gdf(pistes_path)
    lifts = _load_gdf(lifts_path)
    elev_pts = _load_gdf(elev_path)
    if osm is None and pistes is None:
        hint = ""
        if from_s3:
            hint = " " + _s3_missing_hint(bucket, cfg.region, "pistes.parquet")
        raise FileNotFoundError(
            "Neither osm_near_winter_sports.parquet nor pistes.parquet found. "
            "Run the existing OSM extract pipeline for this region." + hint
        )
    osm = filter_by_ski(osm, cfg, polygon)
    pistes = filter_by_ski(pistes, cfg, polygon)
    lifts = filter_by_ski(lifts, cfg, polygon)
    if elev_pts is not None and "winter_sports_id" in elev_pts.columns:
        elev_pts = elev_pts[_id_match(elev_pts["winter_sports_id"], cfg.winter_sports_id)].copy()
    coverage = osm_feature_hull(osm, pistes, lifts)
    if coverage is None:
        coverage = polygon
        warnings.append("OSM feature hull empty; DEM crop fell back to ski-area polygon")
    else:
        log.info("DEM coverage: OSM-feature convex hull (not winter_sports AOI)")
    dem_path, dem_note = resolve_dem(
        data_root,
        cfg,
        polygon,
        cache_dir,
        fetch_skadi,
        s3=client,
        s3_bucket=bucket if from_s3 else None,
        coverage_geom=coverage,
    )
    if osm is None:
        warnings.append("osm_near_winter_sports.parquet missing; using lifts/pistes only")
    input_files = [p for p in (osm_path, pistes_path, lifts_path) if p is not None]
    return ResolvedInputs(
        ski_area=row,
        ski_polygon=polygon,
        region_dir=region_dir,
        osm_nearby=osm,
        pistes=pistes,
        lifts=lifts,
        elevation_points=elev_pts,
        dem_path=dem_path,
        dem_source_note=dem_note,
        warnings=warnings,
        input_files=input_files,
    )


def file_fingerprint(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.name.encode())
    etag = read_cached_etag(path)
    if etag:
        h.update(etag.encode())
        return h.hexdigest()[:16]
    h.update(str(path.stat().st_size).encode())
    h.update(str(int(path.stat().st_mtime)).encode())
    return h.hexdigest()[:16]


def scene_version(cfg: GameExportConfig, dem_path: Path, extra: list[Path]) -> str:
    h = hashlib.sha256()
    h.update(json.dumps(cfg.raw, sort_keys=True, default=str).encode())
    h.update(file_fingerprint(dem_path).encode())
    for p in extra:
        if p and p.is_file():
            h.update(file_fingerprint(p).encode())
    return "v0-" + h.hexdigest()[:12]
