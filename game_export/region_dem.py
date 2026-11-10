"""Coarse DEM mosaics for wiki region clay (Mapterhorn Terrarium tiles)."""
from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Optional
import geopandas as gpd
import numpy as np
from shapely.geometry import box
from shapely.ops import transform as shp_transform, unary_union
from shapely.prepared import prep

log = logging.getLogger("game_export")

MAX_SKADI_TILES = 80
MAX_TERRARIUM_ZOOM = 7
MIN_TERRARIUM_ZOOM = 4


def unwrap_dateline_west(geom):
    """Shift 0–180°E fragments west so USA/Russia bounds are not globe-wide."""
    if geom is None or geom.is_empty:
        return geom
    minx, _miny, maxx, _maxy = geom.bounds
    if (maxx - minx) <= 180.0:
        return geom
    return shift_positive_lons_west(geom)


def shift_positive_lons_west(geom):
    if geom is None or geom.is_empty:
        return geom

    def _xy(x, y, z=None):
        return (x - 360.0 if x > 0 else x, y)

    return shp_transform(_xy, geom)


def unwrap_boundary_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    out["geometry"] = [unwrap_dateline_west(g) for g in out.geometry]
    return out


def country_crs_from_lonlat(lon: float, lat: float) -> str:
    """Lambert azimuthal equal-area centered on the country (not a single UTM zone)."""
    return (
        f"+proj=laea +lat_0={lat:.8f} +lon_0={lon:.8f} "
        "+x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
    )


def _tile_lonlat_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    n = 2**z
    lon_w = x / n * 360.0 - 180.0
    lon_e = (x + 1) / n * 360.0 - 180.0
    lat_n = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_s = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lon_w, lat_s, lon_e, lat_n


def _lonlat_to_tile(lon: float, lat: float, z: int) -> tuple[int, int]:
    lat = min(85.05112878, max(-85.05112878, lat))
    n = 2**z
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    x = min(n - 1, max(0, x))
    y = min(n - 1, max(0, y))
    return x, y


def _wrap_lon(lon: float) -> float:
    while lon < -180.0:
        lon += 360.0
    while lon > 180.0:
        lon -= 360.0
    return lon


def _choose_terrarium_zoom(res_m: float) -> int:
    # Mapterhorn tiles are 512 px, so the same z is twice as fine as the old 256 px AWS tiles.
    from game_export.mapterhorn import choose_zoom

    return choose_zoom(0.0, max(res_m, 500.0), min_zoom=MIN_TERRARIUM_ZOOM, max_zoom=MAX_TERRARIUM_ZOOM)


def _fetch_terrarium_tile(z: int, x: int, y: int, cache_dir: Path) -> Optional[np.ndarray]:
    from game_export.mapterhorn import fetch_tile

    return fetch_tile(z, x, y, cache_dir)


def purge_region_dem_scratch(cache_dir: Path) -> None:
    """Delete DEM download scratch so overnight country bakes do not fill the disk."""
    import shutil

    for name in ("terrarium", "skadi", "mapterhorn"):
        root = cache_dir / name
        if root.is_dir():
            shutil.rmtree(root, ignore_errors=True)
            log.info("Removed DEM scratch %s", root)


def mosaic_terrarium_for_boundary(
    boundary_wgs: gpd.GeoDataFrame,
    cache_dir: Path,
    target_res_m: float,
    pad_deg: float = 0.2,
    max_cells: int = 200_000,
):
    """Resample Mapterhorn Terrarium tiles onto a coarse WGS84 grid (country scale)."""
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds
    from rasterio.warp import Resampling, reproject

    geom = unwrap_dateline_west(unary_union(list(boundary_wgs.geometry)))
    minx, miny, maxx, maxy = geom.bounds
    minx -= pad_deg
    miny -= pad_deg
    maxx += pad_deg
    maxy += pad_deg
    clat = (miny + maxy) / 2.0
    lat_m = 111_320.0
    lon_m = max(1.0, 111_320.0 * math.cos(math.radians(clat)))
    res_m = max(float(target_res_m), 2000.0)
    res_deg_x = res_m / lon_m
    res_deg_y = res_m / lat_m
    width = max(2, int(math.ceil((maxx - minx) / res_deg_x)))
    height = max(2, int(math.ceil((maxy - miny) / res_deg_y)))
    while width * height > max_cells:
        res_deg_x *= 1.25
        res_deg_y *= 1.25
        width = max(2, int(math.ceil((maxx - minx) / res_deg_x)))
        height = max(2, int(math.ceil((maxy - miny) / res_deg_y)))
    cell_m = float(res_deg_x * lon_m)
    z = _choose_terrarium_zoom(min(cell_m, 8000.0))
    z = max(z, 6)
    log.info(
        "Mapterhorn mosaic z=%s %s×%s cells (~%.0fm) bbox=[%.2f,%.2f,%.2f,%.2f]",
        z,
        width,
        height,
        cell_m,
        minx,
        miny,
        maxx,
        maxy,
    )

    dst = np.full((height, width), np.nan, dtype=np.float32)
    dst_transform = from_bounds(minx, miny, maxx, maxy, width, height)
    dst_crs = CRS.from_epsg(4326)
    prepared = prep(geom.buffer(0.15))

    corners = [
        (_wrap_lon(minx), miny),
        (_wrap_lon(maxx), miny),
        (_wrap_lon(minx), maxy),
        (_wrap_lon(maxx), maxy),
        (_wrap_lon((minx + maxx) / 2), clat),
    ]
    xs, ys = zip(*(_lonlat_to_tile(lon, lat, z) for lon, lat in corners))
    n = 2**z
    x0, x1 = min(xs), max(xs)
    y0, y1 = min(ys), max(ys)
    # Dateline: also walk tiles covering unwrapped longitudes < -180.
    tiles: list[tuple[int, int]] = []
    if minx < -180:
        x_west, _ = _lonlat_to_tile(_wrap_lon(minx), clat, z)
        x_east, _ = _lonlat_to_tile(180.0 - 1e-6, clat, z)
        for x in list(range(x_west, n)) + list(range(0, x1 + 1)):
            for y in range(y0, y1 + 1):
                tiles.append((x, y))
    else:
        for x in range(x0, x1 + 1):
            for y in range(y0, y1 + 1):
                tiles.append((x, y))
    tiles = sorted(set(tiles))

    got = 0
    for x, y in tiles:
        lon_w, lat_s, lon_e, lat_n = _tile_lonlat_bounds(z, x, y)
        tile_geom = box(lon_w, lat_s, lon_e, lat_n)
        tile_unwrapped = unwrap_dateline_west(tile_geom)
        # Also test the western copy for 170E–180E tiles.
        extra = None
        if lon_w > 0:
            extra = box(lon_w - 360.0, lat_s, lon_e - 360.0, lat_n)
        if not prepared.intersects(tile_unwrapped) and not (extra is not None and prepared.intersects(extra)):
            continue
        arr = _fetch_terrarium_tile(z, x, y, cache_dir)
        if arr is None:
            continue
        try:
            side = int(arr.shape[0])
            src_transform = from_bounds(lon_w, lat_s, lon_e, lat_n, side, side)
            tmp = np.full((height, width), np.nan, dtype=np.float32)
            reproject(
                source=arr.astype(np.float32, copy=False),
                destination=tmp,
                src_transform=src_transform,
                src_crs=dst_crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                resampling=Resampling.average,
                src_nodata=None,
                dst_nodata=np.nan,
            )
            hit = np.isfinite(tmp)
            dst[hit] = tmp[hit]
            del tmp
            if extra is not None:
                src_transform_w = from_bounds(lon_w - 360.0, lat_s, lon_e - 360.0, lat_n, side, side)
                tmp2 = np.full((height, width), np.nan, dtype=np.float32)
                reproject(
                    source=arr.astype(np.float32, copy=False),
                    destination=tmp2,
                    src_transform=src_transform_w,
                    src_crs=dst_crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.average,
                    src_nodata=None,
                    dst_nodata=np.nan,
                )
                hit2 = np.isfinite(tmp2)
                dst[hit2] = tmp2[hit2]
                del tmp2
            got += 1
        finally:
            del arr

    if got == 0 or not np.any(np.isfinite(dst)):
        raise RuntimeError(f"No Mapterhorn tiles for bbox {miny},{minx},{maxy},{maxx}")
    log.info("Mapterhorn mosaic used %s land-intersecting tiles at z=%s", got, z)
    return dst, float(minx), float(miny), float(maxx), float(maxy), np.nan
