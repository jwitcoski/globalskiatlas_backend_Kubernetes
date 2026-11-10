"""Mapterhorn Terrarium tiles as the elevation source.

Tiles are 512×512 WebP, XYZ, Terrarium RGB:

    elevation_m = R * 256 + G + B / 256 - 32768

https://tiles.mapterhorn.com/{z}/{x}/{y}.webp

Cloudflare rejects the default Python user agent, so every request sends one.
A cached ``.hgt`` from an older Skadi run is still read by the 1° helper; a
cache miss is filled from Mapterhorn, not from the AWS terrain bucket.
"""
from __future__ import annotations

import io
import logging
import math
from pathlib import Path
from typing import Optional
from urllib.request import Request, urlopen

import numpy as np

log = logging.getLogger("game_export")

TILE_URL = "https://tiles.mapterhorn.com/{z}/{x}/{y}.webp"
SOURCE_PAGE = "https://mapterhorn.com/data-access/"
ATTRIBUTION_URL = "https://mapterhorn.com/attribution"
PROVIDER = "Mapterhorn"
USER_AGENT = "GlobalSkiAtlas/1.0 (DEM mosaic; https://github.com/globalskiatlas)"
TILE_SIZE = 512
EARTH_CIRCUM_M = 40_075_016.68557849
HGT_NODATA = -32768
HGT_SIDE = 3601
# Resort / game meshes are 4–12 m. 5 m keeps a sample near each game vertex
# without pulling zoom 15 lidar tiles for every hill.
RESORT_TARGET_M = 5.0
RESORT_MAX_ZOOM = 14
RESORT_MAX_CELLS = 20_000_000
# 1° cells stay on the old 1-arc-second grid so contours and overview maps
# can keep mosaicking them. Source zoom follows ~30 m, not the resort zoom.
DEGREE_TARGET_M = 30.0
DEGREE_MAX_ZOOM = 12


def meters_per_pixel(lat_deg: float, zoom: int) -> float:
    cos = max(0.05, abs(math.cos(math.radians(lat_deg))))
    return EARTH_CIRCUM_M * cos / (TILE_SIZE * (2**zoom))


def choose_zoom(lat_deg: float, target_m: float, *, min_zoom: int = 0, max_zoom: int = 14) -> int:
    """Smallest zoom whose pixel is at or under ``target_m`` (then clamped)."""
    cos = max(0.05, abs(math.cos(math.radians(lat_deg))))
    ratio = (EARTH_CIRCUM_M * cos) / (TILE_SIZE * max(float(target_m), 0.5))
    zoom = int(math.ceil(math.log2(max(ratio, 1.0)) - 1e-9))
    return int(min(max_zoom, max(min_zoom, zoom)))


def decode_terrarium(data: bytes) -> np.ndarray:
    from PIL import Image

    image = Image.open(io.BytesIO(data))
    rgb = np.asarray(image)
    if rgb.ndim != 3 or rgb.shape[2] < 3:
        raise ValueError(f"expected RGB Terrarium tile, got shape {rgb.shape}")
    red = rgb[:, :, 0].astype(np.float32)
    green = rgb[:, :, 1].astype(np.float32)
    blue = rgb[:, :, 2].astype(np.float32)
    return (red * 256.0 + green + blue / 256.0) - 32768.0


def _tile_lonlat_bounds(zoom: int, x: int, y: int) -> tuple[float, float, float, float]:
    n = 2**zoom
    lon_w = x / n * 360.0 - 180.0
    lon_e = (x + 1) / n * 360.0 - 180.0
    lat_n = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_s = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lon_w, lat_s, lon_e, lat_n


def lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    lat = min(85.05112878, max(-85.05112878, lat))
    n = 2**zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    x = min(n - 1, max(0, x))
    y = min(n - 1, max(0, y))
    return x, y


def fetch_tile(
    zoom: int,
    x: int,
    y: int,
    cache_dir: Path,
    *,
    timeout: int = 60,
    log_errors: bool = True,
) -> Optional[np.ndarray]:
    """Return one decoded elevation tile, or None if the request fails."""
    cache_path = cache_dir / "mapterhorn" / str(zoom) / str(x) / f"{y}.webp"
    data: Optional[bytes] = None
    if cache_path.is_file():
        try:
            data = cache_path.read_bytes()
        except OSError:
            data = None
    if data is None:
        url = TILE_URL.format(z=zoom, x=x, y=y)
        request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "image/webp"})
        try:
            with urlopen(request, timeout=timeout) as resp:
                data = resp.read()
        except Exception as exc:
            if log_errors:
                log.warning("Mapterhorn tile %s/%s/%s failed: %s", zoom, x, y, exc)
            return None
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(data)
        except OSError as exc:
            log.warning("Mapterhorn cache write failed for %s: %s", cache_path, exc)
    try:
        elev = decode_terrarium(data)
    except Exception as exc:
        log.warning("Mapterhorn decode %s/%s/%s failed: %s", zoom, x, y, exc)
        cache_path.unlink(missing_ok=True)
        return None
    elev[elev <= (HGT_NODATA + 1)] = np.nan
    return elev


def _tiles_covering(west: float, south: float, east: float, north: float, zoom: int) -> list[tuple[int, int]]:
    x0, y_north = lonlat_to_tile(west, north, zoom)
    x1, y_south = lonlat_to_tile(east, south, zoom)
    if east < west:
        n = 2**zoom
        xs = list(range(x0, n)) + list(range(0, x1 + 1))
    else:
        xs = list(range(min(x0, x1), max(x0, x1) + 1))
    ys = range(min(y_north, y_south), max(y_north, y_south) + 1)
    return [(x, y) for x in xs for y in ys]


def resolve_tile(
    zoom: int,
    x: int,
    y: int,
    cache_dir: Path,
    *,
    min_zoom: int = 0,
) -> Optional[tuple[np.ndarray, int, int, int]]:
    """Return the requested tile, or the nearest coarser parent that exists.

    High zooms are regional. A 404 at z14 is normal outside those extracts.
    """
    z, tx, ty = zoom, x, y
    while z >= min_zoom:
        arr = fetch_tile(z, tx, ty, cache_dir, log_errors=False)
        if arr is not None:
            return arr, z, tx, ty
        z -= 1
        tx //= 2
        ty //= 2
    log.warning("Mapterhorn tile %s/%s/%s and its parents are missing", zoom, x, y)
    return None


def mosaic_bbox(
    west: float,
    south: float,
    east: float,
    north: float,
    cache_dir: Path,
    *,
    target_m: float,
    max_cells: int = RESORT_MAX_CELLS,
    max_zoom: int = RESORT_MAX_ZOOM,
    min_zoom: int = 0,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> tuple[np.ndarray, float, float, float, float]:
    """Sample Mapterhorn onto a WGS84 grid covering the bbox.

    Row 0 is north. Nodata is NaN. Returns (array, west, south, east, north).
    """
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds
    from rasterio.warp import Resampling, reproject

    if east < west or north < south:
        raise ValueError(f"empty bbox {west},{south},{east},{north}")
    clat = (south + north) / 2.0
    lon_m = max(1.0, 111_320.0 * math.cos(math.radians(clat)))
    lat_m = 111_320.0
    res_m = max(float(target_m), 0.5)
    if width is None or height is None:
        res_deg_x = res_m / lon_m
        res_deg_y = res_m / lat_m
        width = max(2, int(math.ceil((east - west) / res_deg_x)))
        height = max(2, int(math.ceil((north - south) / res_deg_y)))
        while width * height > max_cells:
            res_deg_x *= 1.25
            res_deg_y *= 1.25
            width = max(2, int(math.ceil((east - west) / res_deg_x)))
            height = max(2, int(math.ceil((north - south) / res_deg_y)))
        res_m = float(res_deg_x * lon_m)
    zoom = choose_zoom(clat, res_m, min_zoom=min_zoom, max_zoom=max_zoom)
    tiles = _tiles_covering(west, south, east, north, zoom)
    if len(tiles) > 800:
        raise RuntimeError(
            f"Mapterhorn mosaic would fetch {len(tiles)} tiles at z={zoom} "
            f"for bbox {south:.3f},{west:.3f},{north:.3f},{east:.3f}"
        )
    log.info(
        "Mapterhorn mosaic z=%s %s×%s cells (~%.0fm) %s tiles bbox=[%.4f,%.4f,%.4f,%.4f]",
        zoom,
        width,
        height,
        res_m,
        len(tiles),
        west,
        south,
        east,
        north,
    )
    dst = np.full((height, width), np.nan, dtype=np.float32)
    dst_transform = from_bounds(west, south, east, north, width, height)
    dst_crs = CRS.from_epsg(4326)
    got = 0
    used: set[tuple[int, int, int]] = set()
    for x, y in tiles:
        resolved = resolve_tile(zoom, x, y, cache_dir, min_zoom=min_zoom)
        if resolved is None:
            continue
        arr, src_z, src_x, src_y = resolved
        if (src_z, src_x, src_y) in used:
            continue
        used.add((src_z, src_x, src_y))
        if src_z != zoom:
            log.info("Mapterhorn z=%s missing here; using z=%s/%s/%s", zoom, src_z, src_x, src_y)
        lon_w, lat_s, lon_e, lat_n = _tile_lonlat_bounds(src_z, src_x, src_y)
        src_transform = from_bounds(lon_w, lat_s, lon_e, lat_n, arr.shape[1], arr.shape[0])
        tmp = np.full((height, width), np.nan, dtype=np.float32)
        reproject(
            source=arr.astype(np.float32, copy=False),
            destination=tmp,
            src_transform=src_transform,
            src_crs=dst_crs,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            resampling=Resampling.bilinear,
            src_nodata=np.nan,
            dst_nodata=np.nan,
        )
        hit = np.isfinite(tmp)
        dst[hit] = tmp[hit]
        got += 1
        del arr, tmp
    if got == 0 or not np.any(np.isfinite(dst)):
        raise RuntimeError(f"No Mapterhorn tiles for bbox {south},{west},{north},{east}")
    log.info("Mapterhorn mosaic used %s/%s tiles at z=%s", got, len(tiles), zoom)
    return dst, float(west), float(south), float(east), float(north)


def elevations_to_hgt(arr: np.ndarray) -> np.ndarray:
    """Quantize a float elevation grid to int16 with Skadi nodata (-32768)."""
    out = np.full(arr.shape, HGT_NODATA, dtype=np.int16)
    good = np.isfinite(arr) & (arr > -1000.0) & (arr < 10000.0)
    out[good] = np.clip(np.rint(arr[good]), -32767, 32767).astype(np.int16)
    return out


def sample_degree_cell(lat_sw: int, lon_sw: int, cache_dir: Path) -> Optional[np.ndarray]:
    """1°×1° elevation at 1 arc-second (3601×3601), sampled from Mapterhorn."""
    try:
        dem, _, _, _, _ = mosaic_bbox(
            float(lon_sw),
            float(lat_sw),
            float(lon_sw + 1),
            float(lat_sw + 1),
            cache_dir,
            target_m=DEGREE_TARGET_M,
            max_cells=HGT_SIDE * HGT_SIDE,
            max_zoom=DEGREE_MAX_ZOOM,
            width=HGT_SIDE,
            height=HGT_SIDE,
        )
    except Exception as exc:
        log.warning("Mapterhorn degree cell %s,%s failed: %s", lat_sw, lon_sw, exc)
        return None
    return elevations_to_hgt(dem)
