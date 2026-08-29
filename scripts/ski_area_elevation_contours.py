#!/usr/bin/env python3
"""
Compute elevation min/max and contour lines per ski area using Mapzen Skadi DEM on AWS S3.

Reads output/combined/ski_areas.parquet (polygon geometry), fetches only the 1° Skadi tiles
that intersect each area, caches tiles locally, and writes:
  - output/combined/ski_areas_elevation.parquet (winter_sports_id, region, elevation_low_m, elevation_high_m, ski_north_angle)
  - output/combined/ski_area_contours.geojson (contour lines with elevation_m and ski area id)
  - output/combined/ski_area_elevation_points.geojson (base + summit points per ski area)
  - when output/combined/ski_areas_analyzed.parquet exists: elevation and ski_north_angle columns are merged into it
  - optionally: output/combined/dems/<region>/<winter_sports_id>.tif (cropped DEM per ski area)

Usage:
  python scripts/ski_area_elevation_contours.py -i output/combined/ski_areas_analyzed.parquet -o output/combined --save-dem
  python scripts/ski_area_elevation_contours.py -i output/combined/ski_areas.parquet --limit 5

Input can be ski_areas.parquet (polygon) or ski_areas_analyzed.parquet (centroid + buffer). When --boundaries
points to ski_areas.parquet with polygons, contours extend to 1000 ft beyond resort boundary; base/summit points
stay inside the resort polygon.
"""

import argparse
import gc
import gzip
import json
import math
import os
import shutil
import sys
from pathlib import Path
from typing import Any, List, Optional, Tuple
from urllib.request import urlopen

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio import features
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from shapely.geometry import LineString, Point

# Mapzen Skadi: 1°×1° tiles, SRTM-style HGT (1 arc-second = 3601×3601)
SKADI_BASE = "https://elevation-tiles-prod.s3.amazonaws.com/skadi"
HGT_NO_DATA = -32768
# 1 arc-second in degrees
DEG_PER_PIXEL_1AS = 1.0 / 3600.0
# Buffer around ski area polygon for contour extent (m). 305 m ≈ 1000 ft, matches extract_nearby_from_pbf
CONTOUR_BUFFER_METERS = 305


def _skadi_tile_name(lat_sw: int, lon_sw: int) -> str:
    """Return Skadi path segment for tile with SW corner (lat_sw, lon_sw). E.g. N45E010, S31W070."""
    ns = "N" if lat_sw >= 0 else "S"
    ew = "E" if lon_sw >= 0 else "W"
    return f"{ns}{abs(lat_sw)}{ew}{abs(lon_sw):03d}"


def tiles_for_bbox(minlat: float, minlon: float, maxlat: float, maxlon: float) -> List[Tuple[int, int]]:
    """Return list of (lat_sw, lon_sw) for 1° tiles that intersect the bbox."""
    lat_lo = int(math.floor(minlat))
    lat_hi = int(math.ceil(maxlat))
    lon_lo = int(math.floor(minlon))
    lon_hi = int(math.ceil(maxlon))
    out = []
    for lat in range(lat_lo, lat_hi):
        for lon in range(lon_lo, lon_hi):
            out.append((lat, lon))
    return out


def fetch_skadi_tile(
    lat_sw: int,
    lon_sw: int,
    cache_dir: Path,
    base_url: str = SKADI_BASE,
    timeout: int = 60,
) -> Optional[np.ndarray]:
    """
    Fetch one Skadi tile, decompress, return 2D array (row = north->south, col = west->east).
    Caches to cache_dir. Returns None on missing/failure.
    """
    name = _skadi_tile_name(lat_sw, lon_sw)
    ns = "N" if lat_sw >= 0 else "S"
    subdir = f"{ns}{abs(lat_sw)}"
    cache_path = cache_dir / "skadi" / subdir / f"{name}.hgt"
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists():
        try:
            return _read_hgt_file(cache_path, lat_sw, lon_sw)
        except Exception:
            cache_path.unlink(missing_ok=True)

    url = f"{base_url}/{subdir}/{name}.hgt.gz"
    try:
        with urlopen(url, timeout=timeout) as resp:
            data = gzip.decompress(resp.read())
    except Exception as e:
        print(f"  Warning: could not fetch {url}: {e}", file=sys.stderr)
        return None

    # Save uncompressed for next time (skip if disk full so run can continue)
    try:
        cache_path.write_bytes(data)
    except OSError as e:
        if e.errno == 28:  # No space left on device
            print(f"  Warning: disk full, skipping cache write for {name}", file=sys.stderr)
        else:
            raise
    return _read_hgt_array(data, lat_sw, lon_sw)


def _read_hgt_file(path: Path, lat_sw: int, lon_sw: int) -> np.ndarray:
    """Read uncompressed HGT file from path."""
    data = path.read_bytes()
    return _read_hgt_array(data, lat_sw, lon_sw)


def _read_hgt_array(data: bytes, lat_sw: int, lon_sw: int) -> np.ndarray:
    """Parse HGT bytes (big-endian int16). Return 2D array; row 0 = north, col 0 = west."""
    n = len(data) // 2
    side = int(math.isqrt(n))
    if side * side != n:
        raise ValueError(f"Unexpected HGT size {n}")
    arr = np.frombuffer(data, dtype=">i2").reshape(side, side)
    return arr


def _mosaic_tiles(
    tiles: List[Tuple[int, int, np.ndarray]],
) -> Tuple[np.ndarray, float, float, float, float]:
    """
    Mosaic tiles (list of (lat_sw, lon_sw, array)) into one array and return
    (array, west, south, east, north) in degrees. Row 0 = north, col 0 = west.
    """
    if not tiles:
        raise ValueError("No tiles to mosaic")
    min_lat_sw = min(t[0] for t in tiles)
    max_lat_sw = max(t[0] for t in tiles)
    min_lon_sw = min(t[1] for t in tiles)
    max_lon_sw = max(t[1] for t in tiles)
    side = tiles[0][2].shape[0]
    # Merged bounds in degrees (SW corner naming)
    west = float(min_lon_sw)
    south = float(min_lat_sw)
    east = float(max_lon_sw + 1)
    north = float(max_lat_sw + 1)
    nrows = (max_lat_sw - min_lat_sw + 1) * (side - 1) + 1
    ncols = (max_lon_sw - min_lon_sw + 1) * (side - 1) + 1
    if side < 2 or nrows <= 0 or ncols <= 0:
        raise ValueError(
            f"Invalid mosaic dimensions: side={side}, nrows={nrows}, ncols={ncols} "
            f"(lat {min_lat_sw}..{max_lat_sw}, lon {min_lon_sw}..{max_lon_sw})"
        )
    out = np.full((nrows, ncols), HGT_NO_DATA, dtype=np.int32)
    for lat_sw, lon_sw, arr in tiles:
        r0 = (max_lat_sw - lat_sw) * (side - 1)
        c0 = (lon_sw - min_lon_sw) * (side - 1)
        h, w = arr.shape
        r1 = min(r0 + h, nrows)
        c1 = min(c0 + w, ncols)
        out[r0:r1, c0:c1] = arr[0 : r1 - r0, 0 : c1 - c0]
    return out, west, south, east, north


def _crop_dem_to_bbox(
    dem: np.ndarray,
    dem_west: float,
    dem_south: float,
    dem_east: float,
    dem_north: float,
    minlon: float,
    minlat: float,
    maxlon: float,
    maxlat: float,
) -> Tuple[np.ndarray, float, float, float, float]:
    """Crop DEM to bbox. Return (crop, west, south, east, north) in degrees. DEM row 0 = north."""
    h, w = dem.shape
    if w == 0 or h == 0:
        return dem, dem_west, dem_south, dem_east, dem_north
    res_x = (dem_east - dem_west) / w
    res_y = (dem_north - dem_south) / h
    # Pixel (r,c): lat = dem_north - r*res_y, lon = dem_west + c*res_x
    r0 = max(0, int(np.floor((dem_north - maxlat) / res_y)))
    r1 = min(h, int(np.ceil((dem_north - minlat) / res_y)) + 1)
    c0 = max(0, int(np.floor((minlon - dem_west) / res_x)))
    c1 = min(w, int(np.ceil((maxlon - dem_west) / res_x)) + 1)
    if r0 >= r1 or c0 >= c1:
        return dem, dem_west, dem_south, dem_east, dem_north
    crop = dem[r0:r1, c0:c1].copy()
    cwest = dem_west + c0 * res_x
    ceast = dem_west + c1 * res_x
    cnorth = dem_north - r0 * res_y
    csouth = dem_north - r1 * res_y
    return crop, cwest, csouth, ceast, cnorth


def _min_max_elevation(
    dem: np.ndarray,
    mask: Optional[np.ndarray] = None,
) -> Tuple[Optional[float], Optional[float]]:
    """Return (min, max) elevation in meters, ignoring no-data. If mask is set, only consider masked pixels (True = valid)."""
    if mask is not None:
        valid = (dem != HGT_NO_DATA) & mask
    else:
        valid = dem != HGT_NO_DATA
    if not np.any(valid):
        return None, None
    vals = dem[valid]
    return float(np.min(vals)), float(np.max(vals))


def _min_max_elevation_with_locations(
    dem: np.ndarray,
    mask: Optional[np.ndarray] = None,
) -> Tuple[
    Optional[float],
    Optional[float],
    Optional[Tuple[int, int]],
    Optional[Tuple[int, int]],
]:
    """Return (min, max, (row_min, col_min), (row_max, col_max)). Pixels are in DEM grid (row 0=north, col 0=west)."""
    if mask is not None:
        valid = (dem != HGT_NO_DATA) & mask
    else:
        valid = dem != HGT_NO_DATA
    if not np.any(valid):
        return None, None, None, None
    flat_valid = np.where(valid)
    vals = dem[flat_valid]
    idx_min = np.argmin(vals)
    idx_max = np.argmax(vals)
    h, w = dem.shape
    row_min = int(flat_valid[0][idx_min])
    col_min = int(flat_valid[1][idx_min])
    row_max = int(flat_valid[0][idx_max])
    col_max = int(flat_valid[1][idx_max])
    return (
        float(np.min(vals)),
        float(np.max(vals)),
        (row_min, col_min),
        (row_max, col_max),
    )


def _pixel_to_latlon(
    row: int,
    col: int,
    h: int,
    w: int,
    west: float,
    south: float,
    east: float,
    north: float,
) -> Tuple[float, float]:
    """Convert DEM pixel (row, col) to (lon, lat). Row 0 = north, col 0 = west. Uses pixel center."""
    res_x = (east - west) / w
    res_y = (north - south) / h
    lat = north - (row + 0.5) * res_y
    lon = west + (col + 0.5) * res_x
    return lon, lat


def _bearing_deg(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Bearing from point 1 to 2 in degrees 0-360 (0=North, 90=East)."""
    dlon = math.radians(lon2 - lon1)
    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2_r)
    y = math.cos(lat1_r) * math.sin(lat2_r) - math.sin(lat1_r) * math.cos(lat2_r) * math.cos(dlon)
    bearing = math.degrees(math.atan2(x, y))
    return (bearing + 360) % 360


def _write_dem_geotiff(
    dem: np.ndarray,
    west: float,
    south: float,
    east: float,
    north: float,
    path: Path,
    nodata: int = HGT_NO_DATA,
) -> None:
    """Write cropped DEM as compressed GeoTIFF (int16, EPSG:4326)."""
    h, w = dem.shape
    transform = from_bounds(west, south, east, north, w, h)
    profile = {
        "driver": "GTiff",
        "width": w,
        "height": h,
        "count": 1,
        "dtype": "int16",
        "crs": CRS.from_epsg(4326),
        "transform": transform,
        "nodata": nodata,
        "compress": "deflate",
        "tiled": True,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    data = dem.astype(np.int16)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)


def _rasterize_polygon_mask(
    geom: Any,
    dem_shape: Tuple[int, int],
    west: float,
    south: float,
    east: float,
    north: float,
) -> Optional[np.ndarray]:
    """Rasterize polygon to boolean mask (True = inside) in DEM grid. Returns None if geom is point/empty."""
    if geom is None or geom.is_empty:
        return None
    if isinstance(geom, Point):
        return None
    h, w = dem_shape
    transform = from_bounds(west, south, east, north, w, h)
    if hasattr(geom, "__geo_interface__"):
        shapes = [geom.__geo_interface__]
    else:
        shapes = [geom]
    mask = features.geometry_mask(
        shapes,
        out_shape=(h, w),
        transform=transform,
        invert=True,
    )
    return mask


def _buffer_geom_meters(geom: Any, meters: float) -> Any:
    """Buffer geometry by meters (uses local UTM projection if pyproj available)."""
    try:
        from shapely.ops import transform
        import pyproj
        centroid = geom.centroid
        lon, lat = centroid.x, centroid.y
        utm_zone = int((lon + 180) / 6) + 1
        hem = "north" if lat >= 0 else "south"
        crs = f"+proj=utm +zone={utm_zone} +{hem} +datum=WGS84 +units=m"
        proj = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        inv = pyproj.Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
        geom_proj = transform(proj.transform, geom)
        buffered = geom_proj.buffer(meters)
        return transform(inv.transform, buffered)
    except ImportError:
        deg = meters / 111320.0
        return geom.buffer(deg)


def _contours_from_dem(
    dem: np.ndarray,
    west: float,
    south: float,
    east: float,
    north: float,
    interval: float = 25.0,
    mask: Optional[np.ndarray] = None,
) -> List[Tuple[float, List[Tuple[float, float]]]]:
    """
    Generate contour lines from DEM. Returns list of (elevation_m, [(lon, lat), ...]) per segment.
    Uses matplotlib contour; segments are in (lon, lat) order.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return []
    h, w = dem.shape
    # Mask no-data so contour doesn't interpolate across
    dem_m = dem.astype(np.float64)
    dem_m[dem == HGT_NO_DATA] = np.nan
    if mask is not None:
        dem_m[~mask] = np.nan
    valid = np.isfinite(dem_m)
    if not np.any(valid):
        return []
    vmin, vmax = float(np.nanmin(dem_m)), float(np.nanmax(dem_m))
    if vmax <= vmin:
        return []
    x = np.linspace(west, east, w)
    y = np.linspace(north, south, h)  # y north->south to match row order
    # Anchor contour levels to interval boundaries (from 0), not local vmin.
    # This ensures predictable "major" contours like x00 meters exist and
    # can be targeted by symbology rules such as elevation_m % 100 = 0.
    level_start = math.floor(vmin / interval) * interval
    level_end = math.ceil(vmax / interval) * interval
    levels = np.arange(level_start, level_end + interval * 0.5, interval)
    if len(levels) == 0:
        return []
    cs = plt.contour(x, y, dem_m, levels=levels)
    plt.close()
    segments = []
    # Use allsegs (level -> list of (n,2) arrays) to avoid deprecated .collections
    for level_idx, level in enumerate(cs.levels):
        if level_idx >= len(cs.allsegs):
            break
        for seg in cs.allsegs[level_idx]:
            if len(seg) < 2:
                continue
            coords = [(float(p[0]), float(p[1])) for p in seg]
            if len(coords) > 2 and coords[0] == coords[-1]:
                coords = coords[:-1]
            if len(coords) < 2:
                continue
            segments.append((float(level), coords))
    return segments


_ID_FALLBACK_KEYS = (
    "winter_sports_id",
    "osm_id",
    "id",
    "osm_way_id",
    "osm_relation_id",
)


def _first_non_na_id(row: gpd.GeoSeries) -> Any:
    """First usable ski/OSM id from a row. Skips NaN (Python's ``nan or x`` wrongly stops at nan)."""
    for k in _ID_FALLBACK_KEYS:
        v = row.get(k)
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except TypeError:
            pass
        return v
    return None


def _ski_area_id(row: gpd.GeoSeries) -> Tuple[Any, Any]:
    """Stable id for join: (winter_sports_id or osm_id / id / osm_way_id / osm_relation_id, region)."""
    ws_id = _first_non_na_id(row)
    region = row.get("region", "")
    return ws_id, region


def process_ski_area(
    row: gpd.GeoSeries,
    cache_dir: Path,
    contour_interval: float = 25.0,
    buffer_km: float = 5.0,
    dem_dir: Optional[Path] = None,
    boundaries_by_key: Optional[dict] = None,
) -> Tuple[Optional[float], Optional[float], List[dict], List[dict], Optional[float]]:
    """
    Process one ski area: fetch tiles, crop, compute min/max, generate contours.
    If dem_dir is set, save the cropped DEM as GeoTIFF under dem_dir/<region>/<winter_sports_id>.tif.
    boundaries_by_key: optional dict (winter_sports_id, region) -> polygon for clipping to resort boundary.
    Returns (elevation_low_m, elevation_high_m, contour features, elevation point features, ski_north_angle).
    """
    ws_id, region = _ski_area_id(row)
    if region is None or (isinstance(region, float) and pd.isna(region)) or not str(region).strip():
        region = os.environ.get("REGION", "") or ""
    region_str = str(region) if region is not None and not (hasattr(region, "__float__") and pd.isna(region)) else ""
    wi = ws_id
    if wi is not None and pd.notna(wi):
        try:
            wi = int(wi)
        except (ValueError, TypeError):
            pass
    key = (wi, region_str)

    # Use boundary polygon - required for DEM cut, contours, base/summit (all must stay inside resort)
    geom = None
    if boundaries_by_key:
        geom = boundaries_by_key.get(key) or boundaries_by_key.get((str(wi), region_str))
    if geom is None or geom.is_empty:
        geom = row.geometry
    if geom is None or geom.is_empty:
        return None, None, [], [], None
    # Centroid-only input: MUST have boundary in ski_areas.parquet - skip if not
    if isinstance(geom, Point):
        return None, None, [], [], None

    # Contours extend 1000 ft beyond polygon; use buffered bounds for DEM coverage
    geom_buffered = _buffer_geom_meters(geom, CONTOUR_BUFFER_METERS)
    bounds = geom_buffered.bounds
    minlon, minlat, maxlon, maxlat = bounds

    # Fetch tiles
    tile_list = tiles_for_bbox(minlat, minlon, maxlat, maxlon)
    tiles_data = []
    for lat_sw, lon_sw in tile_list:
        arr = fetch_skadi_tile(lat_sw, lon_sw, cache_dir)
        if arr is not None:
            tiles_data.append((lat_sw, lon_sw, arr))
    if not tiles_data:
        return None, None, [], [], None

    try:
        dem, dem_west, dem_south, dem_east, dem_north = _mosaic_tiles(tiles_data)
    except Exception as e:
        print(f"  Mosaic failed: {e}", file=sys.stderr)
        return None, None, [], [], None

    crop, cwest, csouth, ceast, cnorth = _crop_dem_to_bbox(
        dem, dem_west, dem_south, dem_east, dem_north,
        minlon, minlat, maxlon, maxlat,
    )

    if crop.size == 0:
        return None, None, [], [], None

    # Cut DEM to ski resort boundary first - only pixels inside polygon count for min/max
    try:
        poly_mask = _rasterize_polygon_mask(geom, crop.shape, cwest, csouth, ceast, cnorth)
    except Exception:
        poly_mask = None
    if poly_mask is None:
        return None, None, [], [], None

    # dem_cut: polygon only (min/max, base/summit)
    dem_cut = np.where(poly_mask, crop, HGT_NO_DATA)

    result = _min_max_elevation_with_locations(dem_cut, mask=None)
    elev_lo, elev_hi = result[0], result[1]
    rc_lo, rc_hi = result[2], result[3]

    # Contours: extend to polygon + 1000 ft buffer
    try:
        poly_mask_buffered = _rasterize_polygon_mask(
            geom_buffered, crop.shape, cwest, csouth, ceast, cnorth
        )
    except Exception:
        poly_mask_buffered = poly_mask
    dem_contour = np.where(poly_mask_buffered, crop, HGT_NO_DATA) if poly_mask_buffered is not None else dem_cut
    segments = _contours_from_dem(
        dem_contour, cwest, csouth, ceast, cnorth,
        interval=contour_interval,
        mask=None,
    )
    # Clip contour segments to buffered polygon so contours extend up to 1000 ft beyond resort
    segments_clipped: List[Tuple[float, List[Tuple[float, float]]]] = []
    for elev, coords in segments:
        if len(coords) < 2:
            continue
        line = LineString(coords)
        try:
            inter = geom_buffered.intersection(line)
        except Exception:
            inter = line
        if inter.is_empty:
            continue
        if hasattr(inter, "geoms"):
            for g in inter.geoms:
                if not g.is_empty and hasattr(g, "coords") and len(list(g.coords)) >= 2:
                    segments_clipped.append((elev, list(g.coords)))
        elif hasattr(inter, "coords") and not inter.is_empty:
            coords_out = list(inter.coords)
            if len(coords_out) >= 2:
                segments_clipped.append((elev, coords_out))
    segments = segments_clipped
    if dem_dir is not None:
        region_path = Path(str(region).strip()).parts if str(region).strip() else ("_",)
        dem_path = dem_dir.joinpath(*region_path, f"{ws_id}.tif")
        try:
            _write_dem_geotiff(dem_cut, cwest, csouth, ceast, cnorth, dem_path)
        except Exception as e:
            print(f"  Warning: could not write DEM {dem_path}: {e}", file=sys.stderr)

    name = row.get("name") or row.get("winter_sports_name") or ""
    features = []
    for elev, coords in segments:
        if len(coords) < 2:
            continue
        line = LineString(coords)
        if line.is_empty or not line.is_valid:
            continue
        features.append({
            "type": "Feature",
            "geometry": line.__geo_interface__,
            "properties": {
                "elevation_m": round(elev, 1),
                "winter_sports_id": ws_id,
                "region": str(region),
                "name": str(name),
            },
        })

    # Elevation points (base + summit) and ski_north_angle
    point_features: List[dict] = []
    ski_north_angle: Optional[float] = None
    if rc_lo is not None and rc_hi is not None and elev_lo is not None and elev_hi is not None:
        h, w = crop.shape
        lon_lo, lat_lo = _pixel_to_latlon(
            rc_lo[0], rc_lo[1], h, w, cwest, csouth, ceast, cnorth
        )
        lon_hi, lat_hi = _pixel_to_latlon(
            rc_hi[0], rc_hi[1], h, w, cwest, csouth, ceast, cnorth
        )
        point_features.append({
            "type": "Feature",
            "geometry": Point(lon_lo, lat_lo).__geo_interface__,
            "properties": {
                "elevation_m": round(elev_lo, 1),
                "point_type": "base",
                "winter_sports_id": ws_id,
                "region": str(region),
                "name": str(name),
            },
        })
        point_features.append({
            "type": "Feature",
            "geometry": Point(lon_hi, lat_hi).__geo_interface__,
            "properties": {
                "elevation_m": round(elev_hi, 1),
                "point_type": "summit",
                "winter_sports_id": ws_id,
                "region": str(region),
                "name": str(name),
            },
        })
        # ski_north_angle: bearing base->summit (0=North, 90=East); use as map bearing so summit is at top
        if (lon_lo, lat_lo) != (lon_hi, lat_hi):
            bearing = _bearing_deg(lon_lo, lat_lo, lon_hi, lat_hi)
            ski_north_angle = round(bearing, 1)

    return elev_lo, elev_hi, features, point_features, ski_north_angle


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute elevation min/max and contours per ski area from Mapzen Skadi DEM",
    )
    parser.add_argument(
        "-i", "--input",
        default="output/combined/ski_areas.parquet",
        help="Input: ski_areas.parquet (polygon geometry, required) or ski_areas_analyzed.parquet (requires --boundaries)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="output/combined",
        help="Output directory for elevation parquet and contours",
    )
    parser.add_argument(
        "--cache-dir",
        default="cache",
        help="Directory for Skadi tile cache (default: cache)",
    )
    parser.add_argument(
        "--contour-interval",
        type=float,
        default=25.0,
        help="Contour interval in meters (default: 25; use 10 for very detailed)",
    )
    parser.add_argument(
        "--buffer-km",
        type=float,
        default=5.0,
        help="Buffer in km when geometry is point (default: 5)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of ski areas (for testing)",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Skip first N ski areas (use with --limit for batches, e.g. --skip 1000 --limit 2000)",
    )
    parser.add_argument(
        "--save-dem",
        action="store_true",
        help="Save cropped DEM per ski area as GeoTIFF (output/dems/<region>/<id>.tif)",
    )
    parser.add_argument(
        "--dem-dir",
        default=None,
        help="Directory for DEM GeoTIFFs (default: <output-dir>/dems when --save-dem)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip areas that already have elevation in output; merge with existing elevation/contours",
    )
    parser.add_argument(
        "--ids-file",
        default=None,
        help="JSON from elevation_preflight.py (candidates[].winter_sports_id) or a text file of ids",
    )
    parser.add_argument(
        "--clear-cache-every",
        type=int,
        default=50,
        help="Delete cached Skadi tiles after this many newly processed areas (default 50; 0=never)",
    )
    parser.add_argument(
        "--boundaries",
        default=None,
        help="Path to ski_areas.parquet with polygon geometry; used to clip contours and constrain base/summit points to resort boundaries (default: <output-dir>/ski_areas.parquet when it exists)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir)
    dem_dir = Path(args.dem_dir) if args.dem_dir else (output_dir / "dems" if args.save_dem else None)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    if dem_dir is not None:
        dem_dir.mkdir(parents=True, exist_ok=True)

    # Boundaries = polygon geometry (parquet or GeoJSON); required for correct DEM cut
    boundaries_path = Path(args.boundaries) if args.boundaries else (output_dir / "ski_areas.parquet")
    boundaries_by_key: dict = {}
    if boundaries_path.exists():
        try:
            if boundaries_path.suffix.lower() in (".geojson", ".json"):
                bounds_gdf = gpd.read_file(boundaries_path)
            else:
                bounds_gdf = gpd.read_parquet(boundaries_path)
            if bounds_gdf.crs and bounds_gdf.crs.to_epsg() != 4326:
                bounds_gdf = bounds_gdf.to_crs("EPSG:4326")
            for _, r in bounds_gdf.iterrows():
                geom_b = r.geometry
                if geom_b is None or geom_b.is_empty or isinstance(geom_b, Point):
                    continue
                ws_id_b = _first_non_na_id(r)
                region_b = str(r.get("region", "")) if r.get("region") is not None and not pd.isna(r.get("region")) else ""
                if ws_id_b is None:
                    continue
                try:
                    boundaries_by_key[(int(ws_id_b), region_b)] = geom_b
                    boundaries_by_key[(str(ws_id_b), region_b)] = geom_b
                except (ValueError, TypeError):
                    boundaries_by_key[(str(ws_id_b), region_b)] = geom_b
            print(f"Loaded {len(boundaries_by_key)} ski area boundaries from {boundaries_path}")
        except Exception as e:
            print(f"  Warning: could not load boundaries from {boundaries_path}: {e}", file=sys.stderr)
    elif args.boundaries:
        print(f"  Boundaries file not found: {boundaries_path}", file=sys.stderr)

    if not input_path.exists():
        # When pipeline requests ski_areas.parquet but it was not created (e.g. invalid geom), use analyzed parquet
        if input_path.name == "ski_areas.parquet":
            fallback = output_dir / "ski_areas_analyzed.parquet"
            if fallback.exists():
                input_path = fallback
                print(f"Using {input_path} (ski_areas.parquet not found)", file=sys.stderr)
            else:
                print("No ski areas input; skipping elevation.", file=sys.stderr)
                sys.exit(0)
        else:
            print(f"Input not found: {input_path}", file=sys.stderr)
            sys.exit(1)

    print(f"Loading {input_path}...")
    # Analyzed parquet is tabular (no geo metadata); use pandas then build geometry from centroid
    if input_path.suffix.lower() in (".geojson", ".json") and "analyzed" not in input_path.name.lower():
        gdf = gpd.read_file(input_path)
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")
        elif not gdf.crs:
            gdf.set_crs("EPSG:4326", inplace=True)
        print(f"Loaded {len(gdf)} ski areas from GeoJSON")
    elif input_path.name == "ski_areas_analyzed.parquet" or "analyzed" in input_path.name:
        df = pd.read_parquet(input_path)
        if "centroid_lat" not in df.columns or "centroid_lon" not in df.columns:
            print("No centroid_lat/centroid_lon in file; no ski areas. Skipping elevation.", file=sys.stderr)
            sys.exit(0)
        valid = df["centroid_lon"].notna() & df["centroid_lat"].notna()
        df = df[valid].copy()
        df["geometry"] = [Point(lon, lat) for lon, lat in zip(df["centroid_lon"], df["centroid_lat"])]
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        print(f"Using centroid point geometry ({len(gdf)} areas with valid centroid)")
    else:
        gdf = gpd.read_parquet(input_path)
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")
        elif not gdf.crs:
            gdf.set_crs("EPSG:4326", inplace=True)
        # If no geometry or all null, try centroid
        if "geometry" not in gdf.columns or gdf["geometry"].isna().all():
            if "centroid_lat" in gdf.columns and "centroid_lon" in gdf.columns:
                valid = gdf["centroid_lon"].notna() & gdf["centroid_lat"].notna()
                gdf = gdf[valid].copy()
                gdf["geometry"] = [Point(lon, lat) for lon, lat in zip(gdf["centroid_lon"], gdf["centroid_lat"])]
                print(f"Using centroid point geometry ({len(gdf)} areas)")
            else:
                print("No geometry or centroid columns; no ski areas. Skipping elevation.", file=sys.stderr)
                sys.exit(0)

    n_total = len(gdf)
    if args.ids_file:
        id_path = Path(args.ids_file)
        if not id_path.exists():
            print(f"ids-file not found: {id_path}", file=sys.stderr)
            sys.exit(1)
        raw = id_path.read_text(encoding="utf-8")
        want = set()
        if id_path.suffix.lower() == ".json":
            payload = json.loads(raw)
            rows = payload.get("candidates", payload if isinstance(payload, list) else [])
            for item in rows:
                if isinstance(item, dict):
                    want.add(str(item.get("winter_sports_id", "")).strip())
                else:
                    want.add(str(item).strip())
        else:
            want = {ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.startswith("#")}
        want.discard("")
        before = len(gdf)
        gdf = gdf[gdf.apply(lambda r: str(_ski_area_id(r)[0]).strip() in want, axis=1)].copy()
        print(f"Filtered to {len(gdf)} ski areas from ids-file ({before} in input, {len(want)} ids)")
        n_total = len(gdf)
    if args.skip:
        gdf = gdf.iloc[args.skip:]
        print(f"Skipped first {args.skip} ski areas; {len(gdf)} remaining")
    if args.limit:
        gdf = gdf.head(args.limit)
        print(f"Limited to {len(gdf)} ski areas")
    else:
        print(f"Processing {n_total} ski areas")

    # Build boundaries from input rows that have polygon geometry (overrides file if input has polygons)
    for _, r in gdf.iterrows():
        geom = r.geometry
        if geom is None or geom.is_empty or isinstance(geom, Point):
            continue
        ws_id_b = _first_non_na_id(r)
        region_b = str(r.get("region", "")) if r.get("region") is not None and not pd.isna(r.get("region")) else ""
        if ws_id_b is None:
            continue
        try:
            boundaries_by_key[(int(ws_id_b), region_b)] = geom
            boundaries_by_key[(str(ws_id_b), region_b)] = geom
        except (ValueError, TypeError):
            boundaries_by_key[(str(ws_id_b), region_b)] = geom
    n_poly = sum(1 for g in gdf.geometry if g and not g.is_empty and not isinstance(g, Point))
    if n_poly > 0:
        print(f"Using polygon geometry from input ({n_poly} areas with boundaries)")
    if dem_dir is not None:
        print(f"Saving DEMs to {dem_dir}/")

    # Resume: load existing elevation (and contours) so we skip already-done areas
    existing_elev_path = output_dir / "ski_areas_elevation.parquet"
    existing_contour_path = output_dir / "ski_area_contours.parquet"
    done_keys = set()
    existing_elev_by_key = {}
    existing_contour_gdf = None
    if args.resume and existing_elev_path.exists():
        existing_elev_df = pd.read_parquet(existing_elev_path)
        for _, r in existing_elev_df.iterrows():
            wi = r["winter_sports_id"]
            re = r["region"]
            k = (int(wi) if pd.notna(wi) and wi is not None else wi, str(re) if pd.notna(re) else "")
            done_keys.add(k)
            existing_elev_by_key[k] = r.to_dict()
        if existing_contour_path.exists():
            try:
                existing_contour_gdf = gpd.read_parquet(existing_contour_path)
            except Exception:
                pass
        print(f"Resume: {len(done_keys)} areas already have elevation; skipping those")

    elevation_rows = []
    all_contour_features = []
    all_point_features: List[dict] = []
    n_processed_since_clear = 0

    # Resume: load existing elevation points
    existing_point_path = output_dir / "ski_area_elevation_points.parquet"
    existing_point_gdf = None
    if args.resume and existing_point_path.exists():
        try:
            existing_point_gdf = gpd.read_parquet(existing_point_path)
        except Exception:
            pass

    for idx, row in gdf.iterrows():
        ws_id, region = _ski_area_id(row)
        # Single-region pipeline (e.g. Iceland): input parquet has no region; use REGION env so merge with analyzed works
        if not region or (isinstance(region, str) and not str(region).strip()) or pd.isna(region):
            region = os.environ.get("REGION", "")
        region_str = str(region) if region is not None and pd.notna(region) else ""
        name = row.get("name") or row.get("winter_sports_name") or ""
        wi = ws_id
        if wi is not None and pd.notna(wi):
            try:
                wi = int(wi)
            except (ValueError, TypeError):
                pass
        key = (wi, region_str)

        if args.resume and key in existing_elev_by_key:
            existing_row = existing_elev_by_key[key]
            elevation_rows.append(existing_row)
            # Keep existing points for skipped areas (filter by winter_sports_id + region)
            if existing_point_gdf is not None and len(existing_point_gdf) > 0:
                m = (existing_point_gdf["winter_sports_id"].astype(str) == str(ws_id)) & (
                    existing_point_gdf["region"].astype(str) == region_str
                )
                for _, pr in existing_point_gdf[m].iterrows():
                    all_point_features.append({
                        "type": "Feature",
                        "geometry": pr.geometry.__geo_interface__,
                        "properties": dict(pr.drop("geometry", errors="ignore")),
                    })
            if (len(elevation_rows)) % 500 == 0 and len(elevation_rows) > 0:
                print(f"  Processed {len(elevation_rows)} / {len(gdf)} (resume skip)")
            continue

        elev_lo, elev_hi, features, point_features, ski_north_angle = process_ski_area(
            row,
            cache_dir,
            contour_interval=args.contour_interval,
            buffer_km=args.buffer_km,
            dem_dir=dem_dir,
            boundaries_by_key=boundaries_by_key,
        )
        n_processed_since_clear += 1
        every = int(args.clear_cache_every or 0)
        if every > 0 and n_processed_since_clear >= every:
            skadi_dir = cache_dir / "skadi"
            if skadi_dir.exists():
                try:
                    shutil.rmtree(skadi_dir)
                    skadi_dir.mkdir(parents=True, exist_ok=True)
                    print(f"  Cleared tile cache (every {every} areas)", file=sys.stderr)
                except Exception as e:
                    print(f"  Warning: could not clear cache: {e}", file=sys.stderr)
            n_processed_since_clear = 0
        gc.collect()
        elevation_rows.append({
            "winter_sports_id": ws_id,
            "region": str(region),
            "name": str(name),
            "elevation_low_m": round(elev_lo, 1) if elev_lo is not None else None,
            "elevation_high_m": round(elev_hi, 1) if elev_hi is not None else None,
            "elevation_source": "Mapzen Skadi",
            "ski_north_angle": ski_north_angle,
        })
        all_contour_features.extend(features)
        all_point_features.extend(point_features)
        if (len(elevation_rows)) % 10 == 0 and len(elevation_rows) > 0:
            print(f"  Processed {len(elevation_rows)} / {len(gdf)}")

    # Write elevation table
    elev_df = pd.DataFrame(elevation_rows)
    elev_path = output_dir / "ski_areas_elevation.parquet"
    # If we ran with skip/limit and had existing data, merge so we don't drop other batches
    if (args.skip or args.limit or args.ids_file) and args.resume and existing_elev_path.exists() and len(existing_elev_by_key) > 0:
        existing_elev_df = pd.read_parquet(existing_elev_path)

        def _elev_key(r):
            wi = r["winter_sports_id"]
            re = r["region"]
            return (int(wi) if pd.notna(wi) and wi is not None else wi, str(re) if pd.notna(re) else "")

        batch_keys = set(_elev_key(r) for _, r in elev_df.iterrows())
        existing_keep = existing_elev_df[~existing_elev_df.apply(lambda r: _elev_key(r) in batch_keys, axis=1)]
        elev_df = pd.concat([existing_keep, elev_df], ignore_index=True)
        print(f"Merged with existing elevation; total rows {len(elev_df)}")
    elev_df.to_parquet(elev_path, index=False)
    print(f"Wrote {elev_path} ({len(elev_df)} rows)")

    # Merge elevation into ski_areas_analyzed.parquet when present (one table for frontend)
    analyzed_path = output_dir / "ski_areas_analyzed.parquet"
    if analyzed_path.exists():
        analyzed_df = pd.read_parquet(analyzed_path)
        # Normalize join keys for reliable merge (e.g. int vs float winter_sports_id)
        merge_on = ["winter_sports_id", "region"]
        if all(c in analyzed_df.columns for c in merge_on):
            elev_r = set(elev_df["region"].astype(str)) if "region" in elev_df.columns else set()
            an_r = set(analyzed_df["region"].astype(str))
            if not (elev_r & an_r):
                merge_on = ["winter_sports_id"]
                print("  Elevation merge on winter_sports_id only (region values differ)")
            elev_cols = ["winter_sports_id", "region", "elevation_low_m", "elevation_high_m", "elevation_source"]
            if "ski_north_angle" in elev_df.columns:
                elev_cols.append("ski_north_angle")
            elev_merge = elev_df[[c for c in elev_cols if c in elev_df.columns]].copy()
            # Coerce elevation merge keys to match analyzed dtypes (e.g. object vs int64)
            for col in merge_on:
                target_dtype = analyzed_df[col].dtype
                src = elev_merge[col]
                # Integer columns with NaN cannot use non-nullable int; use pandas Int64
                if pd.api.types.is_integer_dtype(target_dtype) and src.isna().any():
                    elev_merge[col] = src.astype("Int64")
                else:
                    elev_merge[col] = src.astype(target_dtype)
            # Drop existing elevation columns if re-run, then merge
            for c in ["elevation_low_m", "elevation_high_m", "elevation_source", "ski_north_angle", "vertical_drop_m", "vertical_drop_ft"]:
                if c in analyzed_df.columns:
                    analyzed_df = analyzed_df.drop(columns=[c])
            analyzed_df = analyzed_df.merge(
                elev_merge,
                on=merge_on,
                how="left",
            )
            # Derived: vertical drop (m and ft)
            analyzed_df["vertical_drop_m"] = analyzed_df.apply(
                lambda r: round(float(r["elevation_high_m"]) - float(r["elevation_low_m"]), 1)
                if pd.notna(r.get("elevation_high_m")) and pd.notna(r.get("elevation_low_m"))
                else None,
                axis=1,
            )
            analyzed_df["vertical_drop_ft"] = analyzed_df["vertical_drop_m"].apply(
                lambda x: round(x * 3.28084, 1) if pd.notna(x) and x is not None else None
            )
            analyzed_df.to_parquet(analyzed_path, index=False)
            n_with_elev = analyzed_df["elevation_low_m"].notna().sum()
            print(f"Merged elevation into {analyzed_path} ({n_with_elev} rows with elevation)")
        else:
            print(f"  Skipping merge: {analyzed_path} missing columns {merge_on}")
    else:
        print(f"  {analyzed_path} not found; skipping elevation merge")

    # Write contours GeoJSON
    if existing_contour_gdf is not None and len(existing_contour_gdf) > 0:
        if all_contour_features:
            new_contour_gdf = gpd.GeoDataFrame.from_features(all_contour_features, crs="EPSG:4326")
            contour_gdf = pd.concat([existing_contour_gdf, new_contour_gdf], ignore_index=True)
        else:
            contour_gdf = existing_contour_gdf
    elif all_contour_features:
        contour_gdf = gpd.GeoDataFrame.from_features(all_contour_features, crs="EPSG:4326")
    else:
        contour_gdf = None

    if contour_gdf is not None:
        contour_path = output_dir / "ski_area_contours.geojson"
        contour_gdf.to_file(contour_path, driver="GeoJSON")
        print(f"Wrote {contour_path} ({len(contour_gdf)} contour segments)")
        contour_parquet = output_dir / "ski_area_contours.parquet"
        contour_gdf.to_parquet(contour_parquet, index=False)
        print(f"Wrote {contour_parquet}")
    else:
        print("No contour segments generated")

    # Write elevation points (base + summit per ski area)
    if all_point_features:
        point_gdf = gpd.GeoDataFrame.from_features(all_point_features, crs="EPSG:4326")
        points_path = output_dir / "ski_area_elevation_points.geojson"
        point_gdf.to_file(points_path, driver="GeoJSON")
        print(f"Wrote {points_path} ({len(point_gdf)} points)")
        points_parquet = output_dir / "ski_area_elevation_points.parquet"
        point_gdf.to_parquet(points_parquet, index=False)
        print(f"Wrote {points_parquet}")
    else:
        print("No elevation points generated")

    print("Done.")


if __name__ == "__main__":
    main()
