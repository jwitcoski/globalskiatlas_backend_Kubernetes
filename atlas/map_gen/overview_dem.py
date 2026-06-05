"""Fetch Mapzen Skadi DEM and build a winter hillshade for regional overview maps."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import rasterio
from rasterio import features
from rasterio.transform import from_bounds
from rasterio.warp import Resampling
from shapely.geometry import mapping

from scripts.ski_area_elevation_contours import (
    HGT_NO_DATA,
    _crop_dem_to_bbox,
    _mosaic_tiles,
    fetch_skadi_tile,
    tiles_for_bbox,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CACHE = REPO_ROOT / "cache"
# Bump when hillshade contrast / color ramp changes (triggers rebuild in build_overview_qgz).
DEM_STYLE_VERSION = 5
# Mist/fog ceiling (m above regional valley floor) — IeQGIS / John Nelson sfumato style.
MIST_CEILING_M = 3000.0


def _build_overviews(path: Path) -> None:
    """Add internal overviews to make QGIS layout rendering fast."""
    try:
        with rasterio.open(path, "r+") as ds:
            h, w = ds.height, ds.width
            if h < 256 or w < 256:
                return
            factors = [2, 4, 8, 16]
            ds.build_overviews(factors, Resampling.average)
            ds.update_tags(ns="rio_overview", resampling="average")
    except Exception:
        # Overviews are only a performance hint; never fail the pipeline for this.
        return


def _hillshade(
    dem: np.ndarray,
    *,
    cellsize_x: float,
    cellsize_y: float,
    azimuth: float = 315.0,
    altitude: float = 35.0,
) -> np.ndarray:
    """Return uint8 hillshade 0–255; nodata pixels stay 255 (masked later)."""
    elev = dem.astype(np.float64)
    valid = elev != HGT_NO_DATA
    if not np.any(valid):
        raise RuntimeError("DEM has no valid elevation pixels")
    fill = float(np.nanmedian(elev[valid]))
    elev = np.where(valid, elev, fill)

    # Raster rows run south from north; flip dy so gradient matches map coordinates.
    dy, dx = np.gradient(elev, cellsize_y, cellsize_x)
    dy = -dy
    slope = np.pi / 2.0 - np.arctan(np.hypot(dx, dy))
    aspect = np.arctan2(-dx, dy)
    az = np.radians(360.0 - azimuth + 90.0)
    alt = np.radians(altitude)
    shaded = np.sin(alt) * np.sin(slope) + np.cos(alt) * np.cos(slope) * np.cos(az - aspect)
    out = np.clip((shaded + 1.0) * 0.5 * 255.0, 0, 255).astype(np.uint8)
    out[~valid] = 255
    return out


def _boost_hillshade_contrast(hs: np.ndarray, inside: np.ndarray) -> np.ndarray:
    """Stretch hillshade inside the unit so ridges and valleys read clearly."""
    out = hs.copy()
    mask = inside & (hs < 255)
    if not np.any(mask):
        return out
    vals = hs[mask].astype(np.float32)
    lo, hi = np.percentile(vals, [1, 99])
    if hi <= lo:
        hi = lo + 1.0
    stretched = np.clip((vals - lo) / (hi - lo), 0.0, 1.0)
    # S-curve: deepen shadows, keep bright peaks
    stretched = np.power(stretched, 0.82)
    out[mask] = (stretched * 255.0).astype(np.uint8)
    return out


def _elevation_winter_rgb(
    elev: np.ndarray, inside: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Hypsometric winter tint from elevation (base layer under hillshade overlay)."""
    valid = inside & (elev != HGT_NO_DATA)
    r = np.full(elev.shape, 255, dtype=np.uint8)
    g = np.full(elev.shape, 255, dtype=np.uint8)
    b = np.full(elev.shape, 255, dtype=np.uint8)
    if not np.any(valid):
        return r, g, b
    vals = elev[valid].astype(np.float32)
    lo, hi = np.percentile(vals, [2, 98])
    if hi <= lo:
        hi = lo + 1.0
    t = np.clip((elev.astype(np.float32) - lo) / (hi - lo), 0.0, 1.0)
    t = np.power(t, 0.85)
    r[valid] = np.clip(70 + 185 * t[valid], 0, 255).astype(np.uint8)
    g[valid] = np.clip(110 + 145 * t[valid], 0, 255).astype(np.uint8)
    b[valid] = np.clip(165 + 90 * t[valid], 0, 255).astype(np.uint8)
    return r, g, b


def _hillshade_overlay_band(hs: np.ndarray, inside: np.ndarray) -> np.ndarray:
    """Grayscale hillshade for semi-transparent overlay; 0 = nodata outside state."""
    out = np.zeros(hs.shape, dtype=np.uint8)
    out[inside] = hs[inside]
    return out


def _mist_rgba(
    elev: np.ndarray, inside: np.ndarray, *, mist_ceiling_m: float = MIST_CEILING_M
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Elevation-based atmospheric mist (RGBA) for Lighten blend in QGIS.

    Follows the IeQGIS sfumato recipe: HSV(215, 15%, 50%) with opacity ramped
    from valley (subtle) to peak (stronger), capped at ``mist_ceiling_m`` above
    the regional valley floor.
    """
    import colorsys

    h, w = elev.shape
    r = np.zeros((h, w), dtype=np.uint8)
    g = np.zeros((h, w), dtype=np.uint8)
    b = np.zeros((h, w), dtype=np.uint8)
    a = np.zeros((h, w), dtype=np.uint8)

    valid = inside & (elev != HGT_NO_DATA)
    if not np.any(valid):
        return r, g, b, a

    rf, gf, bf = colorsys.hsv_to_rgb(215.0 / 360.0, 0.15, 0.50)
    mr, mg, mb = int(rf * 255), int(gf * 255), int(bf * 255)

    vals = elev[valid].astype(np.float32)
    valley = float(np.percentile(vals, 5))
    relief = float(np.percentile(vals, 98) - valley)
    ceiling = float(max(400.0, min(mist_ceiling_m, relief * 0.85)))

    # 0 m above valley floor -> 75% transparent; ceiling -> fully opaque mist tint
    t = np.clip((elev.astype(np.float32) - valley) / ceiling, 0.0, 1.0)
    alpha = (64.0 + t * 191.0).astype(np.uint8)  # 64..255

    r[valid] = mr
    g[valid] = mg
    b[valid] = mb
    a[valid] = alpha[valid]
    return r, g, b, a


def _pick_res_m(boundary_proj: gpd.GeoDataFrame) -> float:
    """Pixel size in meters for projected hillshade (~1–2k px across the unit)."""
    minx, miny, maxx, maxy = boundary_proj.total_bounds
    span = max(maxx - minx, maxy - miny)
    return float(max(200.0, min(450.0, span / 900.0)))


def _warp_elevation_to_crs(
    dem: np.ndarray,
    west: float,
    south: float,
    east: float,
    north: float,
    boundary_wgs: gpd.GeoDataFrame,
    target_crs: str,
    res_m: float,
    work_dir: Path,
) -> tuple[np.ndarray, object, str]:
    """Warp Skadi mosaic from WGS84 to target_crs; return (dem, transform, crs)."""
    from rasterio.crs import CRS
    from rasterio.warp import Resampling, calculate_default_transform, reproject

    h, w = dem.shape
    src_transform = from_bounds(west, south, east, north, w, h)
    src_crs = CRS.from_epsg(4326)
    dst_crs = CRS.from_user_input(target_crs)

    boundary_proj = boundary_wgs.to_crs(target_crs)
    minx, miny, maxx, maxy = boundary_proj.total_bounds
    pad = res_m * 4
    minx -= pad
    miny -= pad
    maxx += pad
    maxy += pad

    width = max(2, int((maxx - minx) / res_m))
    height = max(2, int((maxy - miny) / res_m))
    dst_transform = from_bounds(minx, miny, maxx, maxy, width, height)

    dem_p = np.full((height, width), HGT_NO_DATA, dtype=np.int32)
    reproject(
        source=dem.astype(np.int32),
        destination=dem_p,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        resampling=Resampling.cubic,
        src_nodata=HGT_NO_DATA,
        dst_nodata=HGT_NO_DATA,
    )

    # Mask to admin polygon in projected CRS
    geom = boundary_proj.geometry.union_all()
    inside = features.geometry_mask(
        [mapping(geom)],
        out_shape=(height, width),
        transform=dst_transform,
        invert=True,
    )
    dem_p[~inside] = HGT_NO_DATA

    crs_out = dst_crs.to_string() if hasattr(dst_crs, "to_string") else target_crs
    return dem_p, dst_transform, crs_out


def build_overview_hillshade(
    boundary_path: Path,
    out_tif: Path,
    *,
    target_crs: str = "EPSG:4326",
    cache_dir: Optional[Path] = None,
    pad_deg: float = 0.02,
) -> Path:
    """
    Download Skadi tiles, build hillshade in ``target_crs``, clip to admin boundary.

    For projected maps, elevation is warped with GDAL (cubic) in meters before
    hillshading — do not warp an RGB hillshade afterward.
    """
    cache_dir = cache_dir or DEFAULT_CACHE
    boundary = gpd.read_file(boundary_path)
    if boundary.crs is None:
        boundary = boundary.set_crs("EPSG:4326")
    else:
        boundary = boundary.to_crs("EPSG:4326")

    geom = boundary.geometry.union_all()
    tile_coords: set[tuple[int, int]] = set()
    geoms = list(boundary.geometry)
    if not geoms:
        geoms = [geom]
    for part in geoms:
        if part is None or part.is_empty:
            continue
        minx, miny, maxx, maxy = part.bounds
        minx -= pad_deg
        miny -= pad_deg
        maxx += pad_deg
        maxy += pad_deg
        tile_coords.update(tiles_for_bbox(miny, minx, maxy, maxx))
    if not tile_coords:
        raise RuntimeError(f"No Skadi tile coords for {boundary_path}")
    tiles = []
    for lat_sw, lon_sw in tile_coords:
        arr = fetch_skadi_tile(lat_sw, lon_sw, cache_dir)
        if arr is not None:
            tiles.append((lat_sw, lon_sw, arr))
    if not tiles:
        minx, miny, maxx, maxy = boundary.total_bounds
        raise RuntimeError(f"No Skadi tiles fetched for bbox {miny},{minx},{maxy},{maxx}")

    minx, miny, maxx, maxy = boundary.total_bounds
    minx -= pad_deg
    miny -= pad_deg
    maxx += pad_deg
    maxy += pad_deg
    dem, west, south, east, north = _mosaic_tiles(tiles)
    dem, west, south, east, north = _crop_dem_to_bbox(
        dem, west, south, east, north, minx, miny, maxx, maxy
    )

    out_tif.parent.mkdir(parents=True, exist_ok=True)
    work_dir = out_tif.parent

    if target_crs.upper() not in ("EPSG:4326", "OGC:CRS84"):
        res_m = _pick_res_m(boundary.to_crs(target_crs))
        dem, transform, out_crs = _warp_elevation_to_crs(
            dem, west, south, east, north, boundary, target_crs, res_m, work_dir
        )
        cell_x = cell_y = res_m
        boundary_mask = boundary.to_crs(out_crs)
        geom_mask = boundary_mask.geometry.union_all()
    else:
        h, w = dem.shape
        if h < 2 or w < 2:
            raise RuntimeError("DEM crop too small")
        cell_x = (east - west) / w
        cell_y = (north - south) / h
        transform = from_bounds(west, south, east, north, w, h)
        out_crs = "EPSG:4326"
        geom_mask = geom

    h, w = dem.shape
    if h < 2 or w < 2:
        raise RuntimeError("DEM crop too small after projection")

    hs = _hillshade(dem, cellsize_x=cell_x, cellsize_y=cell_y)
    inside = features.geometry_mask(
        [mapping(geom_mask)],
        out_shape=(h, w),
        transform=transform,
        invert=True,
    )
    hs = _boost_hillshade_contrast(hs, inside)
    color_r, color_g, color_b = _elevation_winter_rgb(dem, inside)
    overlay = _hillshade_overlay_band(hs, inside)
    mist_r, mist_g, mist_b, mist_a = _mist_rgba(dem, inside)

    base_profile = {
        "driver": "GTiff",
        "height": h,
        "width": w,
        "dtype": "uint8",
        "crs": out_crs,
        "transform": transform,
        "compress": "deflate",
    }
    color_path = out_tif.parent / "dem_color.tif"
    overlay_path = out_tif.parent / "dem_hillshade_overlay.tif"
    with rasterio.open(color_path, "w", count=3, **base_profile) as dst:
        dst.write(color_r, 1)
        dst.write(color_g, 2)
        dst.write(color_b, 3)
    _build_overviews(color_path)
    with rasterio.open(
        overlay_path, "w", count=1, nodata=0, **base_profile
    ) as dst:
        dst.write(overlay, 1)
    _build_overviews(overlay_path)

    mist_path = out_tif.parent / "dem_mist.tif"
    with rasterio.open(mist_path, "w", count=4, **base_profile) as dst:
        dst.write(mist_r, 1)
        dst.write(mist_g, 2)
        dst.write(mist_b, 3)
        dst.write(mist_a, 4)
    _build_overviews(mist_path)

    # Legacy combined RGB (hillshade × color baked in) for tools expecting one file
    ov = overlay.astype(np.float32) / 255.0
    with rasterio.open(out_tif, "w", count=3, **base_profile) as dst:
        for i, src in enumerate((color_r, color_g, color_b), start=1):
            baked_band = np.clip(
                src.astype(np.float32) * (0.55 + 0.45 * ov), 0, 255
            ).astype(np.uint8)
            dst.write(baked_band, i)

    # Sidecar so build_overview_qgz knows to refresh after style tweaks
    meta_path = out_tif.parent / "overview_meta.json"
    if meta_path.is_file():
        import json

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["dem_style_version"] = DEM_STYLE_VERSION
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return out_tif


def main() -> int:
    import argparse
    import sys

    ap = argparse.ArgumentParser(description="Build dem_hillshade.tif for an overview folder")
    ap.add_argument("dir", type=Path, help="Overview unit folder with admin_boundary.geojson")
    ap.add_argument("--crs", type=str, default="EPSG:4326", help="Output CRS (e.g. EPSG:5070)")
    args = ap.parse_args()
    out_dir = args.dir
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    boundary = out_dir / "admin_boundary.geojson"
    out_tif = out_dir / "dem_hillshade.tif"
    try:
        build_overview_hillshade(boundary, out_tif, target_crs=args.crs)
        print(f"Wrote {out_tif} ({args.crs})")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
