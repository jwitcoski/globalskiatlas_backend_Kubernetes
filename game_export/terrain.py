"""Heightfields, slope/aspect/hillshade/normals, and terrain mesh."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import calculate_default_transform, reproject

from game_export.coords import LocalCRS, make_transformers, utm_crs_from_lonlat
from game_export.glb import write_terrain_glb
from game_export import jsonutil

log = logging.getLogger("game_export")

UINT16_NODATA = 65535


def _quantize_u16(elev: np.ndarray, nodata_mask: np.ndarray) -> tuple[np.ndarray, float, float, float, float]:
    valid = elev[~nodata_mask]
    if valid.size == 0:
        raise RuntimeError("DEM has no valid elevation samples in the scene AOI")
    zmin = float(np.nanmin(valid))
    zmax = float(np.nanmax(valid))
    span = max(zmax - zmin, 1e-3)
    scale = span / 65534.0
    offset = zmin
    q = np.full(elev.shape, UINT16_NODATA, dtype=np.uint16)
    q[~nodata_mask] = np.clip(
        np.round((elev[~nodata_mask] - offset) / scale), 0, 65534
    ).astype(np.uint16)
    return q, offset, scale, zmin, zmax


def _write_hf_meta(
    path: Path,
    *,
    rows: int,
    cols: int,
    cell_m: float,
    offset: float,
    scale: float,
    zmin: float,
    zmax: float,
    local: LocalCRS,
    west_e: float,
    south_n: float,
    east_e: float,
    north_n: float,
    label: str,
) -> dict:
    meta = {
        "format": "uint16_raw",
        "filename": path.name.replace("-metadata.json", "-u16.bin")
        if "metadata" in path.name
        else path.with_suffix(".bin").name,
        "dtype": "uint16",
        "byte_order": "little_endian",
        "layout": "row_major",
        "row_0": "north",
        "col_0": "west",
        "rows": rows,
        "cols": cols,
        "cell_size_m": cell_m,
        "nodata_uint16": UINT16_NODATA,
        "elevation_offset_m": offset,
        "elevation_scale_m": scale,
        "elevation_reconstruction": "elevation_m = elevation_offset_m + uint16_value * elevation_scale_m",
        "elevation_min_m": zmin,
        "elevation_max_m": zmax,
        "bounds_projected_m": {
            "west_easting": west_e,
            "south_northing": south_n,
            "east_easting": east_e,
            "north_northing": north_n,
        },
        "bounds_local_m": {
            "min_east_m": west_e - local.origin_easting_m,
            "min_north_m": south_n - local.origin_northing_m,
            "max_east_m": east_e - local.origin_easting_m,
            "max_north_m": north_n - local.origin_northing_m,
        },
        "pixel_to_projected": {
            "easting_m": "west_easting + (col + 0.5) * cell_size_m",
            "northing_m": "north_northing - (row + 0.5) * cell_size_m",
        },
        "pixel_to_game": {
            "x": "easting_m - local_origin.easting_m",
            "y": "elevation_m",
            "z": "-(northing_m - local_origin.northing_m)",
        },
        "kind": label,
    }
    path.write_text(jsonutil.dumps(meta), encoding="utf-8")
    return meta


def warp_dem_to_utm(
    dem_path: Path,
    projected_crs: str,
    dst_res_m: float,
) -> tuple[np.ndarray, rasterio.Affine, float]:
    with rasterio.open(dem_path) as src:
        src_crs = src.crs
        nodata = src.nodata
        transform, width, height = calculate_default_transform(
            src_crs,
            projected_crs,
            src.width,
            src.height,
            *src.bounds,
            resolution=dst_res_m,
        )
        dst = np.full((height, width), np.nan, dtype=np.float32)
        reproject(
            source=rasterio.band(src, 1),
            destination=dst,
            src_transform=src.transform,
            src_crs=src_crs,
            dst_transform=transform,
            dst_crs=projected_crs,
            resampling=Resampling.bilinear,
            src_nodata=nodata,
            dst_nodata=np.nan,
        )
        src_crs_str = src.crs.to_string() if src.crs else "unknown"
    return dst, transform, src_crs_str


def _grid_xy(transform: rasterio.Affine, rows: int, cols: int) -> tuple[np.ndarray, np.ndarray]:
    cols_i, rows_i = np.meshgrid(np.arange(cols, dtype=np.float64), np.arange(rows, dtype=np.float64))
    xs = transform.c + (cols_i + 0.5) * transform.a + (rows_i + 0.5) * transform.b
    ys = transform.f + (cols_i + 0.5) * transform.d + (rows_i + 0.5) * transform.e
    return xs, ys


def _derivatives(elev: np.ndarray, cell_m: float) -> tuple[np.ndarray, np.ndarray]:
    """dz/dx, dz/dy in meters (y north). nan-safe central differences."""
    dzdx = np.full_like(elev, np.nan, dtype=np.float64)
    dzdy = np.full_like(elev, np.nan, dtype=np.float64)
    dzdx[:, 1:-1] = (elev[:, 2:] - elev[:, :-2]) / (2.0 * cell_m)
    dzdy[1:-1, :] = (elev[:-2, :] - elev[2:, :]) / (2.0 * cell_m)
    return dzdx, dzdy


def _slope_aspect_hillshade_normals(elev: np.ndarray, cell_m: float):
    dzdx, dzdy = _derivatives(elev, cell_m)
    slope_rad = np.arctan(np.hypot(dzdx, dzdy))
    slope_deg = np.degrees(slope_rad)
    slope_pct = np.hypot(dzdx, dzdy) * 100.0
    # aspect: 0 north, clockwise-ish from atan2(-dzdx, dzdy)? GDAL: atan2(dzdy, -dzdx)
    aspect = (np.degrees(np.arctan2(dzdx, -dzdy)) + 360.0) % 360.0
    # Hillshade: altitude 45, azimuth 315
    alt = np.radians(45.0)
    az = np.radians(315.0)
    hs = np.sin(alt) * np.cos(slope_rad) + np.cos(alt) * np.sin(slope_rad) * np.cos(
        az - np.radians(aspect)
    )
    hs = np.clip(hs, 0, 1)
    # Terrain normals in projected ENU: x east, y north, z up
    nx = -dzdx
    ny = -dzdy
    nz = np.ones_like(elev)
    nlen = np.sqrt(nx * nx + ny * ny + nz * nz)
    nx, ny, nz = nx / nlen, ny / nlen, nz / nlen
    # Game normals: X east, Y up, Z negative north → (nx, nz, -ny)
    gx, gy, gz = nx, nz, -ny
    roughness = np.full_like(elev, np.nan)
    roughness[1:-1, 1:-1] = np.abs(
        elev[1:-1, 1:-1]
        - 0.25 * (elev[:-2, 1:-1] + elev[2:, 1:-1] + elev[1:-1, :-2] + elev[1:-1, 2:])
    )
    return slope_deg, slope_pct, aspect, hs, gx, gy, gz, roughness


def sample_elev(elev: np.ndarray, transform: rasterio.Affine, easting: float, northing: float) -> float:
    r, c = rasterio.transform.rowcol(transform, easting, northing)
    if r < 0 or c < 0 or r >= elev.shape[0] or c >= elev.shape[1]:
        return float("nan")
    v = float(elev[r, c])
    return v


def export_homepage_terrain(
    dem_path: Path,
    out_dir: Path,
    cfg,
    *,
    mesh_resolution_m: float,
) -> dict:
    """Coarse terrain mesh only — no heightfields, previews, or collision grids."""
    from game_export.config import GameExportConfig

    assert isinstance(cfg, GameExportConfig)
    mesh_res = float(mesh_resolution_m)
    work_res = mesh_res

    with rasterio.open(dem_path) as src:
        src_crs = src.crs.to_string() if src.crs else "EPSG:4326"
        b = src.bounds
        clon = (b.left + b.right) / 2
        clat = (b.bottom + b.top) / 2
        if src.crs and src.crs.to_epsg() != 4326:
            to_wgs = rasterio.warp.transform(src.crs, "EPSG:4326", [clon], [clat])
            clon, clat = float(to_wgs[0][0]), float(to_wgs[1][0])

    if cfg.target_crs == "auto_utm":
        projected = utm_crs_from_lonlat(clon, clat)
    else:
        projected = cfg.target_crs

    log.info("Homepage terrain %s → %s @ %sm", dem_path, projected, work_res)
    elev, transform, src_crs_str = warp_dem_to_utm(dem_path, projected, work_res)
    rows, cols = elev.shape
    xs, ys = _grid_xy(transform, rows, cols)
    west_e = float(transform.c)
    north_n = float(transform.f)
    east_e = west_e + cols * work_res
    south_n = north_n - rows * work_res

    to_proj, to_wgs = make_transformers(projected)
    origin_e, origin_n = west_e, south_n
    olon, olat = to_wgs.transform(origin_e, origin_n)
    local = LocalCRS(
        source_crs=src_crs_str,
        projected_crs=projected,
        origin_easting_m=float(origin_e),
        origin_northing_m=float(origin_n),
        origin_longitude=float(olon),
        origin_latitude=float(olat),
    )

    nodata = ~np.isfinite(elev)
    slope_deg, slope_pct, aspect, hs, gx, gy, gz, roughness = _slope_aspect_hillshade_normals(
        np.where(nodata, np.nan, elev), work_res
    )

    terrain_dir = out_dir / "terrain"
    terrain_dir.mkdir(parents=True, exist_ok=True)

    mesh_elev = elev
    mesh_cell = work_res
    mr, mc = mesh_elev.shape
    mx, my = _grid_xy(
        rasterio.Affine(mesh_cell, 0, west_e, 0, -mesh_cell, north_n), mr, mc
    )
    valid = np.isfinite(mesh_elev)
    xg = mx - local.origin_easting_m
    zg = -(my - local.origin_northing_m)
    yg = np.where(valid, mesh_elev, 0.0).astype(np.float64)
    pos = np.stack([xg, yg, zg], axis=-1).reshape(-1, 3).astype(np.float32)
    nrm = np.stack(
        [np.nan_to_num(gx, nan=0.0), np.nan_to_num(gy, nan=1.0), np.nan_to_num(gz, nan=0.0)],
        axis=-1,
    ).reshape(-1, 3).astype(np.float32)
    ii = np.arange(mr * mc, dtype=np.uint32).reshape(mr, mc)
    tmask = valid[:-1, :-1] & valid[:-1, 1:] & valid[1:, :-1] & valid[1:, 1:]
    i00 = ii[:-1, :-1][tmask]
    i10 = ii[:-1, 1:][tmask]
    i01 = ii[1:, :-1][tmask]
    i11 = ii[1:, 1:][tmask]
    idx = np.stack([i00, i10, i11, i00, i11, i01], axis=1).reshape(-1).astype(np.uint32)
    glb_info = write_terrain_glb(terrain_dir / "terrain-mesh.glb", pos, nrm, idx)

    valid_elev = elev[np.isfinite(elev)]
    zmin = float(np.nanmin(valid_elev))
    zmax = float(np.nanmax(valid_elev))
    terrain_meta = {
        "kind": "homepage_hero",
        "source_dem": str(dem_path),
        "source_crs": src_crs_str,
        "projected_crs": projected,
        "work_resolution_m": work_res,
        "mesh": {
            "file": "terrain-mesh.glb",
            **glb_info,
            "vertex_spacing_m": mesh_cell,
            "coordinate_space": "game (X east, Y elevation, Z negative north)",
        },
        "local_crs": local.to_dict(),
        "grid": {"rows": rows, "cols": cols, "cell_m": work_res},
        "elevation_min_m": zmin,
        "elevation_max_m": zmax,
    }
    (terrain_dir / "terrain-metadata.json").write_text(
        jsonutil.dumps(terrain_meta), encoding="utf-8"
    )
    return {
        "local": local,
        "elev": elev,
        "transform": transform,
        "cell_m": work_res,
        "slope_deg": slope_deg,
        "slope_pct": slope_pct,
        "hillshade": hs,
        "to_proj": to_proj,
        "to_wgs": to_wgs,
        "terrain_meta": terrain_meta,
        "west_e": west_e,
        "south_n": south_n,
        "east_e": east_e,
        "north_n": north_n,
    }


def export_terrain(
    dem_path: Path,
    out_dir: Path,
    cfg,
    local: LocalCRS | None = None,
) -> dict:
    """Warp DEM, write heightfields, derived rasters, GLB, metadata."""
    from game_export.config import GameExportConfig

    assert isinstance(cfg, GameExportConfig)
    mesh_res = cfg.terrain_mesh_resolution_m
    hf_res = cfg.heightfield_resolution_m
    col_res = cfg.collision_heightfield_resolution_m

    # Work at the finest requested grid, then subsample.
    work_res = min(hf_res, mesh_res, col_res)
    # Peek source CRS and centroid for UTM
    with rasterio.open(dem_path) as src:
        src_crs = src.crs.to_string() if src.crs else "EPSG:4326"
        b = src.bounds
        clon = (b.left + b.right) / 2
        clat = (b.bottom + b.top) / 2
        if src.crs and src.crs.to_epsg() != 4326:
            to_wgs = rasterio.warp.transform(src.crs, "EPSG:4326", [clon], [clat])
            clon, clat = float(to_wgs[0][0]), float(to_wgs[1][0])

    if cfg.target_crs == "auto_utm":
        projected = utm_crs_from_lonlat(clon, clat)
    else:
        projected = cfg.target_crs

    log.info("Warping DEM %s → %s @ %sm", dem_path, projected, work_res)
    elev, transform, src_crs_str = warp_dem_to_utm(dem_path, projected, work_res)
    rows, cols = elev.shape
    xs, ys = _grid_xy(transform, rows, cols)
    west_e = float(transform.c)
    north_n = float(transform.f)
    east_e = west_e + cols * work_res
    south_n = north_n - rows * work_res

    to_proj, to_wgs = make_transformers(projected)
    origin_e, origin_n = west_e, south_n  # SW corner of raster
    olon, olat = to_wgs.transform(origin_e, origin_n)
    local = LocalCRS(
        source_crs=src_crs_str,
        projected_crs=projected,
        origin_easting_m=float(origin_e),
        origin_northing_m=float(origin_n),
        origin_longitude=float(olon),
        origin_latitude=float(olat),
    )

    nodata = ~np.isfinite(elev)
    slope_deg, slope_pct, aspect, hs, gx, gy, gz, roughness = _slope_aspect_hillshade_normals(
        np.where(nodata, np.nan, elev), work_res
    )

    terrain_dir = out_dir / "terrain"
    terrain_dir.mkdir(parents=True, exist_ok=True)

    def _subsample(arr, src_res, dst_res):
        step = max(1, int(round(dst_res / src_res)))
        return arr[::step, ::step], src_res * step

    def _write_heightfield(arr, res, bin_name, meta_name, label):
        sub, cell = _subsample(arr, work_res, res)
        mask = ~np.isfinite(sub)
        q, off, sc, zmin, zmax = _quantize_u16(np.where(mask, 0, sub), mask)
        bpath = terrain_dir / bin_name
        bpath.write_bytes(q.tobytes(order="C"))
        west = west_e
        north = north_n
        south = north - sub.shape[0] * cell
        east = west + sub.shape[1] * cell
        meta = _write_hf_meta(
            terrain_dir / meta_name,
            rows=sub.shape[0],
            cols=sub.shape[1],
            cell_m=cell,
            offset=off,
            scale=sc,
            zmin=zmin,
            zmax=zmax,
            local=local,
            west_e=west,
            south_n=south,
            east_e=east,
            north_n=north,
            label=label,
        )
        # filename field
        meta["filename"] = bin_name
        (terrain_dir / meta_name).write_text(jsonutil.dumps(meta), encoding="utf-8")
        return meta, sub, cell

    hf_meta, hf_elev, hf_cell = _write_heightfield(
        elev, hf_res, "heightfield-u16.bin", "heightfield-metadata.json", "gameplay"
    )
    col_meta, _, _ = _write_heightfield(
        elev,
        col_res,
        "collision-heightfield-u16.bin",
        "collision-heightfield-metadata.json",
        "collision",
    )

    # Mesh at mesh_res
    mesh_elev, mesh_cell = _subsample(elev, work_res, mesh_res)
    mr, mc = mesh_elev.shape
    mx, my = _grid_xy(
        rasterio.Affine(mesh_cell, 0, west_e, 0, -mesh_cell, north_n), mr, mc
    )
    valid = np.isfinite(mesh_elev)
    gx_s, _ = _subsample(gx, work_res, mesh_res)
    gy_s, _ = _subsample(gy, work_res, mesh_res)
    gz_s, _ = _subsample(gz, work_res, mesh_res)
    mr, mc = mesh_elev.shape
    gx_s = np.asarray(gx_s)[:mr, :mc]
    gy_s = np.asarray(gy_s)[:mr, :mc]
    gz_s = np.asarray(gz_s)[:mr, :mc]
    if gx_s.shape != mesh_elev.shape:
        gx_s = np.resize(gx_s, mesh_elev.shape)
        gy_s = np.resize(gy_s, mesh_elev.shape)
        gz_s = np.resize(gz_s, mesh_elev.shape)
    xg = mx - local.origin_easting_m
    zg = -(my - local.origin_northing_m)
    yg = np.where(valid, mesh_elev, 0.0).astype(np.float64)
    pos = np.stack([xg, yg, zg], axis=-1).reshape(-1, 3).astype(np.float32)
    nrm = np.stack(
        [np.nan_to_num(gx_s, nan=0.0), np.nan_to_num(gy_s, nan=1.0), np.nan_to_num(gz_s, nan=0.0)],
        axis=-1,
    ).reshape(-1, 3).astype(np.float32)
    ii = np.arange(mr * mc, dtype=np.uint32).reshape(mr, mc)
    tmask = valid[:-1, :-1] & valid[:-1, 1:] & valid[1:, :-1] & valid[1:, 1:]
    i00 = ii[:-1, :-1][tmask]
    i10 = ii[:-1, 1:][tmask]
    i01 = ii[1:, :-1][tmask]
    i11 = ii[1:, 1:][tmask]
    idx = np.stack([i00, i10, i11, i00, i11, i01], axis=1).reshape(-1).astype(np.uint32)
    glb_info = write_terrain_glb(terrain_dir / "terrain-mesh.glb", pos, nrm, idx)

    # PNG previews via matplotlib
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    def _save_img(arr, path, cmap, vmin=None, vmax=None, title=""):
        fig, ax = plt.subplots(figsize=(8, 8))
        im = ax.imshow(arr, cmap=cmap, vmin=vmin, vmax=vmax)
        ax.set_title(title)
        ax.set_xlabel("col (west→east)")
        ax.set_ylabel("row (north→south)")
        fig.colorbar(im, ax=ax, fraction=0.046)
        ax.text(
            0.01,
            0.01,
            "© OpenStreetMap contributors · Mapzen Skadi DEM\nPrototype — not a navigation product",
            transform=ax.transAxes,
            fontsize=7,
            color="white",
            va="bottom",
        )
        # Simple north arrow
        ax.annotate("N", xy=(0.95, 0.12), xytext=(0.95, 0.02), xycoords="axes fraction",
                    arrowprops=dict(arrowstyle="->", color="white"), color="white", ha="center")
        fig.tight_layout()
        fig.savefig(path, dpi=120)
        plt.close(fig)

    preview = np.where(np.isfinite(elev), elev, np.nan)
    _save_img(preview, terrain_dir / "terrain-preview.png", "terrain", title="Elevation (m)")
    _save_img(hs, terrain_dir / "hillshade.png", "gray", 0, 1, title="Hillshade")
    _save_img(slope_deg, terrain_dir / "slope.png", "YlOrRd", 0, 60, title="Slope (degrees)")
    _save_img(aspect, terrain_dir / "aspect.png", "hsv", 0, 360, title="Aspect (degrees)")
    # Normal map RGB in game axes, 0-255, Y-up encoded in G
    nmap = np.dstack(
        [
            np.clip((gx + 1) * 0.5, 0, 1),
            np.clip((gy + 1) * 0.5, 0, 1),
            np.clip((gz + 1) * 0.5, 0, 1),
        ]
    )
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.imshow(np.nan_to_num(nmap, nan=0.5))
    ax.set_title("Terrain normals (game RGB = X/Y/Z)")
    fig.tight_layout()
    fig.savefig(terrain_dir / "normal-map.png", dpi=120)
    plt.close(fig)

    np.save(terrain_dir / "slope-deg.npy", slope_deg)
    terrain_meta = {
        "source_dem": str(dem_path),
        "source_crs": src_crs_str,
        "projected_crs": projected,
        "work_resolution_m": work_res,
        "nodata_source": "NaN after warp (Skadi nodata -32768)",
        "heightfield": hf_meta,
        "collision_heightfield": col_meta,
        "mesh": {
            "file": "terrain-mesh.glb",
            **glb_info,
            "vertex_spacing_m": mesh_cell,
            "coordinate_space": "game (X east, Y elevation, Z negative north)",
        },
        "local_crs": local.to_dict(),
        "grid": {"rows": rows, "cols": cols, "cell_m": work_res},
        "elevation_min_m": hf_meta["elevation_min_m"],
        "elevation_max_m": hf_meta["elevation_max_m"],
    }
    (terrain_dir / "terrain-metadata.json").write_text(
        jsonutil.dumps(terrain_meta), encoding="utf-8"
    )
    return {
        "local": local,
        "elev": elev,
        "transform": transform,
        "cell_m": work_res,
        "slope_deg": slope_deg,
        "slope_pct": slope_pct,
        "hillshade": hs,
        "to_proj": to_proj,
        "to_wgs": to_wgs,
        "terrain_meta": terrain_meta,
        "west_e": west_e,
        "south_n": south_n,
        "east_e": east_e,
        "north_n": north_n,
    }
