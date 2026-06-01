"""Reproject overview vector layers to the map CRS (DEM is built in that CRS separately)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd


def reproject_overview_vectors(
    out_dir: Path,
    meta: dict[str, Any],
    crs: str,
) -> tuple[Path, Path, str]:
    """
    Write *_proj.geojson for boundary and resorts. Does not touch the DEM.

    Returns (boundary_path, resorts_path, crs).
    """
    admin = out_dir / "admin_boundary.geojson"
    resorts = out_dir / "ski_resorts.geojson"

    boundary = gpd.read_file(admin)
    points = gpd.read_file(resorts)
    if boundary.crs is None:
        boundary = boundary.set_crs("EPSG:4326")
    if points.crs is None:
        points = points.set_crs("EPSG:4326")

    if crs.upper() in ("EPSG:4326", "OGC:CRS84"):
        b_out, r_out = admin, resorts
    else:
        boundary.to_crs(crs).to_file(out_dir / "admin_boundary_proj.geojson", driver="GeoJSON")
        points.to_crs(crs).to_file(out_dir / "ski_resorts_proj.geojson", driver="GeoJSON")
        b_out = out_dir / "admin_boundary_proj.geojson"
        r_out = out_dir / "ski_resorts_proj.geojson"

    meta_out = {**meta, "crs": crs}
    (out_dir / "overview_meta.json").write_text(
        json.dumps(meta_out, indent=2), encoding="utf-8"
    )
    return b_out, r_out, crs


def reproject_overview_folder(
    out_dir: Path,
    meta: dict[str, Any],
    crs: str,
) -> tuple[Path, Path, Optional[Path], str]:
    """Legacy wrapper: vectors only; DEM path unchanged if present."""
    b, r, c = reproject_overview_vectors(out_dir, meta, crs)
    dem = out_dir / "dem_hillshade.tif"
    return b, r, dem if dem.is_file() else None, c
