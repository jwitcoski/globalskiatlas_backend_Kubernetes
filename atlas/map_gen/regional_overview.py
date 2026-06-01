#!/usr/bin/env python3
"""
Prepare country / state regional overview map folders for QGIS.

Exports admin boundary + ski resort points (with map_tier) per unit, then copies
atlas_overview_template.qgz when present.

Usage:
  python -m atlas.map_gen.regional_overview --list
  python -m atlas.map_gen.regional_overview --country "Austria"
  python -m atlas.map_gen.regional_overview --state Pennsylvania --country "United States of America"
  python -m atlas.map_gen.regional_overview --all
  python -m atlas.map_gen.regional_overview --all --data-only
"""
from __future__ import annotations

import argparse
import json
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd

from atlas.book_gen.constants import CATEGORY_TO_MAP_TIER
from atlas.book_gen.resort_category import is_not_downhill, resort_size_category
from atlas.map_gen.data_to_qgis import (
    _replace_maplayer_datasource,
    load_config,
    slugify,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BOUNDARIES_DIR = REPO_ROOT / "boundaries"
COUNTRIES_SHP = BOUNDARIES_DIR / "ne_10m_admin_0_countries.shp"
STATES_SHP = BOUNDARIES_DIR / "ne_10m_admin_1_states_provinces.shp"
TEMPLATE_NAME = "atlas_overview_template.qgz"
TEMPLATE_QGS_NAME = "atlas_overview_template.qgs"
LAYOUT_NAME = "Regional Overview"


def _reconfigure_stdio_utf8() -> None:
    """Avoid UnicodeEncodeError on Windows terminals (cp1252)."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass


def _build_qgz_via_qgis(out_dir: Path) -> None:
    """Build QGZ using QGIS' python-qgis*.bat wrapper (not the venv interpreter)."""
    import subprocess

    runner = REPO_ROOT / "atlas" / "map_gen" / "run_build_overview_qgz.bat"
    if not runner.is_file():
        raise FileNotFoundError(runner)
    subprocess.run(
        [str(runner), "--dir", str(out_dir)],
        cwd=str(REPO_ROOT),
        check=True,
    )


@dataclass(frozen=True)
class OverviewUnit:
    kind: str  # "country" | "state"
    country: str
    state: Optional[str]
    country_slug: str
    state_slug: Optional[str]
    resort_count: int

    @property
    def title_line1(self) -> str:
        if self.kind == "state" and self.state:
            return self.state.upper()
        return self.country.upper()

    @property
    def title_line2(self) -> str:
        return "SKI AREAS"

    def work_dir(self, overview_root: Path) -> Path:
        if self.kind == "state" and self.state_slug:
            return (
                overview_root
                / "states"
                / self.country_slug
                / self.state_slug
            )
        return overview_root / "countries" / self.country_slug

    @property
    def file_slug(self) -> str:
        if self.kind == "state" and self.state_slug:
            return self.state_slug
        return self.country_slug


def _repo_root() -> Path:
    return REPO_ROOT


def _norm(s: Any) -> str:
    if s is None:
        return ""
    t = str(s).strip()
    if t.casefold() in {"nan", "none", ""}:
        return ""
    return t


def _country_col(gdf: gpd.GeoDataFrame) -> str:
    for c in ("country", "Country", "ADMIN"):
        if c in gdf.columns:
            return c
    raise KeyError("No country column in ski areas parquet")


def _state_col(gdf: gpd.GeoDataFrame) -> Optional[str]:
    for c in ("state", "State", "NAME_1"):
        if c in gdf.columns:
            return c
    return None


def _name_col(gdf: gpd.GeoDataFrame) -> str:
    for c in ("name", "Ski Area", "english_name", "Name"):
        if c in gdf.columns:
            return c
    return "name"


def _parquet_to_gdf(parquet_path: Path) -> gpd.GeoDataFrame:
    """Load ski areas — GeoParquet or flat analyzed table with centroid_lat/lon."""
    if not parquet_path.is_file():
        raise FileNotFoundError(parquet_path)
    try:
        gdf = gpd.read_parquet(parquet_path)
    except ValueError as exc:
        if "geo metadata" not in str(exc).lower():
            raise
        import pandas as pd
        from shapely.geometry import Point

        df = pd.read_parquet(parquet_path)
        lon_col = next(
            (c for c in ("centroid_lon", "lon", "longitude", "centroid_x") if c in df.columns),
            None,
        )
        lat_col = next(
            (c for c in ("centroid_lat", "lat", "latitude", "centroid_y") if c in df.columns),
            None,
        )
        if not lon_col or not lat_col:
            raise ValueError(
                f"{parquet_path} has no geometry metadata and no centroid_lon/lat columns"
            ) from exc
        geoms = [
            Point(float(row[lon_col]), float(row[lat_col]))
            if pd.notna(row[lon_col]) and pd.notna(row[lat_col])
            else None
            for _, row in df.iterrows()
        ]
        gdf = gpd.GeoDataFrame(df, geometry=geoms, crs="EPSG:4326")
        gdf = gdf[gdf.geometry.notna()].copy()

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def _load_ski_gdf(parquet_path: Path) -> gpd.GeoDataFrame:
    return _parquet_to_gdf(parquet_path)


def _downhill_mask(gdf: gpd.GeoDataFrame) -> Any:
    mask = []
    for _, row in gdf.iterrows():
        if is_not_downhill(row.to_dict()):
            mask.append(False)
            continue
        mask.append(True)
    return mask


def _map_tier_for_row(row: dict[str, Any]) -> str:
    cat = resort_size_category(row)
    return CATEGORY_TO_MAP_TIER.get(cat, "small")


def _load_boundaries() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if not COUNTRIES_SHP.is_file():
        raise FileNotFoundError(
            f"Missing {COUNTRIES_SHP}. Download Natural Earth 10m admin 0 into boundaries/"
        )
    if not STATES_SHP.is_file():
        raise FileNotFoundError(
            f"Missing {STATES_SHP}. Download Natural Earth 10m admin 1 into boundaries/"
        )
    countries = gpd.read_file(COUNTRIES_SHP)
    states = gpd.read_file(STATES_SHP)
    for g in (countries, states):
        if g.crs is None:
            g.set_crs("EPSG:4326", inplace=True)
        else:
            g.to_crs("EPSG:4326", inplace=True)
    return countries, states


def _admin_match(
    gdf: gpd.GeoDataFrame, column: str, value: str
) -> gpd.GeoDataFrame:
    """Case-insensitive match on admin name column."""
    v = value.strip().casefold()
    col = gdf[column].astype(str).str.strip().str.casefold()
    hit = gdf[col == v]
    if not hit.empty:
        return hit
    # Natural Earth: ADMIN vs NAME_LONG
    for alt in ("ADMIN", "NAME", "NAME_LONG", "name", "NAME_1"):
        if alt not in gdf.columns:
            continue
        col2 = gdf[alt].astype(str).str.strip().str.casefold()
        hit = gdf[col2 == v]
        if not hit.empty:
            return hit
    return gdf.iloc[0:0]


def discover_units(gdf: gpd.GeoDataFrame) -> list[OverviewUnit]:
    country_c = _country_col(gdf)
    state_c = _state_col(gdf)
    mask = _downhill_mask(gdf)
    gdf = gdf[mask].copy()
    if gdf.empty:
        return []

    units: list[OverviewUnit] = []
    seen: set[tuple[str, str, str]] = set()

    for country, grp in gdf.groupby(gdf[country_c].map(_norm)):
        if not country:
            continue
        cslug = slugify(country) or "country"
        key = ("country", country, "")
        if key not in seen:
            seen.add(key)
            units.append(
                OverviewUnit(
                    kind="country",
                    country=country,
                    state=None,
                    country_slug=cslug,
                    state_slug=None,
                    resort_count=len(grp),
                )
            )
        if not state_c:
            continue
        for state, sgrp in grp.groupby(grp[state_c].map(_norm)):
            if not state:
                continue
            sslug = slugify(state) or "state"
            skey = ("state", country, state)
            if skey in seen:
                continue
            seen.add(skey)
            units.append(
                OverviewUnit(
                    kind="state",
                    country=country,
                    state=state,
                    country_slug=cslug,
                    state_slug=sslug,
                    resort_count=len(sgrp),
                )
            )

    units.sort(key=lambda u: (u.kind, u.country, u.state or ""))
    return units


def _resorts_for_unit(
    gdf: gpd.GeoDataFrame, unit: OverviewUnit
) -> gpd.GeoDataFrame:
    country_c = _country_col(gdf)
    state_c = _state_col(gdf)
    mask = _downhill_mask(gdf)
    sub = gdf[mask].copy()
    sub = sub[sub[country_c].map(_norm) == unit.country]
    if unit.kind == "state" and unit.state and state_c:
        sub = sub[sub[state_c].map(_norm) == unit.state]
    return sub


def _boundary_for_unit(
    unit: OverviewUnit,
    countries: gpd.GeoDataFrame,
    states: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    if unit.kind == "state" and unit.state:
        g = _admin_match(states, "name", unit.state)
        if g.empty:
            g = _admin_match(states, "NAME", unit.state)
        if g.empty and "admin" in states.columns:
            g = _admin_match(states, "admin", unit.state)
        # Prefer state in correct country when multiple matches
        if not g.empty and "admin" in g.columns:
            cfold = unit.country.casefold()
            for col in ("ADMIN", "adm0_a3", "sov_a3"):
                if col not in g.columns:
                    continue
            if "name" in countries.columns:
                cmatch = countries[
                    countries["ADMIN"].astype(str).str.strip().str.casefold()
                    == cfold
                ]
                if not cmatch.empty and "iso_a2" in g.columns and "iso_a2" in cmatch.columns:
                    iso = cmatch.iloc[0]["iso_a2"]
                    g2 = g[g["iso_a2"] == iso] if "iso_a2" in g.columns else g.iloc[0:0]
                    if not g2.empty:
                        g = g2
        if len(g) > 1:
            g = g.iloc[[0]]
    else:
        g = _admin_match(countries, "ADMIN", unit.country)
        if g.empty:
            g = _admin_match(countries, "NAME", unit.country)
        if len(g) > 1:
            g = g.iloc[[0]]
    if g.empty:
        raise ValueError(f"No boundary polygon for {unit}")
    return g[["geometry"]].copy()


def _export_resort_points(sub: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    name_c = _name_col(sub)
    rows = []
    for _, row in sub.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        if geom.geom_type != "Point":
            geom = geom.centroid
        d = row.to_dict()
        rows.append(
            {
                "name": _norm(row.get(name_c)) or "Unknown",
                "map_tier": _map_tier_for_row(d),
                "geometry": geom,
            }
        )
    if not rows:
        return gpd.GeoDataFrame(columns=["name", "map_tier"], geometry=[], crs="EPSG:4326")
    return gpd.GeoDataFrame(rows, crs="EPSG:4326")


def _write_meta(unit: OverviewUnit, out_dir: Path) -> None:
    # Preserve existing build metadata (DEM style version, CRS, etc.) across re-runs.
    existing: dict[str, Any] = {}
    meta_path = out_dir / "overview_meta.json"
    if meta_path.is_file():
        try:
            existing = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}

    meta = {
        "kind": unit.kind,
        "country": unit.country,
        "state": unit.state,
        "title_line1": unit.title_line1,
        "title_line2": unit.title_line2,
        "resort_count": unit.resort_count,
        "layout_name": LAYOUT_NAME,
    }
    for k in ("crs", "dem_style_version"):
        if k in existing and k not in meta:
            meta[k] = existing[k]
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def _patch_qgz_content(content: str, out_dir: Path) -> str:
    """Point template layers at ./ files next to the QGZ (relative paths)."""
    content = _replace_maplayer_datasource(
        content, "admin_boundary", "./admin_boundary.geojson"
    )
    content = _replace_maplayer_datasource(
        content, "ski_resorts", "./ski_resorts.geojson"
    )
    # DEM stack (preferred): color base + hillshade overlay
    if (out_dir / "dem_color.tif").is_file():
        content = _replace_maplayer_datasource(content, "dem_color", "./dem_color.tif")
    if (out_dir / "dem_hillshade_overlay.tif").is_file():
        content = _replace_maplayer_datasource(
            content, "dem_hillshade", "./dem_hillshade_overlay.tif"
        )
    elif (out_dir / "dem_hillshade.tif").is_file():
        content = _replace_maplayer_datasource(content, "dem_hillshade", "./dem_hillshade.tif")
    if (out_dir / "dem_mist.tif").is_file():
        content = _replace_maplayer_datasource(content, "dem_mist", "./dem_mist.tif")
    return content


def _write_overview_qgz(
    template_path: Path, out_qgz: Path, out_dir: Path, file_slug: str
) -> None:
    internal_qgs = f"{file_slug}_overview_map.qgs"
    with zipfile.ZipFile(template_path, "r") as zin:
        items = [(info, zin.read(info.filename)) for info in zin.infolist()]

    qgs_names = [info.filename for info, _ in items if info.filename.endswith(".qgs")]
    src_qgs = (
        TEMPLATE_QGS_NAME
        if TEMPLATE_QGS_NAME in qgs_names
        else (qgs_names[0] if qgs_names else TEMPLATE_QGS_NAME)
    )

    with zipfile.ZipFile(out_qgz, "w", zipfile.ZIP_DEFLATED) as zout:
        for info, data in items:
            if info.filename == src_qgs:
                text = _patch_qgz_content(data.decode("utf-8"), out_dir)
                data = text.encode("utf-8")
                info.filename = internal_qgs
            zout.writestr(info, data)


def export_unit(
    unit: OverviewUnit,
    gdf: gpd.GeoDataFrame,
    countries: gpd.GeoDataFrame,
    states: gpd.GeoDataFrame,
    overview_root: Path,
    template_path: Optional[Path],
    *,
    data_only: bool = False,
) -> Path:
    out_dir = unit.work_dir(overview_root)
    out_dir.mkdir(parents=True, exist_ok=True)

    sub = _resorts_for_unit(gdf, unit)
    if sub.empty:
        raise ValueError(f"No resorts for {unit}")

    boundary = _boundary_for_unit(unit, countries, states)
    boundary.to_file(out_dir / "admin_boundary.geojson", driver="GeoJSON")

    points = _export_resort_points(sub)
    points.to_file(out_dir / "ski_resorts.geojson", driver="GeoJSON")

    _write_meta(unit, out_dir)
    meta_on_disk = json.loads(
        (out_dir / "overview_meta.json").read_text(encoding="utf-8")
    )

    dem_path = out_dir / "dem_hillshade.tif"
    dem_color = out_dir / "dem_color.tif"
    dem_overlay = out_dir / "dem_hillshade_overlay.tif"
    dem_mist = out_dir / "dem_mist.tif"
    try:
        from atlas.map_gen.overview_crs import overview_projected_crs

        unit_meta = {
            "kind": unit.kind,
            "country": unit.country,
            "state": unit.state,
        }
        map_crs = overview_projected_crs(unit_meta, boundary)

        # Ensure meta carries CRS/style so downstream QGZ builder can avoid rebuilds.
        meta_dirty = False
        if meta_on_disk.get("crs") != map_crs:
            meta_on_disk["crs"] = map_crs
            meta_dirty = True
        if dem_color.is_file() and dem_overlay.is_file() and dem_mist.is_file():
            if int(meta_on_disk.get("dem_style_version") or 0) < 5:
                meta_on_disk["dem_style_version"] = 5
                meta_dirty = True
        elif dem_color.is_file() and dem_overlay.is_file():
            if int(meta_on_disk.get("dem_style_version") or 0) < 4:
                meta_on_disk["dem_style_version"] = 4
                meta_dirty = True
        if meta_dirty:
            (out_dir / "overview_meta.json").write_text(
                json.dumps(meta_on_disk, indent=2), encoding="utf-8"
            )

        # If already built for this CRS + current style, don't touch (avoids Windows file locks).
        crs_ok = meta_on_disk.get("crs") == map_crs or not meta_on_disk.get("crs")
        style_version = int(meta_on_disk.get("dem_style_version") or 0)
        if dem_color.is_file() and dem_overlay.is_file():
            dem_ok = dem_mist.is_file() and style_version >= 5 and crs_ok
        else:
            dem_ok = dem_path.is_file() and crs_ok

        if not dem_ok:
            import os
            import subprocess

            # Remove QGIS/OSGeo4W from PATH so rasterio/GDAL doesn't pick up plugin paths.
            env = os.environ.copy()
            parts = [
                p
                for p in env.get("PATH", "").split(os.pathsep)
                if p and "qgis" not in p.lower() and "osgeo4w" not in p.lower()
            ]
            env["PATH"] = os.pathsep.join(parts)

            print(f"  Building DEM layers ({map_crs}) for {unit.title_line1}...", flush=True)
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "atlas.map_gen.overview_dem",
                    str(out_dir),
                    "--crs",
                    map_crs,
                ],
                cwd=str(REPO_ROOT),
                check=True,
                env=env,
            )
            # overview_dem updates overview_meta.json (incl dem_style_version); reload it.
            meta_on_disk = json.loads(
                (out_dir / "overview_meta.json").read_text(encoding="utf-8")
            )
        stale = out_dir / "dem_hillshade_proj.tif"
        if stale.is_file():
            stale.unlink()
    except Exception as exc:
        print(f"  Warning: DEM hillshade skipped ({exc})", file=sys.stderr)

    if data_only:
        return out_dir

    slug = unit.file_slug
    out_qgz = out_dir / f"{slug}_overview_map.qgz"
    if template_path is not None and template_path.is_file():
        _write_overview_qgz(template_path, out_qgz, out_dir, slug)
    else:
        # Avoid importing PyQGIS into the venv interpreter (No module named 'qgis').
        _build_qgz_via_qgis(out_dir)
    return out_dir


def main() -> int:
    _reconfigure_stdio_utf8()
    ap = argparse.ArgumentParser(description="Regional overview map data + QGZ")
    ap.add_argument("--config", type=Path, default=_repo_root() / "config" / "atlas.yaml")
    ap.add_argument("--parquet", type=Path, help="ski_areas parquet (default: config input_dir)")
    ap.add_argument("--work-dir", type=Path, help="overview root (default: config overview.work_dir)")
    ap.add_argument("--list", action="store_true", help="List units and exit")
    ap.add_argument("--all", action="store_true", help="Export every country and state")
    ap.add_argument("--country", type=str, help="Country name (Natural Earth ADMIN)")
    ap.add_argument("--state", type=str, help="State name (with --country)")
    ap.add_argument("--data-only", action="store_true", help="GeoJSON only, no QGZ copy")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    cfg = load_config(args.config)
    ov = cfg.get("overview") or {}
    input_dir = Path(cfg.get("input_dir") or "output/combined")
    if not input_dir.is_absolute():
        input_dir = _repo_root() / input_dir
    parquet = args.parquet or input_dir / "ski_areas_analyzed.parquet"
    if not parquet.is_file():
        parquet = input_dir / "ski_areas.parquet"
    overview_root = args.work_dir or Path(ov.get("work_dir") or "atlas_work/overview")
    if not overview_root.is_absolute():
        overview_root = _repo_root() / overview_root

    template_rel = (cfg.get("template") or {}).get("overview") or (
        f"atlas/map_gen/templates/{TEMPLATE_NAME}"
    )
    template_path = _repo_root() / template_rel

    try:
        gdf = _load_ski_gdf(parquet)
        countries, states = _load_boundaries()
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 1

    units = discover_units(gdf)

    if args.list:
        for u in units:
            loc = u.state or "(country)"
            print(f"{u.kind}\t{u.country}\t{loc}\t{u.resort_count}\t{u.work_dir(overview_root)}")
        print(f"Total: {len(units)} units", file=sys.stderr)
        return 0

    if args.all:
        selected = units
    elif args.country:
        c = args.country.strip()
        if args.state:
            s = args.state.strip()
            selected = [
                u
                for u in units
                if u.country.casefold() == c.casefold()
                and u.state
                and u.state.casefold() == s.casefold()
            ]
        else:
            selected = [
                u
                for u in units
                if u.kind == "country" and u.country.casefold() == c.casefold()
            ]
            if not selected:
                selected = [u for u in units if u.country.casefold() == c.casefold()]
    else:
        ap.print_help()
        return 1

    if args.limit and args.limit > 0:
        selected = selected[: args.limit]

    if not selected:
        print("No matching units.", file=sys.stderr)
        return 1

    if not args.data_only and not template_path.is_file():
        print(
            f"No template at {template_path}; building .qgz with PyQGIS (requires QGIS install).",
            file=sys.stderr,
        )

    n = 0
    for unit in selected:
        try:
            export_unit(
                unit,
                gdf,
                countries,
                states,
                overview_root,
                template_path,
                data_only=args.data_only,
            )
            print(f"OK {unit.kind} {unit.country}" + (f" / {unit.state}" if unit.state else ""))
            n += 1
        except Exception as e:
            print(f"FAIL {unit}: {e}", file=sys.stderr)

    print(f"Done: {n}/{len(selected)}", file=sys.stderr)
    return 0 if n == len(selected) else 1


if __name__ == "__main__":
    sys.exit(main())
