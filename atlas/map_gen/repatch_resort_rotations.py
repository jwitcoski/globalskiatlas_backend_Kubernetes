#!/usr/bin/env python3
"""
Re-apply map rotation + extent to existing *_map.qgz under atlas_work (no full regenerate).

Use after fixing ski_north_angle lookup / mapcanvas rotation patching, e.g.:
  python scripts/merge_elevation_into_analyzed.py -d output/north-america/us/colorado
  python -m atlas.map_gen.repatch_resort_rotations --region north-america/us/colorado
  atlas\\map_gen\\run_export_layouts.bat   # re-export PNGs
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd

from atlas.map_gen.data_to_qgis import (
    TEMPLATE_QGS_NAME,
    _resolve_layout_tier,
    _ski_north_angle_for_row,
    load_config,
    patch_qgs,
    slugify,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _layout_tier_from_dir(dir_name: str, n_trails: int, tiers_cfg: dict) -> str:
    m = re.search(r"-layout-(\w+)-landscape$", dir_name)
    if m:
        return f"{m.group(1)}_landscape"
    return _resolve_layout_tier(dir_name, n_trails, tiers_cfg, override=None)


def _portrait_slug(dir_name: str) -> str:
    return re.sub(r"-layout-(?:small|medium|large|mega)-landscape$", "", dir_name)


def _row_for_resort_dir(
    resort_dir: Path,
    input_dir: Path,
    region_filter: str,
) -> tuple[Any, str]:
    """Resolve full ski_areas row from regional parquet (per-resort data/ is geometry-only)."""
    regional_path = input_dir / "ski_areas.parquet"
    slug = _portrait_slug(resort_dir.name)
    resort_name = slug.replace("-", " ").title()

    if regional_path.is_file():
        regional = gpd.read_parquet(regional_path)
        name_col = "Ski Area" if "Ski Area" in regional.columns else "name"
        if name_col in regional.columns:
            slugs = regional[name_col].astype(str).map(
                lambda n: slugify(str(n)) if str(n).strip() not in ("", "nan") else ""
            )
            hit = regional[slugs == slug]
            if hit.empty:
                hit = regional[
                    regional[name_col]
                    .astype(str)
                    .str.contains(slug.replace("-", " "), case=False, na=False)
                ]
            if not hit.empty:
                row = hit.iloc[0].copy()
                resort_name = str(row.get(name_col) or resort_name).strip()
                row["region"] = region_filter
                return row, resort_name

    ski_path = resort_dir / "data" / "ski_areas.parquet"
    if ski_path.is_file():
        local = gpd.read_parquet(ski_path)
        if not local.empty:
            row = local.iloc[0].copy()
            row["region"] = region_filter
            return row, resort_name

    raise ValueError(f"No regional ski_areas match for folder slug {slug!r}")


def _state_name_from_row(row: Any) -> str:
    for col in ("State", "state"):
        if hasattr(row, "get") and col in row.index:
            v = str(row.get(col) or "").strip()
            if v and v.casefold() not in {"nan", "none"}:
                return v
    return "Colorado"


def repatch_qgz(
    qgz_path: Path,
    *,
    input_dir: Path,
    config: dict[str, Any],
    region_filter: str,
) -> bool:
    data_dir = qgz_path.parent / "data"
    buffer_path = data_dir / "ski_areas_1000ft_buffer.parquet"
    row, resort_name = _row_for_resort_dir(qgz_path.parent, input_dir, region_filter)

    buffer_bounds: Optional[tuple[float, float, float, float]] = None
    if buffer_path.is_file():
        buf = gpd.read_parquet(buffer_path)
        if not buf.empty:
            buffer_bounds = tuple(buf.total_bounds)

    pistes_path = data_dir / "pistes.parquet"
    n_trails = -1
    if pistes_path.is_file():
        pistes = gpd.read_parquet(pistes_path)
        n_trails = len(pistes)

    layout_tier = _layout_tier_from_dir(
        qgz_path.parent.name,
        n_trails,
        config.get("trail_tiers") or {},
    )
    ski_north = _ski_north_angle_for_row(input_dir, row, resort_name)
    rotation = (360.0 - ski_north) % 360.0 if ski_north is not None else 0.0

    geom = row.geometry
    centroid = geom.centroid if hasattr(geom, "centroid") else geom
    state_name = _state_name_from_row(row)
    inset_country = str(row.get("Country") or "").strip() or None

    with zipfile.ZipFile(qgz_path, "r") as zin:
        items = [(info, zin.read(info.filename)) for info in zin.infolist()]

    qgs_name = next((info.filename for info, _ in items if info.filename.endswith(".qgs")), None)
    if not qgs_name:
        print(f"  skip (no .qgs): {qgz_path}")
        return False

    patched = patch_qgs(
        next(data for info, data in items if info.filename == qgs_name).decode("utf-8"),
        resort_name,
        state_name,
        buffer_bounds,
        ski_north,
        centroid_lon=float(centroid.x),
        centroid_lat=float(centroid.y),
        inset_country_raw=inset_country,
        layout_tier=layout_tier,
        resort_dir=qgz_path.parent,
    )

    with zipfile.ZipFile(qgz_path, "w", zipfile.ZIP_DEFLATED) as zout:
        for info, data in items:
            if info.filename == qgs_name or info.filename == TEMPLATE_QGS_NAME:
                zout.writestr(info.filename, patched.encode("utf-8"))
            else:
                zout.writestr(info, data)

    ang_str = f"{ski_north:.1f}" if ski_north is not None else "none"
    print(f"  {qgz_path.parent.name}: ski_north={ang_str}  map_rotation={rotation:.1f}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-patch map rotation on existing resort QGZ files")
    parser.add_argument("--region", required=True, help="e.g. north-america/us/colorado")
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--input-dir", type=Path, default=None)
    args = parser.parse_args()

    root = _repo_root()
    config = load_config()
    region = args.region.strip().replace("\\", "/")
    work_dir = args.work_dir or Path(config.get("work_dir", "atlas_work"))
    input_dir = args.input_dir or Path(f"output/{region}")
    if not work_dir.is_absolute():
        work_dir = root / work_dir
    if not input_dir.is_absolute():
        input_dir = root / input_dir

    prefix = work_dir / region
    if not prefix.is_dir():
        print(f"Missing {prefix}", file=sys.stderr)
        return 1

    qgz_files = sorted(
        p for p in prefix.rglob("*_map.qgz") if p.is_file()
    )
    if not qgz_files:
        print(f"No *_map.qgz under {prefix}", file=sys.stderr)
        return 1

    ok = 0
    failed = 0
    for qgz in qgz_files:
        try:
            if repatch_qgz(qgz, input_dir=input_dir, config=config, region_filter=region):
                ok += 1
        except Exception as e:
            failed += 1
            print(f"  ERROR {qgz.parent.name}: {e}", file=sys.stderr)
    print(f"\nRepatched {ok}/{len(qgz_files)} QGZ(s), {failed} failed. Re-export layouts to refresh PNGs.")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
