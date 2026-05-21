"""Resolve atlas_work export PNG paths for portrait and landscape layouts.

Work folders mirror ``output/{region}/`` (e.g. ``atlas_work/north-america/us/virginia/{slug}/``)
so regional batches stay navigable at scale.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, Optional

import geopandas as gpd

from atlas.map_gen.data_to_qgis import (
    _landscape_pair_for_print_tier,
    _resolve_layout_tier,
    _safe_slug_fallback,
    slugify,
)


def normalize_region(region: Optional[str]) -> Optional[str]:
    """Region path as in output/ and ski_areas.parquet (forward slashes)."""
    region = str(region or "").strip()
    if not region or region.casefold() in {"nan", "none"}:
        return None
    return region.replace("\\", "/").strip("/")


def resort_work_prefix(work_dir: Path, region: Optional[str]) -> Path:
    """Directory under work_dir matching ``output/{region}/`` layout."""
    r = normalize_region(region)
    if r:
        return work_dir / r
    return work_dir


def slug_scope_key(region: Optional[str], slug: str) -> str:
    r = normalize_region(region)
    return f"{r}/{slug}" if r else slug


def atlas_slug_for_resort(resort_name: str, row: Any, seen_slugs: dict[str, int]) -> str:
    """Same slug rules as data_to_qgis.run_resorts (folder basename for portrait)."""
    region = None
    if hasattr(row, "get"):
        region = normalize_region(str(row.get("region") or ""))
    base = slugify(resort_name) or _safe_slug_fallback(row)
    key = slug_scope_key(region, base)
    if key in seen_slugs:
        seen_slugs[key] += 1
        return f"{base}-{seen_slugs[key]}"
    seen_slugs[key] = 0
    return base


def layout_tier_for_resort(
    resort_name: str,
    n_trails: int,
    tiers_cfg: dict[str, Any],
) -> str:
    return _resolve_layout_tier(resort_name, n_trails, tiers_cfg, override=None)


def export_png_path(resort_dir: Path) -> Path:
    """PNG next to {basename}_map.qgz from export_layouts / data_to_qgis."""
    return resort_dir / f"{resort_dir.name}_export.png"


def portrait_dir(work_dir: Path, slug: str, region: Optional[str] = None) -> Path:
    return resort_work_prefix(work_dir, region) / slug


def landscape_dir(
    work_dir: Path, slug: str, portrait_tier: str, region: Optional[str] = None
) -> Path:
    return resort_work_prefix(work_dir, region) / f"{slug}-layout-{portrait_tier}-landscape"


def resolve_export_paths(
    work_dir: Path,
    slug: str,
    portrait_tier: str,
    *,
    config: dict[str, Any],
    region: Optional[str] = None,
) -> dict[str, Optional[Path]]:
    """Return portrait and landscape export paths when those layouts were generated."""
    portrait = export_png_path(portrait_dir(work_dir, slug, region))
    landscape: Optional[Path] = None
    ls = _landscape_pair_for_print_tier(portrait_tier)
    if ls:
        from atlas.map_gen.data_to_qgis import _template_qgz_for_tier

        if _template_qgz_for_tier(config, ls).exists():
            landscape = export_png_path(
                landscape_dir(work_dir, slug, portrait_tier, region)
            )
    return {"portrait": portrait, "landscape": landscape}


def qgz_path_for_dir(resort_dir: Path, *, project_slug: Optional[str] = None) -> Path:
    stem = project_slug or resort_dir.name
    path = resort_dir / f"{stem}_map.qgz"
    if path.is_file():
        return path
    alt = resort_dir / f"{resort_dir.name}_map.qgz"
    return alt if alt.is_file() else path


def iter_expected_qgz_paths(
    work_dir: Path,
    input_dir: Path,
    config: dict[str, Any],
    *,
    region_filter: Optional[str] = None,
    resort_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> Iterator[Path]:
    """QGZ paths for resorts matching the same filters as run_resorts / upload."""
    ski_areas_path = input_dir / "ski_areas.parquet"
    if not ski_areas_path.exists():
        return

    gdf = gpd.read_parquet(ski_areas_path)
    if region_filter and "region" in gdf.columns:
        gdf = gdf[gdf["region"] == region_filter].copy()
    if resort_id:
        for id_col in ("winter_sports_id", "osm_way_id", "osm_id"):
            if id_col in gdf.columns:
                gdf = gdf[gdf[id_col].astype(str) == resort_id].copy()
                break
    if limit:
        gdf = gdf.head(limit)

    name_col = "Ski Area" if "Ski Area" in gdf.columns else "name"
    pistes_all = None
    pistes_path = input_dir / "pistes.parquet"
    if pistes_path.exists():
        pistes_all = gpd.read_parquet(pistes_path)

    tiers_cfg = config.get("trail_tiers") or {}
    seen_slugs: dict[str, int] = {}

    for _, row in gdf.iterrows():
        resort_name = str(row.get(name_col) or "").strip()
        if not resort_name:
            continue
        region = normalize_region(str(row.get("region") or ""))
        slug = atlas_slug_for_resort(resort_name, row, seen_slugs)
        n_trails = -1
        if pistes_all is not None and name_col in pistes_all.columns:
            n_trails = len(pistes_all[pistes_all[name_col] == resort_name])
        tier = layout_tier_for_resort(resort_name, n_trails, tiers_cfg)

        yield qgz_path_for_dir(portrait_dir(work_dir, slug, region))
        ls = _landscape_pair_for_print_tier(tier)
        if ls:
            from atlas.map_gen.data_to_qgis import _template_qgz_for_tier

            if _template_qgz_for_tier(config, ls).exists():
                yield qgz_path_for_dir(landscape_dir(work_dir, slug, tier, region))
