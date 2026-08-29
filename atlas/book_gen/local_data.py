"""Local-only chapter data: parquet + atlas_work map PNG index (no API/S3)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import geopandas as gpd

from atlas.book_gen.resort_category import resort_size_category
from atlas.map_gen.resort_map_paths import (
    atlas_slug_for_resort,
    filter_gdf_by_region,
    normalize_region,
    resort_work_prefix,
)
from atlas.map_gen.wiki_page_id import wiki_page_id_from_row, wiki_row_from_parquet

from atlas.book_gen.log_util import log


def index_local_maps(work_dir: Path, region: Optional[str]) -> dict[str, Path]:
    """
    Scan ``atlas_work/{region}/`` once for ``*_export.png``.

    Keys: portrait folder basename and base slug (strip ``-layout-*-landscape``).
  """
    prefix = resort_work_prefix(work_dir, region)
    if not prefix.is_dir():
        log(f"  map index: directory not found: {prefix}")
        return {}

    log(f"  map index: scanning {prefix} ...")
    index: dict[str, Path] = {}
    n_files = 0
    for png in prefix.rglob("*_export.png"):
        if not png.is_file():
            continue
        n_files += 1
        if n_files == 1 or n_files % 25 == 0:
            log(f"  map index: found {n_files} PNG(s) so far ...")
        folder = png.parent.name
        index[folder] = png
        # Do not alias base slug -> landscape PNG; that steals portrait lookups.
    log(f"  map index: {n_files} PNG file(s) -> {len(index)} lookup key(s)")
    return index


def pick_map_from_index(
    index: dict[str, Path],
    slug: str,
    *,
    page_id: Optional[str] = None,
    prefer_landscape: bool,
    map_tier: Optional[str] = None,
) -> Optional[Path]:
    if not index:
        return None

    from atlas.book_gen.map_resolver import pick_map_path

    candidates: list[str] = []
    if slug:
        candidates.append(slug)
    if page_id:
        candidates.append(page_id)
        if "-" in page_id:
            # pageId is often {name-slug}-{state-slug}; folders use name slug only
            parts = page_id.rsplit("-", 1)
            if len(parts) == 2 and len(parts[1]) > 2:
                candidates.append(parts[0])

    seen: set[str] = set()
    keys_to_try: list[str] = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            keys_to_try.append(c)

    tier = (map_tier or "").strip().lower() or None
    portrait: Optional[Path] = None
    landscape: Optional[Path] = None
    tier_landscape: Optional[Path] = None

    for key in keys_to_try:
        if key in index and index[key].is_file():
            # Exact folder key is the portrait dir basename.
            if portrait is None and "-layout-" not in key:
                portrait = index[key]
        for idx_key, path in index.items():
            if not path.is_file():
                continue
            if not (idx_key == key or idx_key.startswith(key + "-")):
                continue
            if "-layout-" in idx_key and idx_key.endswith("-landscape"):
                if landscape is None:
                    landscape = path
                if tier and f"-layout-{tier}-landscape" in idx_key:
                    tier_landscape = path
            elif portrait is None and "-layout-" not in idx_key:
                portrait = path

    return pick_map_path(
        {
            "portrait": portrait,
            "landscape": tier_landscape or landscape,
        },
        prefer_landscape=prefer_landscape,
        map_tier=tier,
    )


def load_chapter_from_local_parquet(
    parquet_path: Path,
    *,
    state_filter: str,
    region_filter: Optional[str] = None,
    limit: Optional[int] = None,
    input_dir: Optional[Path] = None,
) -> list[dict[str, Any]]:
    """Single parquet read; each row includes ``slug`` and ``pageId``."""
    import time

    if parquet_path.is_file():
        mb = parquet_path.stat().st_size / (1024 * 1024)
        log(f"  parquet: {parquet_path} ({mb:.1f} MB)")
    else:
        log(f"  parquet: {parquet_path} (missing)")
        return []

    log("  parquet: reading (this can take 30-120s on large files) ...")
    t0 = time.perf_counter()
    try:
        gdf = gpd.read_parquet(parquet_path)
    except ValueError as exc:
        if "geo metadata" not in str(exc).lower():
            raise
        import pandas as pd

        log("  parquet: no geometry metadata, using pandas.read_parquet")
        gdf = pd.read_parquet(parquet_path)
    log(f"  parquet: loaded {len(gdf)} rows in {time.perf_counter() - t0:.1f}s")

    if region_filter:
        log(f"  filter: region={region_filter!r} ...")
        before = len(gdf)
        gdf = filter_gdf_by_region(gdf, region_filter, input_dir=input_dir or parquet_path.parent)
        log(f"  filter: region -> {len(gdf)} rows (from {before})")

    if "Ski Area" in gdf.columns:
        name_col = "Ski Area"
    elif "name" in gdf.columns:
        name_col = "name"
    else:
        name_col = "name"
    state_col = next(
        (c for c in ("state", "State", "addr:state", "province") if c in gdf.columns),
        None,
    )
    log(f"  columns: name={name_col!r}, state={state_col!r}")

    if state_filter and state_col:
        sf = state_filter.strip().casefold()
        before = len(gdf)
        gdf = gdf[gdf[state_col].astype(str).str.strip().str.casefold() == sf]
        log(f"  filter: state={state_filter!r} -> {len(gdf)} rows (from {before})")
    elif state_filter:
        log(f"  filter: state={state_filter!r} skipped (no state column)")

    if limit:
        gdf = gdf.head(limit)
        log(f"  limit: first {limit} rows")

    n_rows = len(gdf)
    log(f"  building {n_rows} resort page dict(s) ...")
    seen_slugs: dict[str, int] = {}
    pages: list[dict[str, Any]] = []
    for i, (_, row) in enumerate(gdf.iterrows(), start=1):
        if i == 1 or i % 10 == 0 or i == n_rows:
            log(f"  parquet rows: {i}/{n_rows}")
        resort_name = str(row.get(name_col) or "").strip()
        if not resort_name:
            continue
        wiki_row = wiki_row_from_parquet(row, name_col=name_col, state_col=state_col)
        page_id = wiki_page_id_from_row(wiki_row)
        slug = atlas_slug_for_resort(resort_name, row, seen_slugs)
        row_d = {k: (v.item() if hasattr(v, "item") else v) for k, v in row.to_dict().items()}
        cat = resort_size_category(row_d)
        pages.append(
            {
                "pageId": page_id,
                "slug": slug,
                "title": resort_name,
                "englishName": row_d.get("english_name"),
                "content": f"# {resort_name}\n\n*Add a description for this resort.*",
                "country": wiki_row.get("country"),
                "state": wiki_row.get("state"),
                "region": row_d.get("region"),
                "resortSizeCategory": cat,
                "pageType": "resort",
                "downhillTrails": row_d.get("downhill_trails"),
                "totalLifts": row_d.get("total_lifts"),
                "downhill_trails": row_d.get("downhill_trails"),
                "total_lifts": row_d.get("total_lifts"),
                "skiableTerrainAcres": row_d.get("skiable_terrain_acres"),
                "skiableTerrainHa": row_d.get("skiable_terrain_ha"),
                "longestTrailMi": row_d.get("longest_trail_mi"),
                "longestLiftMi": row_d.get("longest_lift_mi"),
                "totalAreaAcres": row_d.get("total_area_acres"),
                "totalAreaHa": row_d.get("total_area_ha"),
                "highElevationM": row_d.get("elevation_high_m")
                or row_d.get("high_elevation_m"),
                "lowElevationM": row_d.get("elevation_low_m")
                or row_d.get("low_elevation_m"),
                "resortType": row_d.get("resort_type"),
                "trailsNovice": row_d.get("trails_novice"),
                "trailsEasy": row_d.get("trails_easy"),
                "trailsIntermediate": row_d.get("trails_intermediate"),
                "trailsAdvanced": row_d.get("trails_advanced"),
                "trailsExpert": row_d.get("trails_expert"),
            }
        )
    log(f"  parquet: {len(pages)} resort(s) for state {state_filter!r}")
    return pages
