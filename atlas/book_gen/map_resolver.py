"""Resolve portrait/landscape map PNG paths for book layout."""

from __future__ import annotations

import urllib.parse
from pathlib import Path
from typing import Any, Optional

import requests

from atlas.book_gen.constants import CATEGORY_TO_MAP_TIER
from atlas.map_gen.resort_map_paths import resolve_export_paths


def map_tier_for_category(category: str) -> str:
    return CATEGORY_TO_MAP_TIER.get(category, "small")


def pick_map_path(
    paths: dict[str, Optional[Path]],
    *,
    prefer_landscape: bool,
) -> Optional[Path]:
    portrait = paths.get("portrait")
    landscape = paths.get("landscape")
    if prefer_landscape:
        if landscape and landscape.is_file():
            return landscape
        if portrait and portrait.is_file():
            return portrait
    else:
        if portrait and portrait.is_file():
            return portrait
        if landscape and landscape.is_file():
            return landscape
    return None


def resolve_local_map(
    work_dir: Path,
    slug: str,
    category: str,
    config: dict[str, Any],
    *,
    region: Optional[str] = None,
    prefer_landscape: bool = False,
) -> tuple[Optional[Path], str]:
    tier = map_tier_for_category(category)
    paths = resolve_export_paths(
        work_dir, slug, tier, config=config, region=region
    )
    chosen = pick_map_path(paths, prefer_landscape=prefer_landscape)
    return chosen, tier


def download_map(
    page_id: str,
    dest: Path,
    *,
    s3_base: str,
    orientation: str = "portrait",
) -> bool:
    enc = urllib.parse.quote(page_id, safe="")
    url = f"{s3_base.rstrip('/')}/{enc}-{orientation}.png"
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            if orientation == "portrait":
                legacy = f"{s3_base.rstrip('/')}/{enc}.png"
                resp = requests.get(legacy, timeout=60)
            if resp.status_code != 200:
                return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(resp.content)
        return True
    except requests.RequestException:
        return False


def ensure_map_file(
    page_id: str,
    slug: str,
    category: str,
    work_dir: Path,
    atlas_config: dict[str, Any],
    book_config: dict[str, Any],
    *,
    region: Optional[str] = None,
    prefer_landscape: bool = False,
    cache_dir: Optional[Path] = None,
    local_only: bool = False,
) -> tuple[Optional[str], str, list[str]]:
    """Return absolute map path string, tier, and warnings."""
    warnings: list[str] = []
    local, tier = resolve_local_map(
        work_dir,
        slug,
        category,
        atlas_config,
        region=region,
        prefer_landscape=prefer_landscape,
    )
    if local and local.is_file():
        return str(local.resolve()), tier, warnings

    if local_only or book_config.get("local_only"):
        warnings.append("no local map PNG in atlas_work")
        return None, tier, warnings

    cache = cache_dir or (work_dir / "book" / "_map_cache")
    orient = "landscape" if prefer_landscape else "portrait"
    dest = cache / f"{page_id}-{orient}.png"
    if dest.is_file():
        return str(dest.resolve()), tier, warnings

    s3_base = book_config.get(
        "maps_s3_base",
        "https://globalskiatlas-resort-maps.s3.us-east-1.amazonaws.com",
    )
    if download_map(page_id, dest, s3_base=s3_base, orientation=orient):
        return str(dest.resolve()), tier, warnings

    alt = "portrait" if orient == "landscape" else "landscape"
    dest_alt = cache / f"{page_id}-{alt}.png"
    if download_map(page_id, dest_alt, s3_base=s3_base, orientation=alt):
        warnings.append(f"used {alt} map (preferred {orient} missing)")
        return str(dest_alt.resolve()), tier, warnings

    warnings.append("no map PNG found locally or on S3")
    return None, tier, warnings
