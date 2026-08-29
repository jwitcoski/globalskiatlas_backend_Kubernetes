"""Resolve portrait/landscape map PNG paths for book layout."""

from __future__ import annotations

import re
import struct
import urllib.parse
from pathlib import Path
from typing import Any, Optional

import requests

from atlas.book_gen.constants import CATEGORY_TO_MAP_TIER
from atlas.map_gen.resort_map_paths import resolve_export_paths

# Folder basename like ``slug-layout-medium-landscape``.
_LANDSCAPE_DIR_RE = re.compile(
    r"^(?P<base>.+)-layout-(?P<tier>small|medium|large|mega)-landscape$",
    re.IGNORECASE,
)


def map_tier_for_category(category: str) -> str:
    return CATEGORY_TO_MAP_TIER.get(category, "small")


def prefer_landscape_for_slot(slot: str) -> bool:
    """Locked orientation: large/half → landscape; mega spread → portrait."""
    return slot in ("full", "half")


def _png_aspect(path: Path) -> tuple[int, int] | None:
    try:
        with path.open("rb") as f:
            if f.read(8) != b"\x89PNG\r\n\x1a\n":
                return None
            f.read(8)  # length + IHDR
            ihdr = f.read(8)
            if len(ihdr) < 8:
                return None
            w, h = struct.unpack(">II", ihdr)
            if w <= 0 or h <= 0:
                return None
            return int(w), int(h)
    except OSError:
        return None


def _is_wider(path: Path) -> bool | None:
    dims = _png_aspect(path)
    if dims is None:
        return None
    w, h = dims
    return w >= h


def _landscape_tier_from_path(path: Path) -> str | None:
    m = _LANDSCAPE_DIR_RE.match(path.parent.name)
    return m.group("tier").lower() if m else None


def pick_map_path(
    paths: dict[str, Optional[Path]],
    *,
    prefer_landscape: bool,
    map_tier: str | None = None,
) -> Optional[Path]:
    """
    Choose portrait/landscape PNG.

    When *prefer_landscape*, prefer a wider-than-tall image (and same-tier
    landscape folder when possible). Note: for ``large``, the folder named
    ``*-layout-large-landscape`` is the *tall* swapped plate — aspect wins.
    """
    portrait = paths.get("portrait")
    landscape = paths.get("landscape")
    candidates: list[Path] = []
    for p in (landscape, portrait):
        if p is not None and p.is_file() and p not in candidates:
            candidates.append(p)
    if not candidates:
        return None

    tier = (map_tier or "").strip().lower() or None

    def score(path: Path) -> tuple:
        wider = _is_wider(path)
        path_tier = _landscape_tier_from_path(path)
        same_tier = 1 if tier and path_tier == tier else 0
        is_portrait_dir = path_tier is None
        if prefer_landscape:
            # Prefer wider aspect, then same-tier landscape folder, then any file.
            aspect_ok = 1 if wider is True else (0 if wider is False else -1)
            # Penalize tall "*-large-landscape" when we want wide large plates.
            tall_named = 1 if path_tier == "large" and wider is False else 0
            return (aspect_ok, same_tier, -tall_named, 0 if is_portrait_dir else 1)
        # Portrait preference (mega): plain portrait dir first, then taller aspect.
        aspect_ok = 1 if wider is False else (0 if wider is True else -1)
        return (1 if is_portrait_dir else 0, aspect_ok, same_tier)

    return max(candidates, key=score)


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
    chosen = pick_map_path(
        paths, prefer_landscape=prefer_landscape, map_tier=tier
    )
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
        path_tier = _landscape_tier_from_path(local)
        if path_tier and path_tier != tier:
            warnings.append(
                f"map fallback: using {path_tier} plate for {tier} slot ({local.parent.name})"
            )
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
