"""Load wiki resort pages from API and/or parquet fallback."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import requests
import yaml

from atlas.book_gen.resort_category import resort_size_category
from atlas.map_gen.wiki_page_id import wiki_page_id_from_row, wiki_row_from_parquet


def load_book_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_atlas_config(repo_root: Path) -> dict[str, Any]:
    p = repo_root / "config" / "atlas.yaml"
    with p.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def fetch_wiki_index(api_base: str) -> list[dict[str, Any]]:
    url = api_base.rstrip("/") + "/api/wiki/index"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    pages = data.get("pages") if isinstance(data, dict) else data
    if not isinstance(pages, list):
        return []
    return pages


def fetch_wiki_page(api_base: str, page_id: str) -> Optional[dict[str, Any]]:
    url = api_base.rstrip("/") + "/api/wiki/" + requests.utils.quote(page_id, safe="")
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def parquet_path_for_config(book_config: dict[str, Any], repo_root: Path) -> Path:
    input_dir = book_config.get("input_dir", "output/combined")
    p = Path(input_dir)
    if not p.is_absolute():
        p = repo_root / p
    preferred = book_config.get("parquet_file")
    if preferred:
        candidate = Path(preferred)
        if not candidate.is_absolute():
            candidate = repo_root / candidate
        if candidate.is_file():
            return candidate
    for name in ("ski_areas_analyzed.parquet", "ski_areas.parquet"):
        candidate = p / name
        if candidate.is_file():
            return candidate
    return p / "ski_areas.parquet"


def download_parquet(url: str) -> Path:
    print(f"Downloading parquet from {url} …", flush=True)
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    print(f"Downloaded {len(resp.content) / 1e6:.1f} MB", flush=True)
    fd, tmp = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    path = Path(tmp)
    path.write_bytes(resp.content)
    return path


def load_resorts_from_parquet(
    parquet_path: Path,
    *,
    state_filter: Optional[str] = None,
    region_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    gdf = gpd.read_parquet(parquet_path)
    if region_filter and "region" in gdf.columns:
        rn = region_filter.replace("\\", "/").strip("/")
        vals = gdf["region"].astype(str)
        mask = (vals == rn) | vals.str.startswith(rn + "/")
        gdf = gdf.loc[mask]

    name_col = "Ski Area" if "Ski Area" in gdf.columns else "name"
    state_col = None
    for c in ("state", "State", "addr:state", "province"):
        if c in gdf.columns:
            state_col = c
            break

    if state_filter and state_col:
        sf = state_filter.strip().casefold()
        gdf = gdf[
            gdf[state_col].astype(str).str.strip().str.casefold() == sf
        ]

    if limit:
        gdf = gdf.head(limit)

    pages: list[dict[str, Any]] = []
    for _, row in gdf.iterrows():
        row_d = row.to_dict()
        wiki_row = wiki_row_from_parquet(row, name_col=name_col, state_col=state_col)
        page_id = wiki_page_id_from_row(wiki_row)
        name = str(row.get(name_col) or page_id)
        cat = resort_size_category(
            {k: (v if not hasattr(v, "item") else v.item()) for k, v in row_d.items()}
        )
        country = wiki_row.get("country")
        state = wiki_row.get("state")
        pages.append(
            {
                "pageId": page_id,
                "title": name,
                "englishName": row_d.get("english_name"),
                "content": f"# {name}\n\n*Add a description for this resort.*",
                "country": country,
                "state": state,
                "region": row_d.get("region"),
                "resortSizeCategory": cat,
                "pageType": "resort",
                "hidden": False,
                "finished": False,
                "downhillTrails": row_d.get("downhill_trails"),
                "totalLifts": row_d.get("total_lifts"),
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
                "_parquet_row": True,
            }
        )
    return pages


def load_chapter_resorts(
    *,
    api_base: str,
    state: str,
    book: Optional[str] = None,
    parquet_path: Optional[Path] = None,
    parquet_url: Optional[str] = None,
    limit: Optional[int] = None,
    include_only_finished: bool = False,
) -> tuple[list[dict[str, Any]], str]:
    """
    Return full resort page dicts for a state chapter.
    Second value is source label: 'wiki_api' or 'parquet'.
    """
    from atlas.book_gen.log_util import log

    state_cf = state.strip().casefold()
    try:
        log(f"  wiki: GET {api_base.rstrip('/')}/api/wiki/index …")
        index = fetch_wiki_index(api_base)
        log(f"  wiki: index has {len(index)} page(s)")
        out: list[dict[str, Any]] = []
        for stub in index:
            if stub.get("hidden"):
                continue
            pt = stub.get("pageType") or "resort"
            if pt not in ("resort", None, ""):
                continue
            st = (stub.get("state") or "").strip().casefold()
            if st != state_cf:
                continue
            if book:
                stub_book = stub.get("book") or ""
                if stub_book and stub_book != book:
                    continue
            pid = stub.get("pageId")
            if not pid:
                continue
            full = fetch_wiki_page(api_base, pid)
            if not full:
                full = stub
            if full.get("hidden"):
                continue
            if include_only_finished and not full.get("finished"):
                continue
            out.append(full)
            if limit and len(out) >= limit:
                break
        if out:
            return out, "wiki_api"
    except requests.RequestException:
        pass

    pq = parquet_path
    tmp: Optional[Path] = None
    if pq is None and parquet_url:
        tmp = download_parquet(parquet_url)
        pq = tmp
    if pq is None or not pq.is_file():
        raise RuntimeError(
            "Wiki API unavailable and no parquet found. "
            "Set parquet_url in book.yaml or place ski_areas.parquet in output/combined."
        )
    try:
        pages = load_resorts_from_parquet(pq, state_filter=state, limit=limit)
        if include_only_finished:
            pages = [p for p in pages if p.get("finished")]
        return pages, "parquet"
    finally:
        if tmp and tmp.is_file():
            try:
                tmp.unlink()
            except OSError:
                pass
