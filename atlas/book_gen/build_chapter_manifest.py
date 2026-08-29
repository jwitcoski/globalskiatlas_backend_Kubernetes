#!/usr/bin/env python3
"""Build chapter manifest.json for Scribus book generation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from atlas.book_gen.local_data import (
    index_local_maps,
    load_chapter_from_local_parquet,
    pick_map_from_index,
)
from atlas.book_gen.map_resolver import (
    map_tier_for_category,
    prefer_landscape_for_slot,
    resolve_local_map,
)
from atlas.book_gen.render_resort_fields import build_scribus_fields
from atlas.book_gen.resort_category import page_fraction, slot_for_fraction
from atlas.book_gen.log_util import log, log_phase
from atlas.book_gen.wiki_client import (
    load_atlas_config,
    load_book_config,
    load_chapter_resorts,
    parquet_path_for_config,
)
from atlas.book_gen.wiki_content_store import WikiContentStore


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def build_manifest(
    *,
    state: str,
    region: str | None,
    book: str | None,
    book_config: dict[str, Any],
    atlas_config: dict[str, Any],
    repo_root: Path,
    limit: int | None,
    api_base: str,
    parquet_only: bool,
    local_only: bool = True,
    no_maps: bool = False,
) -> tuple[list[dict[str, Any]], str]:
    parquet_path = parquet_path_for_config(book_config, repo_root)
    work_dir = Path(book_config.get("work_dir", "atlas_work"))
    if not work_dir.is_absolute():
        work_dir = repo_root / work_dir

    use_local = local_only or parquet_only
    log(
        f"Manifest: state={state!r} region={region!r} "
        f"local_only={use_local} no_maps={no_maps} require_map={book_config.get('require_map', True)}"
    )
    log(f"  parquet path: {parquet_path}")
    log(f"  work_dir: {work_dir}")

    if use_local:
        if not parquet_path.is_file():
            raise RuntimeError(
                f"Local parquet not found: {parquet_path}\n"
                "Place ski_areas.parquet under output/combined/ (see config/book.yaml)."
            )
        with log_phase("Load resorts from local parquet"):
            pages = load_chapter_from_local_parquet(
                parquet_path,
                state_filter=state,
                region_filter=region,
                limit=limit,
                input_dir=parquet_path.parent,
            )
        source = "local_parquet"
    else:
        log("Manifest: trying wiki API (may be slow if server is down) ...")
        parquet_url = book_config.get("parquet_url") if not parquet_path.is_file() else None
        with log_phase("Load resorts from wiki API / remote parquet"):
            pages, source = load_chapter_resorts(
                api_base=api_base,
                state=state,
                book=book,
                parquet_path=parquet_path if parquet_path.is_file() else None,
                parquet_url=parquet_url,
                limit=limit,
                include_only_finished=bool(book_config.get("include_only_finished")),
            )

    wiki_store = WikiContentStore.from_config(repo_root, book_config)
    with log_phase("Merge Bedrock wiki copy"):
        merged, missing = wiki_store.apply_to_pages(pages)
        log(f"  bedrock copy: {merged}/{len(pages)} resorts ({missing} without copy)")

    map_index: dict[str, Path] = {}
    if not no_maps and region:
        with log_phase("Index local map PNGs"):
            map_index = index_local_maps(work_dir, region)
    elif no_maps:
        log("Skipping map index (--no-maps)")

    manifest: list[dict[str, Any]] = []
    require_map = bool(book_config.get("require_map", True))
    skipped_no_map = 0
    skipped_category = 0
    n_pages = len(pages)

    log(f"Matching {n_pages} resort(s) to local maps ...")
    for i, page in enumerate(pages):
        if i == 0 or (i + 1) % 5 == 0 or (i + 1) == n_pages:
            log(f"  manifest progress: {i + 1}/{n_pages} (included so far: {len(manifest)})")
        cat = page.get("resortSizeCategory") or "unknown"
        frac = page_fraction(cat)
        slot = slot_for_fraction(frac)
        if slot == "skip":
            skipped_category += 1
            log(f"  skip (category={cat!r}): {page.get('pageId')} ({page.get('title')})")
            continue

        pid = page.get("pageId") or f"resort-{i}"
        slug = page.get("slug") or pid
        row_region = page.get("region")
        if isinstance(row_region, str):
            row_region = row_region.replace("\\", "/").strip("/") or None

        # Large/half → landscape (wider); mega spread → portrait (taller).
        prefer_landscape = prefer_landscape_for_slot(slot)
        map_path: str | None = None
        map_tier = map_tier_for_category(cat)
        warnings: list[str] = []

        if not no_maps:
            chosen = pick_map_from_index(
                map_index,
                slug,
                page_id=pid,
                prefer_landscape=prefer_landscape,
                map_tier=map_tier,
            )
            if chosen is None:
                local, map_tier = resolve_local_map(
                    work_dir,
                    slug,
                    cat,
                    atlas_config,
                    region=row_region or region,
                    prefer_landscape=prefer_landscape,
                )
                chosen = local
            if chosen and chosen.is_file():
                map_path = str(chosen.resolve())
                # Warn when a different plate tier is used as fallback.
                parent = chosen.parent.name
                if "-layout-" in parent and f"-layout-{map_tier}-" not in parent:
                    warnings.append(
                        f"map fallback: {parent} for {map_tier} slot"
                    )
            else:
                warnings.append("no local map PNG in atlas_work")

        if require_map and not map_path and not no_maps:
            skipped_no_map += 1
            if skipped_no_map <= 5 or skipped_no_map % 20 == 0:
                log(f"  skip (no local map): {pid} slug={slug}")
            continue

        fields = build_scribus_fields(page, page_num=i + 1)
        manifest.append(
            {
                "pageId": pid,
                "slug": slug,
                "title": fields["title"],
                "country": page.get("country"),
                "state": page.get("state"),
                "region": row_region or region,
                "resortSizeCategory": cat,
                "pageFraction": frac,
                "slot": slot,
                "mapPath": map_path,
                "mapTier": map_tier,
                "warnings": warnings,
                "scribusFields": fields,
                "sortKey": f"{page.get('country')}|{page.get('state')}|{fields['title']}",
            }
        )

    log(
        f"Manifest complete: {len(manifest)} included, "
        f"{skipped_no_map} skipped (no map), "
        f"{skipped_category} skipped (category unknown), source={source}"
    )
    return manifest, source


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Scribus chapter manifest")
    parser.add_argument("--state", required=True, help="US state name (e.g. Virginia)")
    parser.add_argument(
        "--region",
        default="north-america/us/virginia",
        help="Parquet/atlas_work region path",
    )
    parser.add_argument("--book", default=None, help="Book volume filter (Americas, etc.)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Default atlas_work/book/{state_slug}/",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        default=None,
        help="Use only local parquet + atlas_work maps (default from book.yaml)",
    )
    parser.add_argument(
        "--parquet-only",
        action="store_true",
        help="Alias for --local-only (deprecated)",
    )
    parser.add_argument("--no-maps", action="store_true", help="SLA without map images")
    parser.add_argument("--wiki-api-base", default=None)
    args = parser.parse_args()

    repo_root = _repo_root()
    book_config = load_book_config(repo_root / "atlas" / "book_gen" / "config" / "book.yaml")
    atlas_config = load_atlas_config(repo_root)

    local_only = book_config.get("local_only", True)
    if args.local_only is True or args.parquet_only:
        local_only = True
    if args.local_only is False:
        local_only = False

    if args.wiki_api_base:
        book_config["wiki_api_base"] = args.wiki_api_base

    state_slug = args.state.strip().lower().replace(" ", "-")
    out_dir = args.output_dir or (
        Path(book_config.get("work_dir", "atlas_work")) / "book" / state_slug
    )
    if not out_dir.is_absolute():
        out_dir = repo_root / out_dir

    try:
        manifest, source = build_manifest(
            state=args.state,
            region=args.region,
            book=args.book,
            book_config=book_config,
            atlas_config=atlas_config,
            repo_root=repo_root,
            limit=args.limit,
            api_base=book_config.get("wiki_api_base", "http://localhost:3000"),
            parquet_only=args.parquet_only,
            local_only=local_only,
            no_maps=args.no_maps,
        )
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 2

    out_path = out_dir / "manifest.json"
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "state": args.state,
        "region": args.region,
        "source": source,
        "resort_count": len(manifest),
        "entries": manifest,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {len(manifest)} resorts to {out_path} (source={source})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
