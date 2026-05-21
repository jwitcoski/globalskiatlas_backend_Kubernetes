#!/usr/bin/env python3
"""
Upload resort map PNGs to the wiki static-map S3 bucket (globalskiatlas-resort-maps).

Every resort gets **both** orientations (curation picks the live wiki image later):
  {pageId}-portrait.png
  {pageId}-landscape.png

Usage:
  python -m atlas.map_gen.upload_resort_maps --all-resorts
  python -m atlas.map_gen.upload_resort_maps --region north-america/us/vermont --limit 5
  python -m atlas.map_gen.upload_resort_maps --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd

from atlas.map_gen.data_to_qgis import load_config
from atlas.map_gen.resort_map_paths import (
    atlas_slug_for_resort,
    layout_tier_for_resort,
    normalize_region,
    resolve_export_paths,
)
from atlas.map_gen.wiki_page_id import wiki_page_id_from_row, wiki_row_from_parquet


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _wiki_maps_cfg(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("wiki_maps") or {}


def upload_file(
    s3: Any,
    bucket: str,
    key: str,
    local_path: Path,
    *,
    dry_run: bool,
) -> bool:
    if not local_path.is_file():
        return False
    if dry_run:
        print(f"  would upload {local_path.name} → s3://{bucket}/{key}")
        return True
    s3.upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={"ContentType": "image/png", "CacheControl": "public, max-age=86400"},
    )
    print(f"  uploaded s3://{bucket}/{key}")
    return True


def run_upload(
    *,
    input_dir: Path,
    work_dir: Path,
    config: dict[str, Any],
    region_filter: Optional[str] = None,
    resort_id: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    manifest_path: Optional[Path] = None,
) -> tuple[int, int, int]:
    wiki_cfg = _wiki_maps_cfg(config)
    bucket = wiki_cfg.get("bucket", "globalskiatlas-resort-maps")
    require_both = bool(wiki_cfg.get("require_both_orientations", True))

    ski_areas_path = input_dir / "ski_areas.parquet"
    if not ski_areas_path.exists():
        print(f"Missing {ski_areas_path}", file=sys.stderr)
        return 0, 0, 0

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
    if gdf.empty:
        print("No resorts matched.", file=sys.stderr)
        return 0, 0, 0

    name_col = "Ski Area" if "Ski Area" in gdf.columns else "name"
    state_col = "State" if "State" in gdf.columns else None

    pistes_all = None
    pistes_path = input_dir / "pistes.parquet"
    if pistes_path.exists():
        pistes_all = gpd.read_parquet(pistes_path)

    tiers_cfg = config.get("trail_tiers") or {}
    seen_slugs: dict[str, int] = {}

    s3 = None
    if not dry_run:
        try:
            import boto3
        except ImportError:
            print("boto3 required for upload (pip install boto3)", file=sys.stderr)
            return 0, 0, 0
        s3 = boto3.client("s3", region_name=wiki_cfg.get("region", "us-east-1"))

    uploaded = 0
    skipped = 0
    failed = 0
    manifest: list[dict[str, Any]] = []

    for _, row in gdf.iterrows():
        resort_name = str(row.get(name_col) or "").strip()
        if not resort_name:
            skipped += 1
            continue

        page_id = wiki_page_id_from_row(wiki_row_from_parquet(row, name_col=name_col, state_col=state_col))
        slug = atlas_slug_for_resort(resort_name, row, seen_slugs)

        n_trails = -1
        if pistes_all is not None and name_col in pistes_all.columns:
            n_trails = len(pistes_all[pistes_all[name_col] == resort_name])

        tier = layout_tier_for_resort(resort_name, n_trails, tiers_cfg)
        region = normalize_region(str(row.get("region") or ""))
        paths = resolve_export_paths(
            work_dir, slug, tier, config=config, region=region
        )
        portrait_path = paths.get("portrait")
        landscape_path = paths.get("landscape")

        entry: dict[str, Any] = {
            "pageId": page_id,
            "slug": slug,
            "resort": resort_name,
            "layout_tier": tier,
            "portrait": str(portrait_path) if portrait_path else None,
            "landscape": str(landscape_path) if landscape_path else None,
        }

        has_portrait = portrait_path is not None and portrait_path.is_file()
        has_landscape = landscape_path is not None and landscape_path.is_file()

        if require_both and (not has_portrait or not has_landscape):
            missing = []
            if not has_portrait:
                missing.append("portrait")
            if not has_landscape:
                missing.append("landscape")
            print(f"  skip {page_id}: missing {', '.join(missing)} (slug={slug} tier={tier})")
            entry["status"] = f"missing_{'_'.join(missing)}"
            manifest.append(entry)
            skipped += 1
            continue

        if not has_portrait and not has_landscape:
            print(f"  skip {page_id}: no export PNGs")
            entry["status"] = "missing_png"
            manifest.append(entry)
            skipped += 1
            continue

        try:
            ok_portrait = has_portrait and upload_file(
                s3,
                bucket,
                f"{page_id}-portrait.png",
                portrait_path,
                dry_run=dry_run,
            )
            ok_landscape = has_landscape and upload_file(
                s3,
                bucket,
                f"{page_id}-landscape.png",
                landscape_path,
                dry_run=dry_run,
            )
        except Exception as e:
            print(f"  ERROR {page_id}: {e}", file=sys.stderr)
            entry["status"] = f"error: {e}"
            manifest.append(entry)
            failed += 1
            continue

        if require_both:
            ok = ok_portrait and ok_landscape
        else:
            ok = ok_portrait or ok_landscape

        if ok:
            entry["status"] = "uploaded"
            uploaded += 1
        else:
            entry["status"] = "upload_failed"
            failed += 1
        manifest.append(entry)

    if manifest_path:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Manifest: {manifest_path}")

    return uploaded, skipped, failed


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload resort map PNGs (portrait + landscape) to S3")
    parser.add_argument("--input-dir", type=Path, default=None)
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--region", type=str, default=None)
    parser.add_argument("--resort", type=str, default=None, help="winter_sports_id / osm id filter")
    parser.add_argument("--all-resorts", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Write upload manifest JSON (default: output/resort_maps_upload_manifest.json)",
    )
    args = parser.parse_args()

    if not args.all_resorts and not args.resort and not args.region:
        parser.print_help()
        return 1

    root = _repo_root()
    config = load_config()
    input_dir = args.input_dir or Path(config.get("input_dir", "output/combined"))
    work_dir = args.work_dir or Path(config.get("work_dir", "atlas_work"))
    if not input_dir.is_absolute():
        input_dir = root / input_dir
    if not work_dir.is_absolute():
        work_dir = root / work_dir

    manifest = args.manifest or (root / "output" / "resort_maps_upload_manifest.json")

    uploaded, skipped, failed = run_upload(
        input_dir=input_dir,
        work_dir=work_dir,
        config=config,
        region_filter=args.region,
        resort_id=args.resort,
        limit=args.limit,
        dry_run=args.dry_run,
        manifest_path=manifest,
    )
    print(f"\nUpload: {uploaded} ok (both orientations), {skipped} skipped, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
