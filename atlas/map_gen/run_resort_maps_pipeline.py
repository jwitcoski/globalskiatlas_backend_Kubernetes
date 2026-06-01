#!/usr/bin/env python3
"""
End-to-end resort map pipeline: generate QGZ + PNG (portrait + landscape), upload to wiki S3.

  1. data_to_qgis  — per-resort layouts at trail-count tier + landscape twin
  2. export_layouts — re-export any missing PNGs (optional if step 1 exported)
  3. upload_resort_maps — both s3://globalskiatlas-resort-maps/{pageId}-portrait.png
     and {pageId}-landscape.png (curation picks which becomes the live wiki {pageId}.png)

Usage:
  python -m atlas.map_gen.run_resort_maps_pipeline --all-resorts
  python -m atlas.map_gen.run_resort_maps_pipeline --region north-america/us/vermont --limit 3
  python -m atlas.map_gen.run_resort_maps_pipeline --upload-only
  python -m atlas.map_gen.run_resort_maps_pipeline --generate-only --no-upload

On Windows with QGIS installed, prefer:
  atlas\\map_gen\\run_resort_maps_pipeline.bat --region north-america/us/virginia --limit 2
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from atlas.map_gen.data_to_qgis import load_config, run_resorts
from atlas.map_gen.export_layouts import ensure_headless_qgis_initialized, shutdown_headless_qgis_if_initialized
from atlas.map_gen.upload_resort_maps import run_upload


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate resort maps (portrait + landscape) and upload PNGs for the wiki",
    )
    parser.add_argument("--input-dir", type=Path, default=None)
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--resort", type=str, default=None)
    parser.add_argument("--region", type=str, default=None)
    parser.add_argument("--all-resorts", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--dpi",
        type=int,
        default=None,
        help="Export DPI (default: config atlas.yaml dpi, else 150)",
    )
    parser.add_argument("--qgis-root", type=Path, default=None)
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Generate + export only; do not upload to S3",
    )
    parser.add_argument(
        "--upload-only",
        action="store_true",
        help="Skip generate/export; upload existing PNGs from atlas_work",
    )
    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="After generate, do not run batch export_layouts",
    )
    parser.add_argument(
        "--reexport",
        action="store_true",
        help="After generate, export all layouts again (default: PNGs written during generate)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Upload dry-run only")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Upload manifest path (default: output/resort_maps_upload_manifest.json)",
    )
    args = parser.parse_args()

    if args.region:
        region_raw = str(args.region).strip()
        if " " in region_raw or region_raw.casefold() in {"north", "south", "america"}:
            print(
                f"Invalid --region {region_raw!r}. Use hyphens, e.g. north-america "
                "(not 'north america').",
                file=sys.stderr,
            )
            return 1

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

    dpi = args.dpi if args.dpi is not None else int(config.get("dpi", 150))
    do_generate = not args.upload_only
    do_upload = not args.generate_only

    if do_generate:
        print("Starting resort map pipeline...", flush=True)
        try:
            ensure_headless_qgis_initialized(args.qgis_root)
        except RuntimeError as e:
            print(f"\n{e}", file=sys.stderr)
            print(
                "Install QGIS or run atlas\\map_gen\\run_resort_maps_pipeline.bat",
                file=sys.stderr,
            )
            return 2

    exit_code = 0
    try:
        if do_generate:
            print(f"=== Generate maps (dpi={dpi}) ===")
            count = run_resorts(
                input_dir,
                work_dir,
                config,
                resort_id=args.resort,
                region_filter=args.region,
                limit=args.limit,
                preview=False,
                export_layout=True,
                export_dpi=dpi,
                layout_tier_override=None,
                all_layout_tiers=False,
            )
            print(f"Generated {count} layout(s) under {work_dir}/")
            print(
                "Each resort should have portrait ({region}/{slug}/{slug}_export.png) and "
                "landscape ({region}/{slug}-layout-{tier}-landscape/..._export.png)."
            )

            if args.reexport and not args.skip_export:
                print("\n=== Re-export all layouts ===")
                from atlas.map_gen.export_layouts import export_qgz
                from atlas.map_gen.resort_map_paths import iter_expected_qgz_paths

                if args.region or args.limit or args.resort:
                    qgz_files = [
                        p
                        for p in iter_expected_qgz_paths(
                            work_dir,
                            input_dir,
                            config,
                            region_filter=args.region,
                            resort_id=args.resort,
                            limit=args.limit,
                        )
                        if p.is_file()
                    ]
                else:
                    qgz_files = sorted(work_dir.rglob("*_map.qgz"))
                for qgz in qgz_files:
                    export_qgz(qgz, dpi=dpi, overwrite=True)
            elif not args.skip_export:
                print("\n(PNGs exported during generate; use --reexport to run layout export again)")

        if do_upload or args.dry_run:
            print("\n=== Upload to wiki S3 ===")
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
            if failed:
                exit_code = 1
            if uploaded == 0 and skipped > 0 and not args.dry_run:
                print("No maps uploaded — run generate first or check atlas_work paths.")
    finally:
        if do_generate:
            shutdown_headless_qgis_if_initialized()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
