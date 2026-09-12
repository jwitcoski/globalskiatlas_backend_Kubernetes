#!/usr/bin/env python3
"""Overnight baker for every wiki country clay island.

Discovers countries with downhill resorts in ski_areas_analyzed.parquet,
bakes `country-{country-slug}` scenes (Terrarium DEM + Natural Earth context),
and continues after failures. Re-run the same command to resume.

  python -u scripts/bake_all_country_clay.py --list
  python -u scripts/bake_all_country_clay.py
  python -u scripts/bake_all_country_clay.py --upload

Progress: config/clay_scenes/regions/_progress_countries.json
Log:      output/clay_scenes/regions/_bake_countries.log

Already-complete scenes (including the United States) are skipped unless --force.
Terrarium/Skadi download scratch is deleted after every country so overnight runs
do not fill the disk.
"""
from __future__ import annotations

import argparse
import gc
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from game_export.region_clay import (
    export_region_scene,
    list_country_units,
    region_id_for_country,
    region_scene_is_ready,
    refresh_region_osm_vectors,
    REGION_OUT_ROOT,
)
from game_export.region_dem import purge_region_dem_scratch
from game_export.s3_inputs import default_s3_bucket

log = logging.getLogger("bake_all_country_clay")
PROGRESS = REPO / "config" / "clay_scenes" / "regions" / "_progress_countries.json"
LOG_PATH = REPO / "output" / "clay_scenes" / "regions" / "_bake_countries.log"


def _write_progress(payload: dict) -> None:
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not root.handlers:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        root.addHandler(sh)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)


def _release_scratch(cache_dir: Path) -> None:
    purge_region_dem_scratch(cache_dir)
    gc.collect()


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-root", type=Path, default=REPO / "output")
    p.add_argument("--cache-dir", type=Path, default=REPO / "cache")
    p.add_argument("--out-root", type=Path, default=REPO / "output")
    p.add_argument("--mesh-m", type=float, default=None)
    p.add_argument("--force", action="store_true", help="Rebuild even if the scene is already complete")
    p.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Do not skip complete scenes (still use --force to replace meshes)",
    )
    p.add_argument("--list", action="store_true")
    p.add_argument("--limit", type=int, default=0, help="Bake at most N countries")
    p.add_argument("--no-from-s3", action="store_true")
    p.add_argument("--upload", action="store_true", help="Upload all local region scenes when the batch finishes")
    args = p.parse_args()
    os.environ.setdefault("GDAL_CACHEMAX", "256")
    _setup_logging()
    skip_existing = not args.no_skip_existing and not args.force

    units = list_country_units(
        args.data_root,
        args.cache_dir,
        from_s3=not args.no_from_s3,
    )
    print(f"{len(units)} countries with downhill resorts", flush=True)
    for u in units:
        rid = region_id_for_country(u.country)
        ready = "ready" if region_scene_is_ready(args.out_root, rid) else "missing"
        print(f"  {u.country}  resorts={u.resort_count}  {rid}  {ready}", flush=True)
    if args.list:
        return 0
    if args.limit and args.limit > 0:
        units = units[: args.limit]

    log.info("Starting country clay batch (%s units). Log: %s", len(units), LOG_PATH)
    ok: list[str] = []
    skipped: list[str] = []
    failed: list[dict] = []
    for i, u in enumerate(units, start=1):
        rid = region_id_for_country(u.country)
        print(f"=== [{i}/{len(units)}] {u.country} ({rid}) ===", flush=True)
        if skip_existing and region_scene_is_ready(args.out_root, rid):
            admin1 = args.out_root / REGION_OUT_ROOT / rid / "vectors" / "admin-1.geojson"
            if admin1.is_file():
                skipped.append(rid)
                log.info("Skip existing complete scene %s", rid)
                continue
            log.info("Patching admin-1 click layer onto existing %s", rid)
            try:
                refresh_region_osm_vectors(
                    country=u.country,
                    cache_dir=args.cache_dir,
                    out_root=args.out_root,
                    data_root=args.data_root,
                )
                skipped.append(rid)
                _release_scratch(args.cache_dir)
                continue
            except Exception as exc:
                log.exception("Failed admin-1 patch %s: %s", rid, exc)
                failed.append({"id": rid, "country": u.country, "error": str(exc)})
                _release_scratch(args.cache_dir)
                continue
        try:
            scene = export_region_scene(
                state=None,
                country=u.country,
                page_id=None,
                data_root=args.data_root,
                cache_dir=args.cache_dir,
                out_root=args.out_root,
                from_s3=not args.no_from_s3,
                s3_bucket=default_s3_bucket(),
                mesh_resolution_m=args.mesh_m,
                force=args.force,
            )
        except Exception as exc:
            log.exception("Failed %s: %s", rid, exc)
            failed.append({"id": rid, "country": u.country, "error": str(exc)})
            _write_progress(
                {
                    "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "ok": ok,
                    "skipped": skipped,
                    "failed": failed,
                }
            )
            _release_scratch(args.cache_dir)
            continue
        glb = scene / "terrain" / "terrain-mesh.glb"
        size = glb.stat().st_size if glb.is_file() else 0
        print(f"  wrote {scene} ({size:,} bytes)", flush=True)
        ok.append(rid)
        _write_progress(
            {
                "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ok": ok,
                "skipped": skipped,
                "failed": failed,
            }
        )
        _release_scratch(args.cache_dir)

    print(f"Done: {len(ok)} baked, {len(skipped)} skipped, {len(failed)} failed", flush=True)
    if failed:
        print("Failed:", ", ".join(f["id"] for f in failed), file=sys.stderr, flush=True)
    log.info("Country clay batch finished. ok=%s skipped=%s failed=%s", len(ok), len(skipped), len(failed))
    _release_scratch(args.cache_dir)

    if args.upload:
        cmd = [sys.executable, str(REPO / "scripts" / "upload_region_clay_scenes.py")]
        return subprocess.call(cmd)
    return 1 if failed and not ok else 0


if __name__ == "__main__":
    raise SystemExit(main())
