#!/usr/bin/env python3
"""Bake wiki region clay islands (admin-boundary terrain + resort points).

One country (United States first):

  python scripts/bake_region_clay.py --country "United States of America" --force
  python scripts/upload_region_clay_scenes.py --only country-united-states-of-america

One state:

  python scripts/bake_region_clay.py --state "West Virginia" --country "United States of America"

All secondary admin areas with downhill resorts (states / provinces / territories):

  python scripts/bake_region_clay.py --all-admin1 --list
  python scripts/bake_region_clay.py --all-admin1
  python scripts/bake_region_clay.py --all-admin1 --country "Canada" --upload
  python scripts/bake_region_clay.py --all-admin1 --limit 3   # smoke

Overnight, every downhill country:

  python -u scripts/bake_all_country_clay.py
  python -u scripts/bake_all_country_clay.py --upload

region_id is always the wiki pageId: state-{state-slug}-{country-slug} or country-{country-slug}
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from game_export.region_clay import (
    export_region_scene,
    list_admin1_units,
    region_id_for_state,
    region_scene_is_ready,
    refresh_region_osm_vectors,
)
from game_export.s3_inputs import default_s3_bucket

log = logging.getLogger("bake_region_clay")
PROGRESS = REPO / "config" / "clay_scenes" / "regions" / "_progress.json"


def _write_progress(payload: dict) -> None:
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _bake_one(args, *, state: str, country: str) -> Path:
    if args.osm_only:
        return refresh_region_osm_vectors(
            state=state,
            country=country,
            page_id=None,
            cache_dir=args.cache_dir,
            out_root=args.out_root,
        )
    return export_region_scene(
        state=state,
        country=country,
        page_id=None,
        data_root=args.data_root,
        cache_dir=args.cache_dir,
        out_root=args.out_root,
        from_s3=not args.no_from_s3,
        s3_bucket=default_s3_bucket(),
        mesh_resolution_m=args.mesh_m,
        force=args.force,
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--state", default=None)
    p.add_argument("--country", default=None, help="Filter or single-scene country (default for one-off: United States of America)")
    p.add_argument("--page-id", default=None)
    p.add_argument("--data-root", type=Path, default=REPO / "output")
    p.add_argument("--cache-dir", type=Path, default=REPO / "cache")
    p.add_argument("--out-root", type=Path, default=REPO / "output")
    p.add_argument("--mesh-m", type=float, default=None)
    p.add_argument("--force", action="store_true")
    p.add_argument(
        "--osm-only",
        action="store_true",
        help="Keep existing terrain mesh; bake Natural Earth highways/water/places",
    )
    p.add_argument(
        "--all-admin1",
        action="store_true",
        help="Bake every downhill state/province/territory in ski_areas_analyzed.parquet",
    )
    p.add_argument("--list", action="store_true", help="Print admin-1 units and exit")
    p.add_argument("--limit", type=int, default=0, help="Bake at most N units (with --all-admin1)")
    p.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip units that already have a complete scene (mesh + all vector layers)",
    )
    p.add_argument("--no-from-s3", action="store_true")
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()

    if args.all_admin1 or args.list:
        units = list_admin1_units(
            args.data_root,
            args.cache_dir,
            from_s3=not args.no_from_s3,
            country=args.country,
        )
        print(f"{len(units)} admin-1 units with downhill resorts")
        for u in units:
            rid = region_id_for_state(u.state or "", u.country)
            ready = "ready" if region_scene_is_ready(args.out_root, rid) else "missing"
            print(f"  {u.country} / {u.state}  resorts={u.resort_count}  {rid}  {ready}")
        if args.list:
            return 0
        if args.limit and args.limit > 0:
            units = units[: args.limit]

        ok: list[str] = []
        skipped: list[str] = []
        failed: list[dict] = []
        for i, u in enumerate(units, start=1):
            rid = region_id_for_state(u.state or "", u.country)
            print(f"=== [{i}/{len(units)}] {u.state}, {u.country} ({rid}) ===", flush=True)
            if args.skip_existing and not args.force and region_scene_is_ready(args.out_root, rid):
                skipped.append(rid)
                log.info("Skip existing complete scene %s", rid)
                continue
            try:
                scene = _bake_one(args, state=u.state or "", country=u.country)
            except Exception as exc:
                log.exception("Failed %s: %s", rid, exc)
                failed.append({"id": rid, "state": u.state, "country": u.country, "error": str(exc)})
                _write_progress(
                    {
                        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "ok": ok,
                        "skipped": skipped,
                        "failed": failed,
                    }
                )
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

        print(f"Done: {len(ok)} baked, {len(skipped)} skipped, {len(failed)} failed")
        if failed:
            print("Failed:", ", ".join(f["id"] for f in failed), file=sys.stderr)

        if args.upload and (ok or skipped):
            cmd = [sys.executable, str(REPO / "scripts" / "upload_region_clay_scenes.py")]
            return subprocess.call(cmd)
        return 1 if failed and not ok else 0

    if not args.all_admin1 and not args.list:
        if args.page_id:
            pass
        elif args.country and not args.state:
            pass
        else:
            if not args.state:
                args.state = "West Virginia"
            if not args.country:
                args.country = "United States of America"

    if args.osm_only:
        scene = refresh_region_osm_vectors(
            state=args.state,
            country=args.country,
            page_id=args.page_id,
            cache_dir=args.cache_dir,
            out_root=args.out_root,
        )
    else:
        scene = export_region_scene(
            state=args.state,
            country=args.country,
            page_id=args.page_id,
            data_root=args.data_root,
            cache_dir=args.cache_dir,
            out_root=args.out_root,
            from_s3=not args.no_from_s3,
            s3_bucket=default_s3_bucket(),
            mesh_resolution_m=args.mesh_m,
            force=args.force,
        )
    glb = scene / "terrain" / "terrain-mesh.glb"
    print(f"Region clay written: {scene}")
    if glb.is_file():
        print(f"Terrain mesh: {glb} ({glb.stat().st_size:,} bytes)")

    if args.upload:
        cmd = [sys.executable, str(REPO / "scripts" / "upload_region_clay_scenes.py")]
        return subprocess.call(cmd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
