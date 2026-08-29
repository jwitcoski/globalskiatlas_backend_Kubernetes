#!/usr/bin/env python3
"""Bake playable-proxy mountains in batches of 10, upload to S3, delete local cakes.

S3 catalog.json is the source of truth. Local catalog is not uploaded.

  python scripts/bake_game_scene_batches.py
  python scripts/bake_game_scene_batches.py --max-batches 1
  python scripts/bake_game_scene_batches.py --max-batches 0   # until queue empty
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from game_export.catalog import catalog_entry_from_scene
from game_export.cli import _export_resort
from game_export.config import config_from_candidate, load_resort_config
from game_export.s3_inputs import DEFAULT_AWS_REGION
from game_export.s3_scenes import (
    CATALOG_KEY,
    PREFIX,
    bucket_name,
    load_progress,
    make_scenes_client,
    merge_and_upload_catalog,
    public_head_ok,
    public_object_url,
    save_progress,
    upload_scene_tree,
)

log = logging.getLogger("bake_batches")
CANDIDATES = REPO / "config" / "resorts" / "_playable_candidates.json"


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def load_candidates(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = list(payload.get("candidates") or [])
    rows.sort(key=lambda r: (str(r.get("region") or ""), str(r.get("winter_sports_id") or "")))
    return rows


def live_yaml_winter_sports_ids() -> list[str]:
    ids = []
    for yml in sorted((REPO / "config" / "resorts").glob("*.yaml")):
        try:
            cfg = load_resort_config(yml)
        except Exception:
            continue
        ids.append(cfg.winter_sports_id)
    picked = REPO / "config" / "resorts" / "_picked_batch.json"
    if picked.is_file():
        for row in json.loads(picked.read_text(encoding="utf-8")):
            wid = str(row.get("winter_sports_id") or "").strip()
            if wid:
                ids.append(wid)
    return ids


def seed_progress(progress: dict, extra_done: list[str]) -> dict:
    done = set(progress.get("done") or [])
    for wid in extra_done:
        if wid:
            done.add(str(wid))
    progress["done"] = sorted(done)
    progress["failed"] = list(progress.get("failed") or [])
    progress["skipped"] = list(progress.get("skipped") or [])
    return progress


def blocked_ids(progress: dict) -> set[str]:
    out = set()
    for key in ("done", "failed", "skipped"):
        out.update(str(x) for x in progress.get(key) or [])
    return out


def next_batch(candidates: list[dict], progress: dict, size: int) -> list[dict]:
    blocked = blocked_ids(progress)
    picked = []
    for row in candidates:
        wid = str(row.get("winter_sports_id") or "").strip()
        if not wid or wid in blocked:
            continue
        picked.append(row)
        if len(picked) >= size:
            break
    return picked


def configs_for_batch(rows: list[dict]) -> list[tuple[dict, object]]:
    used: list[str] = []
    out = []
    for row in rows:
        cfg = config_from_candidate(row, used_ids=used)
        used.append(cfg.resort_id)
        out.append((row, cfg))
    return out


def delete_local_cake(scene_dir: Path | None, region: str, wid: str, cache_dir: Path) -> None:
    if scene_dir is not None and scene_dir.exists():
        shutil.rmtree(scene_dir, ignore_errors=True)
        parent = scene_dir.parent
        if parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
    dem_root = cache_dir / "game_export_dems" / region.strip("/")
    for name in (f"{wid}-osmhull.tif", f"{wid}.tif"):
        p = dem_root / name
        if p.is_file():
            p.unlink()


def mark(progress: dict, bucket_name_key: str, wid: str) -> None:
    for key in ("done", "failed", "skipped"):
        progress[key] = [x for x in progress.get(key) or [] if str(x) != wid]
    progress.setdefault(bucket_name_key, []).append(wid)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--batch-size", type=int, default=10)
    p.add_argument(
        "--max-batches",
        type=int,
        default=1,
        help="0 means keep going until the candidate queue is empty",
    )
    p.add_argument("--candidates", type=Path, default=CANDIDATES)
    p.add_argument("--out-root", type=Path, default=REPO / "output" / "game_scenes")
    p.add_argument("--cache-dir", type=Path, default=REPO / "cache")
    p.add_argument("--s3-bucket", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--keep-local", action="store_true", help="Do not delete cakes after upload")
    return p.parse_args(argv)


def main(argv=None) -> int:
    _setup_logging()
    args = parse_args(argv)
    bucket = bucket_name(args.s3_bucket)
    s3 = make_scenes_client()
    candidates = load_candidates(args.candidates)
    progress = seed_progress(load_progress(s3, bucket), live_yaml_winter_sports_ids())
    log.info(
        "Candidates %s; done=%s failed=%s skipped=%s",
        len(candidates),
        len(progress["done"]),
        len(progress["failed"]),
        len(progress["skipped"]),
    )
    export_args = SimpleNamespace(dry_run=False, force=False, fetch_skadi=True, skip_catalog=True)
    data_root = REPO / "output"
    batches_done = 0
    while True:
        if args.max_batches and batches_done >= args.max_batches:
            break
        if not next_batch(candidates, progress, 1):
            log.info("Queue empty")
            break
        batches_done += 1
        log.info("=== batch %s ===", batches_done)
        incoming_catalog = []
        uploaded_dirs: list[tuple[object, Path]] = []
        while len(uploaded_dirs) < args.batch_size:
            need = args.batch_size - len(uploaded_dirs)
            chunk = next_batch(candidates, progress, need)
            if not chunk:
                break
            for row, cfg in configs_for_batch(chunk):
                wid = cfg.winter_sports_id
                log.info("Export %s %s (%s)", cfg.resort_id, cfg.display_name, wid)
                if args.dry_run:
                    log.info("dry-run skip export %s", wid)
                    uploaded_dirs.append((cfg, Path("_dry_run")))
                    if len(uploaded_dirs) >= args.batch_size:
                        break
                    continue
                try:
                    result = _export_resort(
                        export_args,
                        resort_id=cfg.resort_id,
                        cfg_path=None,
                        data_root=data_root,
                        cache_dir=args.cache_dir,
                        out_root=args.out_root,
                        from_s3=True,
                        s3_bucket=bucket,
                        cfg=cfg,
                    )
                except Exception:
                    log.exception("Export crashed %s", wid)
                    mark(progress, "failed", wid)
                    partial = args.out_root / cfg.resort_id
                    if partial.is_dir():
                        shutil.rmtree(partial, ignore_errors=True)
                    continue
                if result.code != 0 or result.scene_dir is None or result.approved < 1:
                    reason = "export_failed" if result.code not in {0, 3} else "no_approved_course"
                    log.error("%s %s (%s)", reason, cfg.resort_id, wid)
                    mark(progress, "failed", wid)
                    delete_local_cake(result.scene_dir, cfg.region, wid, args.cache_dir)
                    continue
                key = f"{PREFIX}/{result.scene_dir.parent.name}/{result.scene_dir.name}/scene-manifest.json"
                upload_scene_tree(s3, bucket, result.scene_dir, dry_run=args.dry_run)
                url = public_object_url(bucket, key, DEFAULT_AWS_REGION)
                if not args.dry_run and not public_head_ok(url):
                    log.error("Public HEAD failed after upload: %s", url)
                    mark(progress, "failed", wid)
                    continue
                incoming_catalog.append(catalog_entry_from_scene(result.scene_dir))
                mark(progress, "done", wid)
                uploaded_dirs.append((cfg, result.scene_dir))
                if len(uploaded_dirs) >= args.batch_size:
                    break
        if incoming_catalog:
            payload = merge_and_upload_catalog(s3, bucket, incoming_catalog, dry_run=args.dry_run)
            log.info("Public catalog now has %s resorts", len(payload.get("resorts") or []))
        save_progress(s3, bucket, progress, dry_run=args.dry_run)
        if not args.keep_local and not args.dry_run:
            for cfg, scene_dir in uploaded_dirs:
                delete_local_cake(scene_dir, cfg.region, cfg.winter_sports_id, args.cache_dir)
        log.info("Batch %s finished", batches_done)
    print(public_object_url(bucket, CATALOG_KEY, DEFAULT_AWS_REGION))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
