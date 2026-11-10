"""Bake every game_scenes resort into the Hemu Ski game format and publish it under ``game_play/`` on S3.

  python scripts/bake_game_play.py --limit 3              # try a few
  python scripts/bake_game_play.py --ids montage_mountain_pa
  python scripts/bake_game_play.py --workers 6            # everything, resumable (done ids are skipped)

Per resort ``<prefix>/<id>/`` gets world.json, detail.u16, backdrop.u16 and diorama/{world.json,terrain.u16}.
``<prefix>/worlds.json`` is the registry the start-screen map reads; ``<prefix>/_progress.json`` tracks done/failed.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from game_export.game_play import SCENE_FILES, convert_scene  # noqa: E402
from game_export.s3_inputs import default_s3_bucket, make_s3_client  # noqa: E402
from game_export.s3_scenes import PREFIX as SCENE_PREFIX, download_json, upload_json  # noqa: E402

log = logging.getLogger("bake_game_play")
TYPES = {".json": "application/json; charset=utf-8", ".u16": "application/octet-stream"}
REQUIRED = {"scene-manifest.json", "terrain/heightfield-metadata.json", "terrain/heightfield-u16.bin",
            "vectors/pistes.geojson"}


def download_scene(s3, bucket, entry, dest: Path):
    base = f"{SCENE_PREFIX}/{entry['path']}/"
    for rel in SCENE_FILES:
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            s3.download_file(bucket, base + rel, str(target))
        except Exception as exc:
            if rel in REQUIRED:
                raise RuntimeError(f"missing {rel}: {exc}") from exc


def upload_game_files(s3, bucket, prefix, resort_id, out: Path):
    for f in sorted(p for p in out.rglob("*") if p.is_file()):
        s3.upload_file(str(f), bucket, f"{prefix}/{resort_id}/{f.relative_to(out).as_posix()}",
                       ExtraArgs={"ContentType": TYPES.get(f.suffix, "application/octet-stream"),
                                  "CacheControl": "public, max-age=3600"})


def bake_one(s3, bucket, prefix, entry, tile_cache: Path, dry_run: bool):
    rid = entry["id"]
    with tempfile.TemporaryDirectory(prefix="gp_") as tmp:
        scene, out = Path(tmp) / "scene", Path(tmp) / "out"
        download_scene(s3, bucket, entry, scene)
        result = convert_scene(scene, out, rid, catalog_name=entry.get("name"), tile_cache=tile_cache)
        if not dry_run:
            upload_game_files(s3, bucket, prefix, rid, out)
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", default=None)
    ap.add_argument("--prefix", default="game_play")
    ap.add_argument("--ids", default=None, help="comma-separated resort ids")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--force", action="store_true", help="re-bake ids already marked done")
    ap.add_argument("--retry-failed", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="convert but upload nothing")
    ap.add_argument("--cache-dir", type=Path, default=Path("cache/game_play_tiles"))
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

    bucket = a.bucket or default_s3_bucket()
    s3 = make_s3_client()
    a.cache_dir.mkdir(parents=True, exist_ok=True)
    catalog = download_json(s3, bucket, f"{SCENE_PREFIX}/catalog.json") or {}
    resorts = catalog.get("resorts") or []
    if a.ids:
        want = set(a.ids.split(","))
        resorts = [r for r in resorts if r["id"] in want]

    progress_key, worlds_key = f"{a.prefix}/_progress.json", f"{a.prefix}/worlds.json"
    progress = download_json(s3, bucket, progress_key) or {}
    done, failed = set(progress.get("done") or []), dict(progress.get("failed") or {})
    worlds = {w["id"]: w for w in (download_json(s3, bucket, worlds_key) or {}).get("resorts", [])}

    todo = [r for r in resorts if a.force or r["id"] not in done]
    if not a.retry_failed and not a.force:
        todo = [r for r in todo if r["id"] not in failed]
    if a.limit:
        todo = todo[:a.limit]
    log.info("%s resorts in catalog, %s done, %s failed, %s to bake", len(resorts), len(done), len(failed), len(todo))

    lock = threading.Lock()

    def save(final=False):
        if a.dry_run:
            return
        upload_json(s3, bucket, progress_key, {"done": sorted(done), "failed": failed}, cache_seconds=60)
        upload_json(s3, bucket, worlds_key, {"resorts": sorted(worlds.values(), key=lambda w: w["name"].lower())},
                    cache_seconds=300)

    t0, n = time.time(), 0
    with ThreadPoolExecutor(max_workers=a.workers) as pool:
        futs = {pool.submit(bake_one, s3, bucket, a.prefix, r, a.cache_dir, a.dry_run): r for r in todo}
        for fut in as_completed(futs):
            rid = futs[fut]["id"]
            n += 1
            try:
                entry = fut.result()
                with lock:
                    worlds[rid] = entry
                    done.add(rid)
                    failed.pop(rid, None)
                log.info("[%s/%s] ok   %s (%s runs, %s lifts)", n, len(todo), rid, entry["runs"], entry["lifts"])
            except Exception as exc:
                with lock:
                    failed[rid] = str(exc)[:200]
                log.warning("[%s/%s] FAIL %s: %s", n, len(todo), rid, exc)
            if n % 25 == 0:
                with lock:
                    save()
    save(final=True)
    log.info("finished %s resorts in %.0fs; %s done, %s failed total", n, time.time() - t0, len(done), len(failed))


if __name__ == "__main__":
    main()
