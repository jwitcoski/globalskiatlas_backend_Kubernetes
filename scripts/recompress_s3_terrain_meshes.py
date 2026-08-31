#!/usr/bin/env python3
"""Re-encode published uncompressed terrain-mesh.glb files with Draco and overwrite S3.

Largest meshes first. Skips files that already have correct KHR_draco mapping
(POSITION unique_id != NORMAL unique_id 0 used as position — the v0 test wrap).

  python scripts/recompress_s3_terrain_meshes.py --dry-run
  python scripts/recompress_s3_terrain_meshes.py --limit 5
  python scripts/recompress_s3_terrain_meshes.py
"""
from __future__ import annotations

import argparse
import json
import logging
import struct
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from game_export.glb import (  # noqa: E402
    compress_terrain_glb_file,
    glb_has_draco,
)
from game_export.s3_inputs import DEFAULT_AWS_REGION, default_s3_bucket, make_s3_client  # noqa: E402

log = logging.getLogger("recompress_meshes")
PREFIX = "game_scenes"
PROGRESS = REPO / "output" / "draco_recompress_progress.json"


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def peek_gltf(s3, bucket: str, key: str) -> dict:
    head = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-19")["Body"].read()
    if len(head) < 20 or head[:4] != b"glTF":
        return {}
    json_len = struct.unpack_from("<I", head, 12)[0]
    end = 19 + json_len
    blob = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{end}")["Body"].read()
    raw = blob[20 : 20 + json_len].rstrip(b" ")
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        return {}


def needs_recompress(gltf: dict, *, force: bool) -> bool:
    if force:
        return True
    used = gltf.get("extensionsUsed") or []
    if "KHR_draco_mesh_compression" not in used:
        return True
    try:
        ext = gltf["meshes"][0]["primitives"][0]["extensions"]["KHR_draco_mesh_compression"]
        ids = ext.get("attributes") or {}
        node = (gltf.get("nodes") or [{}])[0]
    except (KeyError, IndexError, TypeError):
        return True
    # Wrong v0 wrap: POSITION unique_id 0 (that id is DracoPy NORMAL).
    if ids.get("POSITION") == 0:
        return True
    if "scale" in node or "translation" in node:
        return True
    return False


def list_meshes(s3, bucket: str) -> list[dict]:
    rows = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{PREFIX}/"):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if not key.endswith("/terrain/terrain-mesh.glb"):
                continue
            if key.endswith(".pre-draco.glb"):
                continue
            rows.append({"key": key, "size": int(obj["Size"])})
    rows.sort(key=lambda r: r["size"], reverse=True)
    return rows


def load_progress() -> dict:
    if PROGRESS.is_file():
        return json.loads(PROGRESS.read_text(encoding="utf-8"))
    return {"done": [], "skipped": [], "failed": []}


def save_progress(progress: dict) -> None:
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(progress, indent=2) + "\n", encoding="utf-8")


def recompress_one(s3, bucket: str, key: str, *, dry_run: bool) -> str:
    src_key = key
    pre = key[: -len("terrain-mesh.glb")] + "terrain-mesh.pre-draco.glb"
    try:
        s3.head_object(Bucket=bucket, Key=pre)
        src_key = pre
        log.info("using uncompressed backup %s", pre)
    except Exception:
        pass
    if dry_run:
        return "dry-run"
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        raw = td_path / "in.glb"
        out = td_path / "out.glb"
        s3.download_file(bucket, src_key, str(raw))
        data = raw.read_bytes()
        if glb_has_draco(data):
            raise ValueError(f"source is Draco and no uncompressed backup: {src_key}")
        info = compress_terrain_glb_file(raw, out)
        extra = {
            "ContentType": "model/gltf-binary",
            "CacheControl": "public, max-age=60, must-revalidate",
        }
        if src_key == key and not glb_has_draco(data):
            s3.copy_object(
                Bucket=bucket,
                Key=pre,
                CopySource={"Bucket": bucket, "Key": key},
                MetadataDirective="COPY",
            )
        s3.upload_file(str(out), bucket, key, ExtraArgs=extra)
        return (
            f"ok bytes {info['byte_size']} compression={info.get('compression')} "
            f"from {raw.stat().st_size}"
        )


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", default=None)
    p.add_argument("--limit", type=int, default=0, help="Max meshes to rewrite this run (0 = all)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true", help="Rewrite even if already Draco")
    args = p.parse_args(argv)
    bucket = args.bucket or default_s3_bucket()
    s3 = make_s3_client()
    log.info("listing %s/%s (region %s)", bucket, PREFIX, DEFAULT_AWS_REGION)
    meshes = list_meshes(s3, bucket)
    log.info("%s terrain-mesh.glb objects", len(meshes))
    progress = load_progress()
    done = set(progress.get("done") or [])
    n = 0
    for row in meshes:
        key = row["key"]
        if key in done and not args.force:
            continue
        gltf = peek_gltf(s3, bucket, key)
        if not needs_recompress(gltf, force=args.force):
            log.info("skip ok-draco %s (%s bytes)", key, row["size"])
            progress.setdefault("skipped", []).append(key)
            save_progress(progress)
            continue
        log.info("compress %s (%s bytes)", key, row["size"])
        try:
            msg = recompress_one(s3, bucket, key, dry_run=args.dry_run)
            log.info("  %s", msg)
            if not args.dry_run:
                progress.setdefault("done", []).append(key)
                done.add(key)
                progress["failed"] = [k for k in progress.get("failed") or [] if k != key]
        except Exception:
            log.exception("failed %s", key)
            progress.setdefault("failed", []).append(key)
        save_progress(progress)
        n += 1
        if args.limit and n >= args.limit:
            break
    log.info("this run rewrote %s, progress %s", n, PROGRESS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
