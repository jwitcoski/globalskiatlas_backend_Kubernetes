#!/usr/bin/env python3
"""Upload latest catalog scene cakes to S3 (not the GIS pantry, not the website client).

Skips per-scene qa/. The Three.js hub lives in GlobalSkiAtlas_2.

  python scripts/upload_game_scenes.py
  python scripts/upload_game_scenes.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCENES = REPO / "output" / "game_scenes"
DEFAULT_BUCKET = "globalskiatlas-backend-k8s-output"
PREFIX = "game_scenes"
EXTRA_TYPES = {
    ".js": "text/javascript; charset=utf-8",
    ".mjs": "text/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".geojson": "application/geo+json",
    ".bin": "application/octet-stream",
    ".glb": "model/gltf-binary",
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".md": "text/markdown; charset=utf-8",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".svg": "image/svg+xml",
    ".woff2": "font/woff2",
}


def content_type(path: Path) -> str:
    extra = EXTRA_TYPES.get(path.suffix.lower())
    if extra:
        return extra
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def skip_relative(rel: Path) -> bool:
    parts = rel.parts
    return "qa" in parts


def iter_files(root: Path):
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if skip_relative(rel):
            continue
        yield p, rel


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--prefix", default=PREFIX)
    p.add_argument("--scenes-dir", type=Path, default=SCENES)
    p.add_argument(
        "--replace-catalog",
        action="store_true",
        help="Upload local catalog.json (destructive if local disk only has a subset). Default is hub+scenes listed in local catalog only.",
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    root = args.scenes_dir
    catalog_path = root / "catalog.json"
    if not catalog_path.is_file():
        print(f"Missing {catalog_path}", file=sys.stderr)
        return 2
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    to_upload: list[tuple[Path, str]] = []

    def add_tree(local_root: Path, key_prefix: str) -> None:
        if local_root.is_file():
            to_upload.append((local_root, f"{key_prefix}/{local_root.name}".replace("\\", "/")))
            return
        if not local_root.is_dir():
            return
        for f, rel in iter_files(local_root):
            key = f"{key_prefix}/{rel.as_posix()}"
            to_upload.append((f, key))

    if args.replace_catalog:
        add_tree(root / "catalog.json", args.prefix)
    for resort in catalog.get("resorts") or []:
        rel = str(resort.get("path") or "").strip().replace("\\", "/")
        if not rel:
            continue
        scene = root.joinpath(*rel.split("/"))
        if not (scene / "scene-manifest.json").is_file():
            print(f"Skip missing scene {scene}", file=sys.stderr)
            continue
        add_tree(scene, f"{args.prefix}/{rel}")

    total = sum(f.stat().st_size for f, _ in to_upload)
    print(f"{len(to_upload)} files, {total / 1e6:.1f} MB -> s3://{args.bucket}/{args.prefix}/")
    if args.dry_run:
        for f, key in to_upload[:12]:
            print(f"  {key}")
        if len(to_upload) > 12:
            print(f"  … {len(to_upload) - 12} more")
        return 0

    import boto3

    s3 = boto3.client("s3")
    for i, (f, key) in enumerate(to_upload, start=1):
        ctype = content_type(f)
        extra = {"CacheControl": "public, max-age=300"}
        if f.name in {"index.html", "catalog.json"} or f.suffix.lower() in {".js", ".html"}:
            extra["CacheControl"] = "public, max-age=60"
        s3.upload_file(
            str(f),
            args.bucket,
            key,
            ExtraArgs={"ContentType": ctype, **extra},
        )
        if i == 1 or i % 25 == 0 or i == len(to_upload):
            print(f"  {i}/{len(to_upload)} {key}", flush=True)
    url = f"https://{args.bucket}.s3.us-east-1.amazonaws.com/{args.prefix}/catalog.json"
    print(url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
