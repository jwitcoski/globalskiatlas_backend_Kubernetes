#!/usr/bin/env python3
"""Upload clay 3D scenes + catalog to S3 (homepage hero + wiki 3D Map tab).

  python scripts/upload_clay_scenes.py
  python scripts/upload_clay_scenes.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEFAULT_BUCKET = "globalskiatlas-backend-k8s-output"
DEFAULT_PREFIX = "clay_scenes"
DEFAULT_SCENES = REPO / "output" / "clay_scenes"
DEFAULT_CATALOG = REPO / "config" / "clay_scenes" / "catalog.json"
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


def iter_files(root: Path):
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        yield p, p.relative_to(root)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--prefix", default=DEFAULT_PREFIX)
    p.add_argument("--scenes-dir", type=Path, default=DEFAULT_SCENES)
    p.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--only",
        default=None,
        help="Comma-separated resort ids to upload (default: all catalog entries with local scenes)",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        help="Fail if any catalog resort is missing locally (default: skip missing and prune catalog upload)",
    )
    args = p.parse_args()

    if not args.catalog.is_file():
        print(f"Missing catalog: {args.catalog}", file=sys.stderr)
        return 2
    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    resorts = catalog.get("resorts") or []
    if not resorts:
        print("Catalog has no resorts", file=sys.stderr)
        return 2

    only_ids = None
    if args.only:
        only_ids = {x.strip() for x in args.only.split(",") if x.strip()}

    to_upload: list[tuple[Path, str]] = []
    missing: list[str] = []
    present: list[dict] = []

    for resort in resorts:
        rid = str(resort.get("id") or "").strip()
        if not rid:
            continue
        if only_ids is not None and rid not in only_ids:
            continue
        scene = args.scenes_dir / rid
        manifest = scene / "scene-manifest.json"
        if not manifest.is_file():
            missing.append(rid)
            continue
        present.append(resort)
        for f, rel in iter_files(scene):
            key = f"{args.prefix}/{rid}/{rel.as_posix()}"
            to_upload.append((f, key))

    if missing:
        print(f"Skipping missing local scenes ({len(missing)}): {', '.join(missing)}", file=sys.stderr)
        if args.strict:
            return 2
    if not present:
        print("No local scenes to upload", file=sys.stderr)
        return 2

    # Catalog on S3 should list every local scene we have, even when --only uploads
    # a subset of scene folders.
    catalog_resorts = []
    for resort in resorts:
        rid = str(resort.get("id") or "").strip()
        if not rid:
            continue
        if (args.scenes_dir / rid / "scene-manifest.json").is_file():
            catalog_resorts.append(resort)
    upload_catalog = {**catalog, "resorts": catalog_resorts}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(upload_catalog, tmp, indent=2, ensure_ascii=True)
        tmp_path = Path(tmp.name)
    to_upload.append((tmp_path, f"{args.prefix}/catalog.json"))

    total = sum(f.stat().st_size for f, _ in to_upload)
    print(
        f"{len(to_upload)} files, {total / 1e6:.1f} MB -> s3://{args.bucket}/{args.prefix}/ "
        f"(uploading {len(present)} scene(s); catalog {len(catalog_resorts)} resorts; "
        f"skipped {len(missing)} missing)"
    )
    if args.dry_run:
        for f, key in to_upload[:12]:
            print(f"  {key}")
        if len(to_upload) > 12:
            print(f"  … {len(to_upload) - 12} more")
        tmp_path.unlink(missing_ok=True)
        return 0

    import boto3

    s3 = boto3.client("s3")
    for i, (f, key) in enumerate(to_upload, start=1):
        ctype = content_type(f)
        extra = {"CacheControl": "public, max-age=300"}
        if f.name == "catalog.json" or f.suffix.lower() in {".js", ".html"}:
            extra["CacheControl"] = "public, max-age=60"
        s3.upload_file(
            str(f),
            args.bucket,
            key,
            ExtraArgs={"ContentType": ctype, **extra},
        )
        if i == 1 or i % 25 == 0 or i == len(to_upload):
            print(f"  {i}/{len(to_upload)} {key}", flush=True)
    tmp_path.unlink(missing_ok=True)
    url = f"https://{args.bucket}.s3.us-east-1.amazonaws.com/{args.prefix}/catalog.json"
    print(url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
