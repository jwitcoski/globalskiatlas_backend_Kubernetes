#!/usr/bin/env python3
"""Upload wiki region clay scenes (does not touch resort clay_scenes/catalog.json).

  python scripts/upload_region_clay_scenes.py
  python scripts/upload_region_clay_scenes.py --dry-run
  python scripts/upload_region_clay_scenes.py --only state-west-virginia-united-states-of-america
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
DEFAULT_PREFIX = "clay_scenes/regions"
DEFAULT_SCENES = REPO / "output" / "clay_scenes" / "regions"
DEFAULT_CATALOG = REPO / "config" / "clay_scenes" / "regions" / "catalog.json"
EXTRA_TYPES = {
    ".json": "application/json; charset=utf-8",
    ".geojson": "application/geo+json",
    ".glb": "model/gltf-binary",
    ".md": "text/markdown; charset=utf-8",
}


def content_type(path: Path) -> str:
    extra = EXTRA_TYPES.get(path.suffix.lower())
    if extra:
        return extra
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def iter_files(root: Path):
    for p in root.rglob("*"):
        if p.is_file():
            yield p, p.relative_to(root)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--prefix", default=DEFAULT_PREFIX)
    p.add_argument("--scenes-dir", type=Path, default=DEFAULT_SCENES)
    p.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--only", default=None, help="Comma-separated region_id / pageId values")
    args = p.parse_args()

    if not args.catalog.is_file():
        print(f"Missing catalog: {args.catalog}", file=sys.stderr)
        return 2
    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    regions = catalog.get("regions") or []
    only_ids = None
    if args.only:
        only_ids = {x.strip() for x in args.only.split(",") if x.strip()}

    to_upload: list[tuple[Path, str]] = []
    present: list[dict] = []
    missing: list[str] = []

    for region in regions:
        rid = str(region.get("id") or "").strip()
        if not rid:
            continue
        if only_ids is not None and rid not in only_ids:
            continue
        scene = args.scenes_dir / rid
        if not (scene / "scene-manifest.json").is_file():
            missing.append(rid)
            continue
        present.append(region)
        for f, rel in iter_files(scene):
            key = f"{args.prefix}/{rid}/{rel.as_posix()}"
            to_upload.append((f, key))

    if missing:
        print(f"Skipping missing local scenes ({len(missing)}): {', '.join(missing)}", file=sys.stderr)
    if not present:
        print("No local region scenes to upload", file=sys.stderr)
        return 2

    catalog_regions = []
    for region in regions:
        rid = str(region.get("id") or "").strip()
        if rid and (args.scenes_dir / rid / "scene-manifest.json").is_file():
            catalog_regions.append({**region, "ready": True})
    upload_catalog = {**catalog, "regions": catalog_regions}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(upload_catalog, tmp, indent=2, ensure_ascii=True)
        tmp_path = Path(tmp.name)
    to_upload.append((tmp_path, f"{args.prefix}/catalog.json"))

    by_page_dir = args.scenes_dir / "by-page"
    if by_page_dir.is_dir():
        for f in by_page_dir.glob("*.json"):
            to_upload.append((f, f"{args.prefix}/by-page/{f.name}"))

    total = sum(f.stat().st_size for f, _ in to_upload)
    print(
        f"{len(to_upload)} files, {total / 1e6:.1f} MB -> s3://{args.bucket}/{args.prefix}/ "
        f"({len(present)} region scene(s))"
    )
    if args.dry_run:
        for f, key in to_upload[:20]:
            print(f"  {key}")
        if len(to_upload) > 20:
            print(f"  … {len(to_upload) - 20} more")
        tmp_path.unlink(missing_ok=True)
        return 0

    import boto3

    s3 = boto3.client("s3")
    for i, (f, key) in enumerate(to_upload, start=1):
        extra = {"CacheControl": "public, max-age=300"}
        if f.name == "catalog.json":
            extra["CacheControl"] = "public, max-age=60"
        s3.upload_file(
            str(f),
            args.bucket,
            key,
            ExtraArgs={"ContentType": content_type(f), **extra},
        )
        if i == 1 or i % 10 == 0 or i == len(to_upload):
            print(f"  {i}/{len(to_upload)} {key}", flush=True)
    tmp_path.unlink(missing_ok=True)
    print(f"https://globalskiatlas.com/{args.prefix}/catalog.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
