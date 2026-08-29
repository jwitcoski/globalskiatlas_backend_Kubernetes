#!/usr/bin/env python3
"""Run elevation/contours/cropped DEMs per region in Docker, optional S3 sync, then delete Skadi cache.

Does not re-download OSM PBFs. Expects output/<region>/ski_areas.parquet.

Usage:
  python scripts/elevation_preflight.py --region north-america/us/virginia
  python scripts/run_elevation_batches.py --region north-america/us/virginia --no-upload
  python scripts/run_elevation_batches.py --upload
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUTPUT = REPO / "output"
CACHE = REPO / "cache"
DEFAULT_IMAGE = "globalskiatlas-pipeline"
DEFAULT_BUCKET = "globalskiatlas-backend-k8s-output"
DEFAULT_IDS = REPO / "config" / "resorts" / "_playable_candidates.json"
ELEV_FILES = [
    "ski_areas_elevation.parquet",
    "ski_area_contours.parquet",
    "ski_area_contours.geojson",
    "ski_area_elevation_points.parquet",
    "ski_area_elevation_points.geojson",
    "ski_areas_analyzed.parquet",
    "ski_areas_analyzed.csv",
]


def discover_regions() -> list[str]:
    sys.path.insert(0, str(REPO / "scripts"))
    from combine_regions import discover_regions as disc

    return [r for r in disc(OUTPUT) if r != "combined"]


def load_ids(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return list(data.get("candidates") or [])


def docker_run(image: str, region: str, data_dir: Path, ids_file: Path | None, extra: list[str]) -> int:
    cmd = [
        "docker",
        "run",
        "--rm",
        "-e",
        "PYTHONUNBUFFERED=1",
        "-e",
        f"REGION={region}",
        "-v",
        f"{data_dir.resolve()}:/data",
        "-v",
        f"{CACHE.resolve()}:/cache",
        "-v",
        f"{(REPO / 'scripts').resolve()}:/app/scripts",
        "-v",
        f"{(REPO / 'config').resolve()}:/app/config",
        "-w",
        "/app",
        image,
        "python",
        "scripts/ski_area_elevation_contours.py",
        "-i",
        "/data/ski_areas.parquet",
        "-o",
        "/data",
        "--cache-dir",
        "/cache",
        "--boundaries",
        "/data/ski_areas.parquet",
        "--save-dem",
        "--clear-cache-every",
        "50",
    ]
    if ids_file:
        cmd.extend(["--ids-file", "/app/config/resorts/" + ids_file.name])
    cmd.extend(extra)
    print(" ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=REPO).returncode


def s3_sync(data_dir: Path, bucket: str, region: str) -> int:
    prefix = f"s3://{bucket}/{region}/"
    cmd = [
        "aws",
        "s3",
        "sync",
        str(data_dir),
        prefix,
        "--exclude",
        "*",
    ]
    for name in ELEV_FILES:
        cmd.extend(["--include", name])
    cmd.extend(["--include", "dems/*.tif", "--include", "dems/**/*.tif"])
    print(" ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=REPO).returncode


def cleanup(data_dir: Path, keep_dems: bool) -> None:
    skadi = CACHE / "skadi"
    if skadi.exists():
        shutil.rmtree(skadi, ignore_errors=True)
        print(f"Deleted {skadi}")
    nested = data_dir / "cache"
    if nested.exists():
        shutil.rmtree(nested, ignore_errors=True)
        print(f"Deleted {nested}")
    if not keep_dems:
        dems = data_dir / "dems"
        if dems.exists():
            shutil.rmtree(dems, ignore_errors=True)
            print(f"Deleted {dems}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Batch elevation + optional S3, then free disk")
    ap.add_argument("--region", type=str, help="Single region path (e.g. north-america/us/virginia)")
    ap.add_argument("--from-region", type=str, default=None)
    ap.add_argument("--ids-file", type=Path, default=DEFAULT_IDS)
    ap.add_argument("--no-ids-file", action="store_true", help="Process every ski area in the region parquet")
    ap.add_argument("--image", default=DEFAULT_IMAGE)
    ap.add_argument("--s3-bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--keep-dems", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    do_upload = args.upload and not args.no_upload

    CACHE.mkdir(parents=True, exist_ok=True)
    ids_file = None if args.no_ids_file else args.ids_file
    if ids_file and not ids_file.is_file():
        print(f"Missing ids file {ids_file}. Run scripts/elevation_preflight.py first.", file=sys.stderr)
        return 1

    if args.region:
        regions = [args.region.strip().replace("\\", "/")]
    else:
        regions = discover_regions()
        if args.from_region:
            start = args.from_region.strip().replace("\\", "/")
            if start in regions:
                regions = regions[regions.index(start) :]
            else:
                print(f"--from-region {start} not in discovered list", file=sys.stderr)
                return 1

    if ids_file:
        cand = load_ids(ids_file)
        by_region = {}
        for c in cand:
            by_region.setdefault(c.get("region") or "", []).append(c)
        if args.region:
            pass
        else:
            regions = [r for r in regions if r in by_region and by_region[r]]
        print(f"Regions with candidates: {len(regions)}")

    extra: list[str] = []
    failed = []
    for region in regions:
        data_dir = OUTPUT.joinpath(*region.split("/"))
        parquet = data_dir / "ski_areas.parquet"
        print(f"\n=== {region} ===")
        if not parquet.is_file():
            print(f"Skip (no ski_areas.parquet): {parquet}")
            continue
        if args.dry_run:
            print(f"Would process {data_dir}")
            continue
        code = docker_run(args.image, region, data_dir, ids_file, extra)
        if code != 0:
            failed.append(region)
            print(f"Elevation failed for {region} (exit {code})", file=sys.stderr)
            continue
        if do_upload:
            up = s3_sync(data_dir, args.s3_bucket, region)
            if up != 0:
                failed.append(region)
                print(f"S3 sync failed for {region}", file=sys.stderr)
                continue
        cleanup(data_dir, keep_dems=args.keep_dems)

    if failed:
        print("Failed:", ", ".join(failed), file=sys.stderr)
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
