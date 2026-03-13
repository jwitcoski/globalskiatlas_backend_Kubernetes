#!/usr/bin/env python3
"""
Run the ski atlas pipeline for one or more regions (country/subregion PBFs).
Output: output/<continent>/ for full continents, output/<continent>/<slug>/ for split regions.
Uses Docker image from Dockerfile.aws (build first: docker build -f Dockerfile.aws -t globalskiatlas-pipeline .).

Usage:
  python scripts/run_region_local.py --list
  python scripts/run_region_local.py --continent europe --slug austria
  python scripts/run_region_local.py --continent north-america
  python scripts/run_region_local.py --continent asia --slug japan
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Install PyYAML: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config" / "regions.yaml"
OUTPUT_ROOT = REPO_ROOT / "output"
DEFAULT_IMAGE = "globalskiatlas-pipeline"


def load_regions():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    rows = []
    for c in data.get("full_continents", []):
        continent = c["continent"]
        config = {k: v for k, v in c.items() if k not in ("continent", "pbf_url")}
        rows.append((continent, continent, c["pbf_url"], config))
    for continent, key in [("north_america", "north_america"), ("europe", "europe"), ("asia", "asia")]:
        for r in data.get(key, []):
            config = {k: v for k, v in r.items() if k not in ("slug", "pbf_url")}
            rows.append((continent, r["slug"], r["pbf_url"], config))
    return rows


def list_regions(rows):
    print("continent\tslug\tpbf_url")
    for continent, slug, url, _ in rows:
        print(f"{continent}\t{slug}\t{url}")


def run_one(
    continent: str, slug: str, pbf_url: str, config: dict, image: str, dry_run: bool, prune: bool
) -> int:
    path_continent = continent.replace("_", "-")
    out_dir = OUTPUT_ROOT / path_continent if slug == continent else OUTPUT_ROOT / path_continent / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    # Volume name cannot contain / (e.g. china/beijing -> china-beijing)
    db_volume = f"pipeline-db-{continent}-{slug.replace('/', '-')}"
    cmd = [
        "docker",
        "run",
        "--rm",
        "-e", f"PBF_URL={pbf_url}",
        "-e", f"REGION={slug}",
    ]
    if config.get("cluster_dist_m") is not None:
        cmd.extend(["-e", f"OSM_NEARBY_CLUSTER_DIST_M={config['cluster_dist_m']}"])
    cmd.extend([
        "-v", f"{out_dir.absolute()}:{'/data'}",
        "-v", f"{db_volume}:/db",
        image,
    ])
    print(f"Output: {out_dir}")
    print(f"PBF: {pbf_url}")
    if dry_run:
        print(" ".join(cmd))
        return 0
    code = subprocess.run(cmd, cwd=REPO_ROOT).returncode
    # Remove this region's PBF volume so Docker doesn't keep the download
    subprocess.run(["docker", "volume", "rm", db_volume], cwd=REPO_ROOT, capture_output=True)
    if prune:
        print("Cleaning Docker (prune)...")
        subprocess.run(["docker", "system", "prune", "-f"], cwd=REPO_ROOT)
    return code


def main():
    ap = argparse.ArgumentParser(description="Run ski atlas pipeline per region (local Docker)")
    ap.add_argument("--list", action="store_true", help="List all regions and exit")
    ap.add_argument("--continent", type=str, help="Continent (e.g. europe, north_america, asia)")
    ap.add_argument("--slug", type=str, help="Region slug (e.g. austria, japan/hokkaido). If omitted, run all in continent.")
    ap.add_argument("--from-slug", type=str, help="When running whole continent, start from this slug inclusive (e.g. japan/chubu).")
    ap.add_argument("--image", type=str, default=DEFAULT_IMAGE, help=f"Docker image (default: {DEFAULT_IMAGE})")
    ap.add_argument("--dry-run", action="store_true", help="Print docker run command only")
    ap.add_argument("--prune", action="store_true", default=True, help="After each region: remove PBF volume and run docker system prune (default: True)")
    ap.add_argument("--no-prune", action="store_false", dest="prune", help="Do not prune Docker after each region")
    args = ap.parse_args()

    if not CONFIG_PATH.exists():
        print(f"Config not found: {CONFIG_PATH}", file=sys.stderr)
        sys.exit(1)

    rows = load_regions()

    if args.list:
        list_regions(rows)
        return 0

    if not args.continent:
        ap.error("Use --list or provide --continent (and optionally --slug)")

    continent = args.continent.strip().lower().replace("-", "_")
    slug = args.slug.strip().lower().replace(" ", "-") if args.slug else None

    from_slug = args.from_slug.strip().lower().replace(" ", "-") if getattr(args, "from_slug", None) else None

    def norm(s: str) -> str:
        return s.replace("-", "_").lower()

    if slug:
        if from_slug:
            ap.error("Use either --slug or --from-slug, not both")
        # Exact match or prefix (e.g. austria matches austria/tirol, austria/salzburg, ...)
        matches = [
            (c, s, u, cfg)
            for c, s, u, cfg in rows
            if norm(c) == continent and (s == slug or s.startswith(slug + "/"))
        ]
        if not matches:
            print(f"No region: continent={continent} slug={slug}", file=sys.stderr)
            sys.exit(1)
        runs = matches
    else:
        matches = [(c, s, u, cfg) for c, s, u, cfg in rows if norm(c) == continent]
        if not matches:
            print(f"No regions for continent={continent}", file=sys.stderr)
            sys.exit(1)
        if from_slug:
            try:
                start_i = next(i for i, (_, s, _, _) in enumerate(matches) if s == from_slug)
            except StopIteration:
                print(f"No region with slug={from_slug} in continent={continent}", file=sys.stderr)
                sys.exit(1)
            runs = matches[start_i:]
        else:
            runs = matches

    for i, (c, s, u, cfg) in enumerate(runs):
        if len(runs) > 1:
            print(f"\n--- [{i+1}/{len(runs)}] {c} / {s} ---")
        code = run_one(c, s, u, cfg, args.image, args.dry_run, args.prune)
        if code != 0:
            sys.exit(code)
    print("\nDone.")


if __name__ == "__main__":
    main()
