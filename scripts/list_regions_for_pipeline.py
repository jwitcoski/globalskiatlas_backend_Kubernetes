#!/usr/bin/env python3
"""
List all pipeline regions from config/regions.yaml for world/aws runners.
Output: one line per region: REGION<TAB>PBF_URL<TAB>CLUSTER_DIST_M
  REGION = continent/country/state path (e.g. europe/iceland, north-america/us/colorado, africa).
  CLUSTER_DIST_M = optional; empty if not set.

Usage:
  python scripts/list_regions_for_pipeline.py
  python scripts/list_regions_for_pipeline.py --continent europe
  python scripts/list_regions_for_pipeline.py --continent north-america --slug us/colorado
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Install PyYAML: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config" / "regions.yaml"


def load_region_rows():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    rows = []
    # Full continents: REGION = continent name
    for c in data.get("full_continents", []):
        continent = c["continent"]
        config = {k: v for k, v in c.items() if k not in ("continent", "pbf_url")}
        rows.append((continent, c["pbf_url"], config.get("cluster_dist_m")))
    # Split continents: REGION = continent/slug (e.g. europe/iceland, north-america/us/colorado)
    for continent_key, key in [
        ("north-america", "north_america"),
        ("europe", "europe"),
        ("asia", "asia"),
    ]:
        for r in data.get(key, []):
            slug = r["slug"]
            config = {k: v for k, v in r.items() if k not in ("slug", "pbf_url")}
            region_path = f"{continent_key}/{slug}" if slug != continent_key else continent_key
            rows.append((region_path, r["pbf_url"], config.get("cluster_dist_m")))
    return rows


def main():
    ap = argparse.ArgumentParser(description="List pipeline regions (REGION\tPBF_URL\tCLUSTER_DIST_M)")
    ap.add_argument("--continent", type=str, help="Filter by continent (e.g. europe, north-america)")
    ap.add_argument("--slug", type=str, help="Filter by slug (e.g. iceland, us/colorado); use with --continent")
    args = ap.parse_args()

    if not CONFIG_PATH.exists():
        print(f"Config not found: {CONFIG_PATH}", file=sys.stderr)
        sys.exit(1)

    rows = load_region_rows()

    if args.continent:
        cont = args.continent.strip().lower().replace("_", "-")
        rows = [(r, u, c) for r, u, c in rows if r == cont or r.startswith(cont + "/")]
    if args.slug:
        slug = args.slug.strip().lower().replace(" ", "-")
        rows = [(r, u, c) for r, u, c in rows if r.endswith("/" + slug) or r == slug]
        if not rows and args.continent:
            # try continent/slug
            want = f"{args.continent.strip().lower().replace('_', '-')}/{slug}"
            rows = [(r, u, c) for r, u, c in load_region_rows() if r == want]

    for region, pbf_url, cluster_dist in rows:
        cd = str(cluster_dist) if cluster_dist is not None else ""
        print(f"{region}\t{pbf_url}\t{cd}")


if __name__ == "__main__":
    main()
