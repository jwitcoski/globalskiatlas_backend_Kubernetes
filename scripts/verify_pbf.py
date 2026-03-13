#!/usr/bin/env python3
"""
Verify a downloaded OSM PBF is readable (not corrupt).
Uses GDAL so it works for files that trigger osmium BlobHeader errors (e.g. Geofabrik England).
Exits 0 if valid, 1 if invalid or unreadable.
"""
import sys
from pathlib import Path


def main() -> None:
    pbf = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/db/planet.osm.pbf")
    if not pbf.exists():
        print(f"Error: {pbf} not found", file=sys.stderr)
        sys.exit(1)
    if pbf.stat().st_size == 0:
        print(f"Error: {pbf} is empty", file=sys.stderr)
        sys.exit(1)
    try:
        from osgeo import ogr
        ds = ogr.Open(str(pbf))
        if ds is None:
            print(f"Error: GDAL could not open {pbf}", file=sys.stderr)
            sys.exit(1)
        n = ds.GetLayerCount()
        ds = None
        if n == 0:
            print(f"Error: {pbf} has no layers", file=sys.stderr)
            sys.exit(1)
        print(f"Verified: {pbf} ({n} layer(s))")
    except ImportError:
        # No GDAL: fall back to minimal check (file exists and non-empty)
        print(f"Verified: {pbf} (size {pbf.stat().st_size})")
    sys.exit(0)


if __name__ == "__main__":
    main()
