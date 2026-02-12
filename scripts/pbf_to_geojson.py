#!/usr/bin/env python3
"""
Extract landuse=winter_sports from OSM PBF and write GeoJSON.
Uses osmium tags-filter (CLI) + ogr2ogr, or pyosmium if available.
No Overpass required.
"""
import json
import subprocess
import sys
from pathlib import Path


def run_osmium_filter(pbf_path: Path, out_pbf: Path) -> bool:
    """Run osmium tags-filter to extract winter_sports."""
    cmd = [
        "osmium", "tags-filter", "-O",
        str(pbf_path), "wr/landuse=winter_sports",
        "-o", str(out_pbf),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True
    except FileNotFoundError:
        return False
    except subprocess.CalledProcessError as e:
        print(f"osmium failed: {e.stderr}", file=sys.stderr)
        return False


def run_ogr2ogr(pbf_path: Path, geojson_path: Path) -> bool:
    """Convert PBF to GeoJSON using ogr2ogr (GDAL). Merges multipolygons + lines + points."""
    all_features = []
    tmp = geojson_path.parent / "tmp.geojson"
    for layer in ["multipolygons", "lines", "points"]:
        cmd = [
            "ogr2ogr", "-f", "GeoJSON", "-t_srs", "EPSG:4326",
            "-sql", f"SELECT * FROM {layer}",
            str(tmp), str(pbf_path),
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            if tmp.exists() and tmp.stat().st_size > 50:
                data = json.loads(tmp.read_text())
                all_features.extend(data.get("features", []))
        except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError):
            pass
        tmp.unlink(missing_ok=True)
    if all_features:
        geojson_path.write_text(json.dumps({"type": "FeatureCollection", "features": all_features}, indent=2))
        return True
    cmd = ["ogr2ogr", "-f", "GeoJSON", "-t_srs", "EPSG:4326", str(geojson_path), str(pbf_path)]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        print(f"ogr2ogr failed: {e}", file=sys.stderr)
        return False


def main():
    pbf_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/db/planet.osm.pbf")
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("/data/ski_areas.geojson")

    if not pbf_path.exists():
        print(f"Error: {pbf_path} not found", file=sys.stderr)
        sys.exit(1)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_pbf = out_path.with_suffix(".filtered.osm.pbf")

    print("Extracting landuse=winter_sports from PBF...")
    if not run_osmium_filter(pbf_path, filtered_pbf):
        print("Error: osmium not found. Install osmium-tool.", file=sys.stderr)
        sys.exit(1)

    if not filtered_pbf.exists() or filtered_pbf.stat().st_size == 0:
        print("No winter_sports features found. Writing empty GeoJSON.")
        out_path.write_text('{"type":"FeatureCollection","features":[]}')
        filtered_pbf.unlink(missing_ok=True)
        return

    print("Converting to GeoJSON...")
    if not run_ogr2ogr(filtered_pbf, out_path):
        print("Error: ogr2ogr failed. Falling back to empty GeoJSON.", file=sys.stderr)
        out_path.write_text('{"type":"FeatureCollection","features":[]}')
    else:
        # Merge multipolygons layer if present; ogr2ogr creates multiple layers
        try:
            data = json.loads(out_path.read_text())
            if isinstance(data, dict) and "type" in data:
                print(f"Saved {len(data.get('features', []))} features to {out_path}")
            else:
                # Might be layernames as keys
                all_features = []
                for v in data.values() if isinstance(data, dict) else []:
                    if isinstance(v, dict) and "features" in v:
                        all_features.extend(v["features"])
                if all_features:
                    merged = {"type": "FeatureCollection", "features": all_features}
                    out_path.write_text(json.dumps(merged, indent=2))
                    print(f"Saved {len(all_features)} features to {out_path}")
        except Exception:
            pass

    filtered_pbf.unlink(missing_ok=True)
    print("Done.")


if __name__ == "__main__":
    main()
