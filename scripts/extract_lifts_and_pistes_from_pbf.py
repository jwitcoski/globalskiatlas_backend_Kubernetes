#!/usr/bin/env python3
"""
Extract all lifts (aerialway=*) and all pistes (piste:type=*) from a local PBF file.
Uses the same pipeline as pbf_to_geojson.py: osmium tags-filter → ogr2ogr → GeoJSON FeatureCollection.
Outputs output/lifts.geojson and output/pistes.geojson (same format as ski_areas.geojson).
"""
import json
import subprocess
import sys
from pathlib import Path


def run_osmium_filter(pbf_path: Path, out_pbf: Path, expressions: list) -> bool:
    """Run osmium tags-filter with given expressions (e.g. ['w/aerialway', 'n/aerialway'])."""
    cmd = ["osmium", "tags-filter", "-O", str(pbf_path)] + expressions + ["-o", str(out_pbf)]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True
    except FileNotFoundError:
        return False
    except subprocess.CalledProcessError as e:
        print(f"osmium failed: {e.stderr}", file=sys.stderr)
        return False


def run_ogr2ogr(pbf_path: Path, geojson_path: Path) -> bool:
    """Convert PBF to GeoJSON using ogr2ogr (same as pbf_to_geojson.py). Merges multipolygons + lines + points."""
    all_features = []
    tmp = geojson_path.parent / "tmp_lifts_pistes.geojson"
    for layer in ["multipolygons", "lines", "points"]:
        cmd = [
            "ogr2ogr", "-f", "GeoJSON", "-t_srs", "EPSG:4326",
            "-sql", f"SELECT * FROM {layer}",
            str(tmp), str(pbf_path),
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            if tmp.exists() and tmp.stat().st_size > 50:
                data = json.loads(tmp.read_text(encoding="utf-8"))
                all_features.extend(data.get("features", []))
        except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError):
            pass
        tmp.unlink(missing_ok=True)
    if all_features:
        geojson_path.write_text(
            json.dumps({"type": "FeatureCollection", "features": all_features}, indent=2),
            encoding="utf-8",
        )
        return True
    # Fallback: single ogr2ogr without layer filter (may produce multiple layers in one file)
    cmd = ["ogr2ogr", "-f", "GeoJSON", "-t_srs", "EPSG:4326", str(geojson_path), str(pbf_path)]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def extract_one(pbf_path: Path, out_geojson: Path, expressions: list, label: str) -> int:
    """Filter PBF by expressions, convert to GeoJSON; return feature count."""
    filtered_pbf = out_geojson.with_suffix(".filtered.osm.pbf")
    if not run_osmium_filter(pbf_path, filtered_pbf, expressions):
        print(f"osmium tags-filter ({label}) failed.", file=sys.stderr)
        return 0
    if not filtered_pbf.exists() or filtered_pbf.stat().st_size == 0:
        out_geojson.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
        filtered_pbf.unlink(missing_ok=True)
        print(f"No {label} found. Wrote empty {out_geojson.name}")
        return 0
    if not run_ogr2ogr(filtered_pbf, out_geojson):
        out_geojson.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
        filtered_pbf.unlink(missing_ok=True)
        return 0
    filtered_pbf.unlink(missing_ok=True)
    data = json.loads(out_geojson.read_text(encoding="utf-8"))
    features = data.get("features", [])
    # Handle ogr2ogr fallback that may write layer names as top-level keys
    if not features and isinstance(data, dict):
        for v in data.values():
            if isinstance(v, dict) and "features" in v:
                features.extend(v["features"])
        if features:
            out_geojson.write_text(
                json.dumps({"type": "FeatureCollection", "features": features}, indent=2),
                encoding="utf-8",
            )
    n = len(features)
    print(f"Saved {n} features to {out_geojson}")
    return n


def extract_lifts_and_pistes(pbf_path: Path, output_dir: Path) -> None:
    """Extract all aerialway (lifts) and piste:type (pistes) from PBF; write lifts.geojson and pistes.geojson."""
    pbf_path = Path(pbf_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    lifts_path = output_dir / "lifts.geojson"
    pistes_path = output_dir / "pistes.geojson"

    print("Extracting lifts (aerialway=*) and pistes (piste:type=*) from PBF...")
    print(f"PBF: {pbf_path} | Output: {lifts_path}, {pistes_path}")

    # Same style as pbf_to_geojson: wr/ for ways and relations; add nodes for point features (e.g. lift stations)
    lift_expr = ["n/aerialway", "w/aerialway", "r/aerialway"]
    piste_expr = ["n/piste:type", "w/piste:type", "r/piste:type"]

    extract_one(pbf_path, lifts_path, lift_expr, "lifts")
    extract_one(pbf_path, pistes_path, piste_expr, "pistes")
    print("Done.")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Extract all lifts and pistes from PBF as GeoJSON (same format as ski_areas.geojson)")
    p.add_argument("pbf", help="Path to OSM PBF file")
    p.add_argument("-o", "--output-dir", default="output", help="Output directory (default: output)")
    args = p.parse_args()
    extract_lifts_and_pistes(Path(args.pbf), Path(args.output_dir))
