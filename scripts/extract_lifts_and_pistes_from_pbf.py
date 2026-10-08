#!/usr/bin/env python3
"""
Extract all lifts (aerialway=*) and all pistes (piste:type=*) from a local PBF file.
Uses the same pipeline as pbf_to_geojson.py: osmium tags-filter → ogr2ogr → GeoJSON FeatureCollection.
Outputs output/lifts.geojson and output/pistes.geojson (same format as ski_areas.geojson).
"""
import json
import shutil
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
        except subprocess.CalledProcessError as e:
            print(f"ogr2ogr {layer} layer failed (exit {e.returncode}): {e.stderr.strip()[-500:]}", file=sys.stderr)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"ogr2ogr {layer} layer skipped: {e}", file=sys.stderr)
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


def run_ogr2ogr_filtered(pbf_path: Path, geojson_path: Path, layer: str, where: str) -> bool:
    """Extract from PBF using ogr2ogr with SQL WHERE (GDAL fallback when osmium fails)."""
    tmp = geojson_path.parent / "tmp_ogr_filtered.geojson"
    cmd = [
        "ogr2ogr", "-f", "GeoJSON", "-t_srs", "EPSG:4326",
        "-sql", f"SELECT * FROM {layer} WHERE {where}",
        str(tmp), str(pbf_path),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        if tmp.exists() and tmp.stat().st_size > 50:
            data = json.loads(tmp.read_text(encoding="utf-8"))
            features = data.get("features", [])
            if features:
                geojson_path.write_text(
                    json.dumps({"type": "FeatureCollection", "features": features}, indent=2),
                    encoding="utf-8",
                )
                tmp.unlink(missing_ok=True)
                return True
    except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError):
        pass
    tmp.unlink(missing_ok=True)
    return False


def extract_one(pbf_path: Path, out_geojson: Path, expressions: list, label: str, gdal_fallback: tuple[str, str] | None = None) -> int:
    """Filter PBF by expressions, convert to GeoJSON; return feature count.
    If osmium fails and gdal_fallback is (layer, where), try ogr2ogr on full PBF."""
    filtered_pbf = out_geojson.with_suffix(".filtered.osm.pbf")
    osmium_ok = run_osmium_filter(pbf_path, filtered_pbf, expressions)
    if osmium_ok and filtered_pbf.exists() and filtered_pbf.stat().st_size > 0:
        if run_ogr2ogr(filtered_pbf, out_geojson):
            filtered_pbf.unlink(missing_ok=True)
            data = json.loads(out_geojson.read_text(encoding="utf-8"))
            features = data.get("features", [])
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
        filtered_pbf.unlink(missing_ok=True)

    if not osmium_ok and gdal_fallback:
        layer, where = gdal_fallback
        print(f"osmium failed; trying ogr2ogr on full PBF ({label})...", file=sys.stderr)
        if run_ogr2ogr_filtered(pbf_path, out_geojson, layer, where):
            data = json.loads(out_geojson.read_text(encoding="utf-8"))
            n = len(data.get("features", []))
            print(f"Saved {n} features to {out_geojson}")
            return n
    elif not osmium_ok:
        print(f"osmium tags-filter ({label}) failed.", file=sys.stderr)

    out_geojson.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
    print(f"No {label} found. Wrote empty {out_geojson.name}")
    return 0


def extract_with_pyogrio(pbf_path: Path, lifts_path: Path, pistes_path: Path) -> bool:
    """Same output as the osmium + ogr2ogr path, read with GDAL's OSM driver through pyogrio.
    For machines without osmium/ogr2ogr; scans the full PBF once per layer."""
    try:
        import geopandas as gpd
        import pandas as pd
        import pyogrio
    except ImportError:
        return False
    lift = "other_tags LIKE '%\"aerialway\"=>%'"
    piste = "other_tags LIKE '%\"piste:type\"=>%'"
    where = {
        "multipolygons": f"{lift} OR {piste}",
        "lines": f"aerialway IS NOT NULL OR {lift} OR {piste}",
        "points": f"{lift} OR {piste}",
    }
    frames = []
    for layer, clause in where.items():
        df = pyogrio.read_dataframe(str(pbf_path), sql=f"SELECT * FROM {layer} WHERE {clause}")
        print(f"  {layer}: {len(df)} features")
        frames.append(df)
    feats = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
    tags = feats["other_tags"].fillna("") if "other_tags" in feats.columns else pd.Series("", index=feats.index)
    is_lift = tags.str.contains('"aerialway"=>', regex=False)
    if "aerialway" in feats.columns:
        is_lift |= feats["aerialway"].notna()
    is_piste = tags.str.contains('"piste:type"=>', regex=False)
    for path, mask, label in ((lifts_path, is_lift, "lifts"), (pistes_path, is_piste, "pistes")):
        part = feats.loc[mask]
        if len(part):
            part = part.dropna(axis=1, how="all")
            part.to_file(path, driver="GeoJSON")
        else:
            path.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
        print(f"Saved {len(part)} {label} features to {path}")
    return True


def extract_lifts_and_pistes(pbf_path: Path, output_dir: Path) -> None:
    """Extract all aerialway (lifts) and piste:type (pistes) from PBF; write lifts.geojson and pistes.geojson."""
    pbf_path = Path(pbf_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    lifts_path = output_dir / "lifts.geojson"
    pistes_path = output_dir / "pistes.geojson"

    print("Extracting lifts (aerialway=*) and pistes (piste:type=*) from PBF...")
    print(f"PBF: {pbf_path} | Output: {lifts_path}, {pistes_path}")

    if not (shutil.which("osmium") and shutil.which("ogr2ogr")):
        print("osmium/ogr2ogr not found; reading the PBF with pyogrio.")
        if extract_with_pyogrio(pbf_path, lifts_path, pistes_path):
            print("Done.")
            return

    # Same style as pbf_to_geojson: wr/ for ways and relations; add nodes for point features (e.g. lift stations)
    lift_expr = ["n/aerialway", "w/aerialway", "r/aerialway"]
    piste_expr = ["n/piste:type", "w/piste:type", "r/piste:type"]
    # GDAL fallback when osmium fails (e.g. England BlobHeader): lines layer has aerialway; piste in other_tags
    extract_one(pbf_path, lifts_path, lift_expr, "lifts", gdal_fallback=("lines", "aerialway IS NOT NULL"))
    extract_one(pbf_path, pistes_path, piste_expr, "pistes", gdal_fallback=("lines", "other_tags LIKE '%piste:type%'"))
    print("Done.")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Extract all lifts and pistes from PBF as GeoJSON (same format as ski_areas.geojson)")
    p.add_argument("pbf", help="Path to OSM PBF file")
    p.add_argument("-o", "--output-dir", default="output", help="Output directory (default: output)")
    args = p.parse_args()
    extract_lifts_and_pistes(Path(args.pbf), Path(args.output_dir))
