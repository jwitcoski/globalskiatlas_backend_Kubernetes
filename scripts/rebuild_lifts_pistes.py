#!/usr/bin/env python3
"""
Rebuild only lifts.parquet and pistes.parquet for pipeline regions, leaving ski areas, analysis,
contours and DEMs untouched. Runs the regular extract -> enrich -> parquet steps for those two files
against the region's existing ski_areas.parquet. Afterwards run scripts/combine_regions.py.

Regions processed before 2026-09-28 lost every LineString in convert_to_geoparquet's geometry
sanitizer; --missing-lines-only picks exactly those regions. A region whose ski_areas.parquet was
never written (but was otherwise processed) gets it rebuilt from the same PBF.

Usage:
  python scripts/rebuild_lifts_pistes.py --continent europe --missing-lines-only
  python scripts/rebuild_lifts_pistes.py --continent asia --workers 3
  python scripts/rebuild_lifts_pistes.py --region europe/austria/vorarlberg --keep-pbf

Then merge only those regions into a copy of the published combined files, keeping single-resort patches:
  python scripts/combine_regions.py --update-existing --combined-dir <copy of s3 combined/> -r <regions...>
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
import traceback
import urllib.request
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import geopandas as gpd  # noqa: E402

from convert_to_geoparquet import geojson_to_geoparquet, normalize_parquet_dir  # noqa: E402
from enrich_geojson_properties import _load_boundaries, enrich_geojson  # noqa: E402
from extract_lifts_and_pistes_from_pbf import extract_lifts_and_pistes  # noqa: E402
from list_regions_for_pipeline import load_region_rows  # noqa: E402


def has_lines(region_dir: Path) -> bool:
    for name in ("lifts.parquet", "pistes.parquet"):
        path = region_dir / name
        if path.exists():
            gdf = gpd.read_parquet(path, columns=["geometry"])
            if gdf.geom_type.isin(["LineString", "MultiLineString"]).any():
                return True
    return False


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    req = urllib.request.Request(url, headers={"User-Agent": "globalskiatlas-rebuild-lifts/1.0"})
    with urllib.request.urlopen(req, timeout=600) as resp, open(tmp, "wb") as f:
        shutil.copyfileobj(resp, f, length=8 * 1024 * 1024)
    tmp.replace(dest)


def restore_ski_areas(pbf: Path, work: Path, ski_parquet: Path, boundaries: Path, cache) -> None:
    """Regions whose ski_areas.parquet was never written: rebuild it from the PBF (as pbf_to_geojson's GDAL path)."""
    import pyogrio

    ski_geojson = work / "ski_areas_restore.geojson"
    gdf = pyogrio.read_dataframe(str(pbf), sql="SELECT * FROM multipolygons WHERE landuse='winter_sports'")
    gdf.dropna(axis=1, how="all").to_file(ski_geojson, driver="GeoJSON")
    enrich_geojson(ski_geojson, boundaries, None, is_ski_areas_file=True, boundaries_cache=cache)
    geojson_to_geoparquet(ski_geojson, ski_parquet)


def rebuild_region(region: str, pbf_url: str, output_dir: Path, pbf_dir: Path, boundaries: Path, keep_pbf: bool) -> str:
    start = time.perf_counter()
    out = output_dir / Path(*region.split("/"))
    ski_parquet = out / "ski_areas.parquet"
    if not ski_parquet.exists() and not (out / "ski_areas_analyzed.parquet").exists():
        return f"{region}: skipped (region was never processed)"

    pbf = pbf_dir / (region.replace("/", "__") + ".osm.pbf")
    if not pbf.exists():
        print(f"[{region}] downloading {pbf_url}", flush=True)
        download(pbf_url, pbf)

    work = out / "_lifts_pistes_rebuild"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    try:
        cache = _load_boundaries(boundaries)
        if not ski_parquet.exists():
            print(f"[{region}] ski_areas.parquet missing; restoring it from the PBF", flush=True)
            restore_ski_areas(pbf, work, ski_parquet, boundaries, cache)
        extract_lifts_and_pistes(pbf, work)
        ski_geojson = work / "ski_areas.geojson"
        gpd.read_parquet(ski_parquet).to_file(ski_geojson, driver="GeoJSON")
        for name in ("lifts", "pistes"):
            src = work / f"{name}.geojson"
            enrich_geojson(src, boundaries, ski_geojson, is_ski_areas_file=False, boundaries_cache=cache)
            geojson_to_geoparquet(src, work / f"{name}.parquet")
        shutil.copy2(ski_parquet, work / "ski_areas.parquet")
        normalize_parquet_dir(work)
        counts = []
        for name in ("lifts", "pistes"):
            gdf = gpd.read_parquet(work / f"{name}.parquet", columns=["geometry"])
            counts.append(f"{name} {len(gdf)} ({int(gdf.geom_type.eq('LineString').sum())} lines)")
            (work / f"{name}.parquet").replace(out / f"{name}.parquet")
    finally:
        shutil.rmtree(work, ignore_errors=True)
        if not keep_pbf:
            pbf.unlink(missing_ok=True)
    return f"{region}: {', '.join(counts)} in {time.perf_counter() - start:.0f}s"


def main() -> int:
    ap = argparse.ArgumentParser(description="Rebuild lifts.parquet and pistes.parquet for pipeline regions")
    ap.add_argument("--continent", help="Only regions under this continent (e.g. europe, asia)")
    ap.add_argument("--region", action="append", help="Exact region path (repeatable), e.g. europe/austria/tirol")
    ap.add_argument("--missing-lines-only", action="store_true", help="Skip regions whose lifts/pistes already have LineStrings")
    ap.add_argument("-o", "--output-dir", default="output", help="Base output dir with region folders (default: output)")
    ap.add_argument("--pbf-dir", default="output/_pbf_cache", help="Where PBFs are downloaded")
    ap.add_argument("-b", "--boundaries", default="boundaries", help="Natural Earth boundaries dir")
    ap.add_argument("--keep-pbf", action="store_true", help="Keep downloaded PBFs")
    ap.add_argument("--workers", type=int, default=1, help="Regions processed in parallel")
    args = ap.parse_args()

    output_dir = Path(args.output_dir)
    rows = load_region_rows()
    if args.continent:
        cont = args.continent.strip().lower()
        rows = [r for r in rows if r[0] == cont or r[0].startswith(cont + "/")]
    if args.region:
        wanted = set(args.region)
        rows = [r for r in rows if r[0] in wanted]
    if args.missing_lines_only:
        rows = [r for r in rows if not has_lines(output_dir / Path(*r[0].split("/")))]
    if not rows:
        print("No regions selected.")
        return 0
    print(f"Rebuilding lifts/pistes for {len(rows)} region(s)", flush=True)

    jobs = [
        (region, url, output_dir, Path(args.pbf_dir), Path(args.boundaries), args.keep_pbf)
        for region, url, _ in rows
    ]
    failed = []
    with ProcessPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(rebuild_region, *job): job[0] for job in jobs}
        for n, fut in enumerate(as_completed(futures), 1):
            region = futures[fut]
            try:
                print(f"[{n}/{len(jobs)}] {fut.result()}", flush=True)
            except Exception:
                failed.append(region)
                print(f"[{n}/{len(jobs)}] {region}: FAILED\n{traceback.format_exc()}", file=sys.stderr, flush=True)
    if failed:
        print(f"Failed regions ({len(failed)}): {' '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
