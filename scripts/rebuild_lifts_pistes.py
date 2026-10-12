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
import tempfile
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


MIN_BYTES_PER_SEC = 200_000
SLOW_GRACE_SEC = 300


def download(url: str, dest: Path, attempts: int = 3) -> None:
    """Download with retries; abandon an attempt that stalls or trickles below MIN_BYTES_PER_SEC."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    req = urllib.request.Request(url, headers={"User-Agent": "globalskiatlas-rebuild-lifts/1.0"})
    for attempt in range(1, attempts + 1):
        start = time.monotonic()
        got = 0
        try:
            with urllib.request.urlopen(req, timeout=120) as resp, open(tmp, "wb") as f:
                while True:
                    chunk = resp.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    got += len(chunk)
                    elapsed = time.monotonic() - start
                    if elapsed > SLOW_GRACE_SEC and got / elapsed < MIN_BYTES_PER_SEC:
                        raise TimeoutError(f"{got / elapsed / 1e3:.0f} kB/s after {elapsed:.0f}s")
            tmp.replace(dest)
            return
        except Exception as e:
            _remove(tmp)
            print(f"download {url} attempt {attempt}/{attempts} failed: {e}", flush=True)
            if attempt == attempts:
                raise
            time.sleep(30)


def _remove(path: Path) -> None:
    """Best-effort delete; antivirus or a lingering GDAL handle can hold a file briefly on Windows."""
    for _ in range(5):
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
            return
        except OSError:
            time.sleep(2)
    print(f"could not remove {path}; leaving it", flush=True)


def restore_ski_areas(pbf: Path, work: Path, ski_parquet: Path, boundaries: Path, cache) -> None:
    """Regions whose ski_areas.parquet was never written: rebuild it from the PBF (as pbf_to_geojson's GDAL path)."""
    import pyogrio

    ski_geojson = work / "ski_areas_restore.geojson"
    gdf = pyogrio.read_dataframe(str(pbf), sql="SELECT * FROM multipolygons WHERE landuse='winter_sports'")
    gdf.dropna(axis=1, how="all").to_file(ski_geojson, driver="GeoJSON")
    enrich_geojson(ski_geojson, boundaries, None, is_ski_areas_file=True, boundaries_cache=cache)
    geojson_to_geoparquet(ski_geojson, ski_parquet)


def rebuild_region(
    region: str, pbf_url: str, output_dir: Path, pbf_dir: Path, work_root: Path, boundaries: Path, keep_pbf: bool
) -> str:
    start = time.perf_counter()
    out = output_dir / Path(*region.split("/"))
    ski_parquet = out / "ski_areas.parquet"
    if not ski_parquet.exists() and not (out / "ski_areas_analyzed.parquet").exists():
        return f"{region}: skipped (region was never processed)"

    slug = region.replace("/", "__")
    pbf = pbf_dir / f"{slug}.osm.pbf"
    if not pbf.exists():
        print(f"[{region}] downloading {pbf_url}", flush=True)
        download(pbf_url, pbf)

    work = work_root / slug
    _remove(work)
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
        for name in ("lifts", "pistes"):
            shutil.copyfile(work / f"{name}.parquet", out / f"{name}.parquet")
    finally:
        _remove(work)
        if not keep_pbf:
            _remove(pbf)
    return f"{region}: {', '.join(counts)} in {time.perf_counter() - start:.0f}s"


def main() -> int:
    ap = argparse.ArgumentParser(description="Rebuild lifts.parquet and pistes.parquet for pipeline regions")
    ap.add_argument("--continent", help="Only regions under this continent (e.g. europe, asia)")
    ap.add_argument("--region", action="append", help="Exact region path (repeatable), e.g. europe/austria/tirol")
    ap.add_argument("--missing-lines-only", action="store_true", help="Skip regions whose lifts/pistes already have LineStrings")
    ap.add_argument("-o", "--output-dir", default="output", help="Base output dir with region folders (default: output)")
    scratch = Path(tempfile.gettempdir()) / "gsa_rebuild_lifts_pistes"
    ap.add_argument("--pbf-dir", default=str(scratch / "pbf"), help="Where PBFs are downloaded")
    ap.add_argument("--work-dir", default=str(scratch / "work"), help="Scratch dir for intermediate GeoJSON")
    ap.add_argument("-b", "--boundaries", default="boundaries", help="Natural Earth boundaries dir")
    ap.add_argument("--keep-pbf", action="store_true", help="Keep downloaded PBFs")
    ap.add_argument("--workers", type=int, default=1, help="Regions processed in parallel")
    args = ap.parse_args()

    output_dir = Path(args.output_dir).resolve()
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

    dirs = (Path(args.pbf_dir).resolve(), Path(args.work_dir).resolve(), Path(args.boundaries).resolve())
    jobs = [(region, url, output_dir, *dirs, args.keep_pbf) for region, url, _ in rows]
    failed = []

    def report(n: int, region: str, run) -> None:
        try:
            print(f"[{n}/{len(jobs)}] {run()}", flush=True)
        except Exception:
            failed.append(region)
            print(f"[{n}/{len(jobs)}] {region}: FAILED\n{traceback.format_exc()}", file=sys.stderr, flush=True)

    if args.workers <= 1:
        for n, job in enumerate(jobs, 1):
            report(n, job[0], lambda: rebuild_region(*job))
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(rebuild_region, *job): job[0] for job in jobs}
            for n, fut in enumerate(as_completed(futures), 1):
                report(n, futures[fut], fut.result)
    if failed:
        print(f"Failed regions ({len(failed)}): {' '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
