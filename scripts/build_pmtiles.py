#!/usr/bin/env python3
"""
Build overview and resort-detail PMTiles from combined GeoParquet via Planetiler.

Reads GeoParquet from output/combined/ via Planetiler-safe copies in output/pmtiles_staging/
(default). Combined parquet uses nullable column types Planetiler cannot read directly;
staging rewrites attributes as strings. Also materializes ski_areas_analyzed centroid
points into staging because that table has no geometry column.

Uses Planetiler (https://github.com/onthegomap/planetiler) — not tippecanoe.
On first run, downloads planetiler.jar to tools/planetiler/. Uses native Java when
available; otherwise runs Java inside Docker (eclipse-temurin:21-jre).

Example:
  python scripts/build_pmtiles.py
  python scripts/build_pmtiles.py --strip-osm-tags
  python scripts/build_pmtiles.py --skip-export --overview-only
  python scripts/build_pmtiles.py --from-geojson
  python scripts/build_pmtiles.py --planetiler-docker

Resort tileset (--resort-min-zoom / --resort-max-zoom): planet-scale OSM at z17+
can run for days. Defaults are z12–15 for full-world builds.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = Path(__file__).resolve().parent
PMTILES_PROFILE = SCRIPTS_DIR / "pmtiles" / "SkiAtlasTiles.java"

PLANETILER_VERSION = "0.10.2"
PLANETILER_JAR_URL = (
    f"https://github.com/onthegomap/planetiler/releases/download/v{PLANETILER_VERSION}/planetiler.jar"
)
DEFAULT_PLANETILER_JAR = REPO_ROOT / "tools" / "planetiler" / "planetiler.jar"
DEFAULT_JAVA_DOCKER_IMAGE = "eclipse-temurin:21-jre"

DEFAULT_RESORT_MIN_ZOOM = 12
DEFAULT_RESORT_MAX_ZOOM = 15
OVERVIEW_MIN_ZOOM = 0
OVERVIEW_MAX_ZOOM = 14
# Layer min-zoom for overview: resort points when zoomed out; geometry as you zoom in.
DEFAULT_ANALYZED_MIN_ZOOM = 0
DEFAULT_SKI_AREAS_MIN_ZOOM = 8
DEFAULT_PISTES_MIN_ZOOM = 10
DEFAULT_LIFTS_MIN_ZOOM = 10
DEFAULT_BUFFER_MIN_ZOOM = 12
DEFAULT_OSM_MIN_ZOOM = 12
DEFAULT_CONTOURS_MIN_ZOOM = 13

OVERVIEW_PARQUET_SOURCES: list[tuple[str, str]] = [
    ("lifts", "lifts.parquet"),
    ("pistes", "pistes.parquet"),
    ("ski_areas", "ski_areas.parquet"),
]

RESORT_PARQUET_SOURCES: list[tuple[str, str]] = [
    ("osm", "osm_near_winter_sports.parquet"),
    ("buffer", "ski_areas_1000ft_buffer.parquet"),
    ("contours", "ski_area_contours.parquet"),
]


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    print("+", " ".join(str(c) for c in cmd))
    subprocess.run(cmd, cwd=cwd, check=True)


def _paths_under_repo(repo: Path, *paths: Path) -> tuple[str, ...]:
    """Return POSIX paths relative to repo for use as /work/... inside Docker."""
    out = []
    for p in paths:
        try:
            out.append(p.resolve().relative_to(repo.resolve()).as_posix())
        except ValueError:
            raise SystemExit(
                f"Docker mode requires paths under the repo root:\n  {p}\nnot under\n  {repo}\n"
                "Use default --staging-dir / --output-dir under the repo, or install native Java."
            ) from None
    return tuple(out)


def ensure_planetiler_jar(jar_path: Path) -> Path:
    if jar_path.is_file():
        return jar_path
    jar_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading Planetiler v{PLANETILER_VERSION} -> {jar_path}")
    urllib.request.urlretrieve(PLANETILER_JAR_URL, jar_path)
    return jar_path


def materialize_analyzed_points(
    input_dir: Path,
    out_path: Path,
    *,
    force: bool = False,
) -> int:
    """Write centroid points from tabular ski_areas_analyzed.parquet to GeoParquet."""
    src = input_dir / "ski_areas_analyzed.parquet"
    if not src.exists():
        print(f"  ski_areas_analyzed.parquet not found in {input_dir}; skipping analyzed points")
        return 0
    if out_path.exists() and not force:
        print(f"  Reusing staged analyzed points: {out_path}")
        return -1

    df = pd.read_parquet(src)
    if "centroid_lon" not in df.columns or "centroid_lat" not in df.columns:
        print("  ski_areas_analyzed: no centroid columns, skipping", file=sys.stderr)
        return 0

    valid = df["centroid_lon"].notna() & df["centroid_lat"].notna()
    df = df[valid].copy()
    geometry = [Point(lon, lat) for lon, lat in zip(df["centroid_lon"], df["centroid_lat"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(out_path)
    print(f"  ski_areas_analyzed points: {len(gdf)} features -> {out_path}")
    return len(gdf)


def materialize_planetiler_parquet(
    src: Path,
    dst: Path,
    *,
    force: bool = False,
) -> bool:
    """Copy GeoParquet to staging with string-typed attributes Planetiler can read."""
    if dst.exists() and not force:
        print(f"  Reusing staged parquet: {dst.name}")
        return True
    if not src.exists():
        print(f"  Skipping {src.name} (not found)")
        return False

    gdf = gpd.read_parquet(src)
    if not gdf.crs or gdf.crs.to_epsg() != 4326:
        if gdf.crs:
            gdf = gdf.to_crs("EPSG:4326")
        else:
            gdf = gdf.set_crs("EPSG:4326")
    for col in list(gdf.columns):
        if col == "geometry":
            continue
        gdf[col] = gdf[col].astype("string")
    dst.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(dst)
    print(f"  {dst.name}: {len(gdf)} features (from {src.name})")
    return True


def materialize_tileset_parquet(
    input_dir: Path,
    staging_dir: Path,
    layers: list[tuple[str, str]],
    *,
    force: bool = False,
) -> int:
    """Stage Planetiler-safe GeoParquet copies. Returns count of layers written."""
    count = 0
    for source_name, combined_name in layers:
        src = input_dir / combined_name
        dst = staging_dir / f"{source_name}.parquet"
        if materialize_planetiler_parquet(src, dst, force=force):
            count += 1
    return count


def _planetiler_cmd(
    *,
    use_docker: bool,
    docker_image: str,
    java_heap: str,
    jar_path: Path,
    repo_root: Path,
    profile_path: Path,
    planetiler_args: list[str],
) -> list[str]:
    java_opts = f"-Xmx{java_heap}"
    if use_docker:
        rel_jar, rel_profile = _paths_under_repo(repo_root, jar_path, profile_path)
        inner = [
            "java",
            java_opts,
            "-cp",
            f"/work/{rel_jar}",
            f"/work/{rel_profile}",
            *planetiler_args,
        ]
        return [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{repo_root.resolve()}:/work",
            "-w",
            "/work",
            docker_image,
            *inner,
        ]

    java_bin = shutil.which("java")
    if java_bin is None:
        raise SystemExit(
            "Java not found on PATH and Docker was not used.\n"
            "Install Java 21+, or install Docker and re-run (Docker is used automatically when java is missing)."
        )
    return [
        java_bin,
        java_opts,
        "-cp",
        str(jar_path.resolve()),
        str(profile_path.resolve()),
        *planetiler_args,
    ]


def _planetiler_run(
    *,
    tileset: str,
    input_dir: Path,
    staging_dir: Path,
    output_file: Path,
    analyzed_path: Path,
    min_zoom: int,
    max_zoom: int,
    from_geojson: bool,
    strip_osm_tags: bool,
    use_docker: bool,
    docker_image: str,
    java_heap: str,
    jar_path: Path,
    extra_args: list[str] | None = None,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    args = [
        f"--tileset={tileset}",
        f"--input-dir={input_dir.resolve()}",
        f"--staging-dir={staging_dir.resolve()}",
        f"--analyzed-path={analyzed_path.resolve()}",
        f"--output={output_file.resolve()}",
        f"--minzoom={min_zoom}",
        f"--maxzoom={max_zoom}",
        "--force",
    ]
    if extra_args:
        args.extend(extra_args)
    if from_geojson:
        args.append("--from-geojson=true")
    if strip_osm_tags:
        args.append("--strip-osm-tags=true")

    cmd = _planetiler_cmd(
        use_docker=use_docker,
        docker_image=docker_image,
        java_heap=java_heap,
        jar_path=jar_path,
        repo_root=REPO_ROOT,
        profile_path=PMTILES_PROFILE,
        planetiler_args=args,
    )
    _run(cmd, cwd=REPO_ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build ski_overview + ski_resort_detail PMTiles from combined GeoParquet via Planetiler",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=REPO_ROOT / "output" / "combined",
        help="Combined parquet directory (default: output/combined)",
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=REPO_ROOT / "output" / "pmtiles_staging",
        help="Staging root (default: output/pmtiles_staging)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "output" / "pmtiles",
        help="Output directory for .pmtiles (default: output/pmtiles)",
    )
    parser.add_argument(
        "--planetiler-jar",
        type=Path,
        default=DEFAULT_PLANETILER_JAR,
        help=f"Path to planetiler.jar (default: {DEFAULT_PLANETILER_JAR.relative_to(REPO_ROOT)})",
    )
    parser.add_argument(
        "--planetiler-docker",
        action="store_true",
        help="Run Planetiler via Docker instead of native Java",
    )
    parser.add_argument(
        "--planetiler-docker-image",
        default=DEFAULT_JAVA_DOCKER_IMAGE,
        help=f"JRE Docker image for --planetiler-docker (default: {DEFAULT_JAVA_DOCKER_IMAGE})",
    )
    parser.add_argument(
        "--java-heap",
        default="8g",
        metavar="SIZE",
        help="Java heap for Planetiler (default: 8g)",
    )
    parser.add_argument(
        "--from-geojson",
        action="store_true",
        help="Export/read GeoJSON from staging instead of reading GeoParquet directly",
    )
    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip GeoJSON export (--from-geojson) and reuse existing staging; still materializes analyzed points if missing",
    )
    parser.add_argument("--overview-only", action="store_true")
    parser.add_argument("--resort-only", action="store_true")
    parser.add_argument(
        "--strip-osm-tags",
        action="store_true",
        help="Drop OSM tags column from osm layer in resort tileset",
    )
    parser.add_argument(
        "--truncate-osm-tags",
        type=int,
        default=None,
        metavar="N",
        help="Pass to resort GeoJSON export: truncate tags to N characters",
    )
    parser.add_argument(
        "--resort-min-zoom",
        type=int,
        default=DEFAULT_RESORT_MIN_ZOOM,
        metavar="Z",
        help=f"Resort PMTiles minimum zoom (default: {DEFAULT_RESORT_MIN_ZOOM})",
    )
    parser.add_argument(
        "--resort-max-zoom",
        type=int,
        default=DEFAULT_RESORT_MAX_ZOOM,
        metavar="z",
        help=f"Resort PMTiles maximum zoom (default: {DEFAULT_RESORT_MAX_ZOOM})",
    )
    parser.add_argument(
        "--analyzed-min-zoom",
        type=int,
        default=DEFAULT_ANALYZED_MIN_ZOOM,
        metavar="Z",
        help=f"Overview: min zoom for ski_areas_analyzed resort points (default: {DEFAULT_ANALYZED_MIN_ZOOM})",
    )
    parser.add_argument(
        "--ski-areas-min-zoom",
        type=int,
        default=DEFAULT_SKI_AREAS_MIN_ZOOM,
        metavar="Z",
        help=f"Overview: min zoom for ski_areas polygons (default: {DEFAULT_SKI_AREAS_MIN_ZOOM})",
    )
    parser.add_argument(
        "--pistes-min-zoom",
        type=int,
        default=DEFAULT_PISTES_MIN_ZOOM,
        metavar="Z",
        help=f"Overview: min zoom for pistes lines/polygons/points (default: {DEFAULT_PISTES_MIN_ZOOM})",
    )
    parser.add_argument(
        "--lifts-min-zoom",
        type=int,
        default=DEFAULT_LIFTS_MIN_ZOOM,
        metavar="Z",
        help=f"Overview: min zoom for lifts (default: {DEFAULT_LIFTS_MIN_ZOOM})",
    )
    parser.add_argument(
        "--buffer-min-zoom",
        type=int,
        default=DEFAULT_BUFFER_MIN_ZOOM,
        metavar="Z",
        help=f"Resort: min zoom for ski area buffers (default: {DEFAULT_BUFFER_MIN_ZOOM})",
    )
    parser.add_argument(
        "--osm-min-zoom",
        type=int,
        default=DEFAULT_OSM_MIN_ZOOM,
        metavar="Z",
        help=f"Resort: min zoom for nearby OSM (default: {DEFAULT_OSM_MIN_ZOOM})",
    )
    parser.add_argument(
        "--contours-min-zoom",
        type=int,
        default=DEFAULT_CONTOURS_MIN_ZOOM,
        metavar="Z",
        help=f"Resort: min zoom for elevation contours (default: {DEFAULT_CONTOURS_MIN_ZOOM})",
    )
    args = parser.parse_args()

    if args.resort_min_zoom < 0 or args.resort_max_zoom < 0 or args.resort_min_zoom > args.resort_max_zoom:
        print("--resort-min-zoom and --resort-max-zoom must satisfy 0 <= min <= max", file=sys.stderr)
        sys.exit(2)

    if args.overview_only and args.resort_only:
        print("Choose at most one of --overview-only / --resort-only", file=sys.stderr)
        sys.exit(2)

    java_ok = shutil.which("java") is not None
    docker_ok = shutil.which("docker") is not None
    use_docker = args.planetiler_docker or (not java_ok and docker_ok)

    if use_docker and not docker_ok:
        print("Docker was requested but `docker` is not on PATH.", file=sys.stderr)
        sys.exit(1)
    if not use_docker and not java_ok:
        print(
            "Java not found on PATH and `docker` is not on PATH.\n"
            "Install Java 21+ (https://adoptium.net/) or Docker, then re-run.",
            file=sys.stderr,
        )
        sys.exit(1)
    if use_docker:
        print(f"Using Planetiler in Docker ({args.planetiler_docker_image})")
    else:
        print("Using native Java for Planetiler")

    input_dir: Path = args.input_dir
    staging_root: Path = args.staging_dir
    staging_overview = staging_root / "overview"
    staging_resort = staging_root / "resort"
    output_dir: Path = args.output_dir
    analyzed_path = staging_overview / "ski_areas_analyzed.geoparquet"
    output_dir.mkdir(parents=True, exist_ok=True)

    do_overview = not args.resort_only
    do_resort = not args.overview_only

    if not input_dir.exists():
        print(f"Input directory not found: {input_dir}", file=sys.stderr)
        sys.exit(1)

    if args.from_geojson and not args.skip_export:
        if do_overview:
            _run(
                [
                    sys.executable,
                    str(SCRIPTS_DIR / "export_combined_to_geojson.py"),
                    "-i",
                    str(input_dir.resolve()),
                    "-o",
                    str(staging_overview.resolve()),
                ],
                cwd=REPO_ROOT,
            )
        if do_resort:
            resort_cmd = [
                sys.executable,
                str(SCRIPTS_DIR / "export_resort_detail_to_geojson.py"),
                "-i",
                str(input_dir.resolve()),
                "-o",
                str(staging_resort.resolve()),
            ]
            if args.strip_osm_tags:
                resort_cmd.append("--strip-osm-tags")
            if args.truncate_osm_tags is not None:
                resort_cmd.extend(["--truncate-osm-tags", str(args.truncate_osm_tags)])
            _run(resort_cmd, cwd=REPO_ROOT)
    elif do_overview and not args.from_geojson:
        materialize_tileset_parquet(
            input_dir,
            staging_overview,
            OVERVIEW_PARQUET_SOURCES,
            force=not args.skip_export,
        )
        materialize_analyzed_points(
            input_dir,
            analyzed_path,
            force=not args.skip_export,
        )
    elif do_resort and not args.from_geojson:
        materialize_tileset_parquet(
            input_dir,
            staging_resort,
            RESORT_PARQUET_SOURCES,
            force=not args.skip_export,
        )

    jar_path = ensure_planetiler_jar(args.planetiler_jar)

    if do_overview:
        print("Building ski_overview.pmtiles ...")
        _planetiler_run(
            tileset="overview",
            input_dir=input_dir,
            staging_dir=staging_root,
            output_file=output_dir / "ski_overview.pmtiles",
            analyzed_path=analyzed_path,
            min_zoom=OVERVIEW_MIN_ZOOM,
            max_zoom=OVERVIEW_MAX_ZOOM,
            from_geojson=args.from_geojson,
            strip_osm_tags=False,
            use_docker=use_docker,
            docker_image=args.planetiler_docker_image,
            java_heap=args.java_heap,
            jar_path=jar_path,
            extra_args=[
                f"--analyzed-min-zoom={args.analyzed_min_zoom}",
                f"--ski-areas-min-zoom={args.ski_areas_min_zoom}",
                f"--pistes-min-zoom={args.pistes_min_zoom}",
                f"--lifts-min-zoom={args.lifts_min_zoom}",
            ],
        )
        print(f"Wrote {output_dir / 'ski_overview.pmtiles'}")

    if do_resort:
        print(
            f"Building ski_resort_detail.pmtiles (z{args.resort_min_zoom}-z{args.resort_max_zoom}) ...",
            file=sys.stderr,
        )
        _planetiler_run(
            tileset="resort",
            input_dir=input_dir,
            staging_dir=staging_root,
            output_file=output_dir / "ski_resort_detail.pmtiles",
            analyzed_path=analyzed_path,
            min_zoom=args.resort_min_zoom,
            max_zoom=args.resort_max_zoom,
            from_geojson=args.from_geojson,
            strip_osm_tags=args.strip_osm_tags,
            use_docker=use_docker,
            docker_image=args.planetiler_docker_image,
            java_heap=args.java_heap,
            jar_path=jar_path,
            extra_args=[
                f"--buffer-min-zoom={args.buffer_min_zoom}",
                f"--osm-min-zoom={args.osm_min_zoom}",
                f"--contours-min-zoom={args.contours_min_zoom}",
            ],
        )
        print(f"Wrote {output_dir / 'ski_resort_detail.pmtiles'}")


if __name__ == "__main__":
    main()
