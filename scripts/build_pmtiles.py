#!/usr/bin/env python3
"""
Build overview and resort-detail PMTiles from combined GeoParquet.

1. Exports GeoJSON via export_combined_to_geojson.py and export_resort_detail_to_geojson.py
2. Runs tippecanoe (https://github.com/felt/tippecanoe) to write .pmtiles

Tippecanoe on Windows: the PyPI package has no Windows wheels. This script uses native
`tippecanoe` when it is on PATH; otherwise it runs tippecanoe inside Docker if `docker` is
available (default image: ghcr.io/versatiles-org/versatiles-tippecanoe:2.79.0).

  docker pull ghcr.io/versatiles-org/versatiles-tippecanoe:2.79.0

On macOS/Linux you can also: pipx install tippecanoe

Example:
  python scripts/build_pmtiles.py
  python scripts/build_pmtiles.py --strip-osm-tags
  python scripts/build_pmtiles.py --skip-export --overview-only
  python scripts/build_pmtiles.py --tippecanoe-docker

Resort tileset (--resort-min-zoom / --resort-max-zoom): planet-scale OSM (~millions of
features) at -z17 can run for days; the progress line often sits near ~85% while encoding
one zoom level. Use defaults (12–15) for full-world builds; raise max zoom only for small
extracts or expect very long runs.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = Path(__file__).resolve().parent

DEFAULT_TIPPECANOE_DOCKER_IMAGE = "ghcr.io/versatiles-org/versatiles-tippecanoe:2.79.0"

# Full-planet resort OSM: z17 explodes tile count and encoding time; 12–15 is workable.
DEFAULT_RESORT_MIN_ZOOM = 12
DEFAULT_RESORT_MAX_ZOOM = 15

OVERVIEW_LAYERS: list[tuple[str, str]] = [
    ("lifts", "lifts.geojson"),
    ("pistes", "pistes.geojson"),
    ("ski_areas", "ski_areas.geojson"),
    ("ski_areas_analyzed", "ski_areas_analyzed.geojson"),
]

RESORT_LAYERS: list[tuple[str, str]] = [
    ("osm", "osm_near_winter_sports.geojson"),
    ("buffer", "ski_areas_1000ft_buffer.geojson"),
    ("contours", "ski_area_contours.geojson"),
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
                "Use default --staging-dir / --output-dir under the repo, or install native tippecanoe."
            ) from None
    return tuple(out)


def _tippecanoe_run(
    *,
    use_docker: bool,
    docker_image: str,
    native_bin: str,
    repo_root: Path,
    staging_dir: Path,
    out_file: Path,
    layers: list[tuple[str, str]],
    extra_flags: list[str],
    empty_message: str,
) -> None:
    staging_dir = staging_dir.resolve()
    out_file = out_file.resolve()
    out_file.parent.mkdir(parents=True, exist_ok=True)

    layer_args: list[str] = []
    n = 0
    for layer_name, filename in layers:
        if (staging_dir / filename).exists():
            layer_args.extend(["-L", f"{layer_name}:{filename}"])
            n += 1
    if n == 0:
        print(empty_message, file=sys.stderr)
        sys.exit(1)

    if use_docker:
        rel_staging, rel_out = _paths_under_repo(repo_root, staging_dir, out_file)
        # Default image (versatiles-tippecanoe) sets ENTRYPOINT to tippecanoe; do not pass "tippecanoe" again
        # or it is treated as an input filename.
        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{repo_root.resolve()}:/work",
            "-w",
            f"/work/{rel_staging}",
            docker_image,
            "-f",
            "-o",
            f"/work/{rel_out}",
            *extra_flags,
            *layer_args,
        ]
        _run(cmd)
    else:
        cmd = [native_bin, "-f", "-o", str(out_file), *extra_flags, *layer_args]
        _run(cmd, cwd=staging_dir)


def _tippecanoe_overview(
    *,
    use_docker: bool,
    docker_image: str,
    native_bin: str,
    repo_root: Path,
    staging_dir: Path,
    out_file: Path,
) -> None:
    _tippecanoe_run(
        use_docker=use_docker,
        docker_image=docker_image,
        native_bin=native_bin,
        repo_root=repo_root,
        staging_dir=staging_dir,
        out_file=out_file,
        layers=OVERVIEW_LAYERS,
        extra_flags=[
            "-Z0",
            "-z14",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
        ],
        empty_message="No overview GeoJSON found in staging; aborting tippecanoe overview",
    )


def _tippecanoe_resort(
    *,
    use_docker: bool,
    docker_image: str,
    native_bin: str,
    repo_root: Path,
    staging_dir: Path,
    out_file: Path,
    min_zoom: int,
    max_zoom: int,
) -> None:
    _tippecanoe_run(
        use_docker=use_docker,
        docker_image=docker_image,
        native_bin=native_bin,
        repo_root=repo_root,
        staging_dir=staging_dir,
        out_file=out_file,
        layers=RESORT_LAYERS,
        extra_flags=[
            f"-Z{min_zoom}",
            f"-z{max_zoom}",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--simplification=10",
            "--read-parallel",
        ],
        empty_message="No resort GeoJSON found in staging; aborting tippecanoe resort",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export combined data and build ski_overview + ski_resort_detail PMTiles")
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
        help="Staging root for GeoJSON (default: output/pmtiles_staging)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "output" / "pmtiles",
        help="Output directory for .pmtiles (default: output/pmtiles)",
    )
    parser.add_argument(
        "--tippecanoe",
        default="tippecanoe",
        help="Native tippecanoe executable when not using Docker (default: tippecanoe)",
    )
    parser.add_argument(
        "--tippecanoe-docker",
        action="store_true",
        help="Run tippecanoe via Docker instead of PATH (see --tippecanoe-docker-image)",
    )
    parser.add_argument(
        "--tippecanoe-docker-image",
        default=DEFAULT_TIPPECANOE_DOCKER_IMAGE,
        help=f"Image for --tippecanoe-docker (default: {DEFAULT_TIPPECANOE_DOCKER_IMAGE})",
    )
    parser.add_argument("--skip-export", action="store_true", help="Reuse existing staging GeoJSON")
    parser.add_argument("--overview-only", action="store_true")
    parser.add_argument("--resort-only", action="store_true")
    parser.add_argument(
        "--strip-osm-tags",
        action="store_true",
        help="Pass to resort export: drop OSM tags column",
    )
    parser.add_argument(
        "--truncate-osm-tags",
        type=int,
        default=None,
        metavar="N",
        help="Pass to resort export: truncate tags to N characters",
    )
    parser.add_argument(
        "--resort-min-zoom",
        type=int,
        default=DEFAULT_RESORT_MIN_ZOOM,
        metavar="Z",
        help=f"Resort PMTiles minimum zoom (default: {DEFAULT_RESORT_MIN_ZOOM}; higher = skip heavy low-zoom tiles)",
    )
    parser.add_argument(
        "--resort-max-zoom",
        type=int,
        default=DEFAULT_RESORT_MAX_ZOOM,
        metavar="z",
        help=f"Resort PMTiles maximum zoom (default: {DEFAULT_RESORT_MAX_ZOOM}; z17+ on global OSM can take days)",
    )
    args = parser.parse_args()

    if args.resort_min_zoom < 0 or args.resort_max_zoom < 0 or args.resort_min_zoom > args.resort_max_zoom:
        print("--resort-min-zoom and --resort-max-zoom must satisfy 0 <= min <= max", file=sys.stderr)
        sys.exit(2)

    if args.overview_only and args.resort_only:
        print("Choose at most one of --overview-only / --resort-only", file=sys.stderr)
        sys.exit(2)

    native_tippecanoe = shutil.which(args.tippecanoe)
    docker_ok = shutil.which("docker") is not None
    use_docker = args.tippecanoe_docker or (native_tippecanoe is None and docker_ok)

    if use_docker and not docker_ok:
        print("Docker was requested but `docker` is not on PATH.", file=sys.stderr)
        sys.exit(1)
    if not use_docker and native_tippecanoe is None:
        print(
            f"tippecanoe not found ({args.tippecanoe!r}) and `docker` is not on PATH.\n"
            "Install tippecanoe (https://github.com/felt/tippecanoe), or install Docker and run:\n"
            f"  docker pull {args.tippecanoe_docker_image}\n"
            "then re-run this script (Docker is used automatically when tippecanoe is missing).",
            file=sys.stderr,
        )
        sys.exit(1)
    if use_docker:
        print(f"Using tippecanoe in Docker: {args.tippecanoe_docker_image}")

    input_dir: Path = args.input_dir
    staging_root: Path = args.staging_dir
    staging_overview = staging_root / "overview"
    staging_resort = staging_root / "resort"
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    do_overview = not args.resort_only
    do_resort = not args.overview_only

    if not args.skip_export:
        if not input_dir.exists():
            print(f"Input directory not found: {input_dir}", file=sys.stderr)
            sys.exit(1)
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

    if do_overview:
        _tippecanoe_overview(
            use_docker=use_docker,
            docker_image=args.tippecanoe_docker_image,
            native_bin=args.tippecanoe,
            repo_root=REPO_ROOT,
            staging_dir=staging_overview,
            out_file=output_dir / "ski_overview.pmtiles",
        )
        print(f"Wrote {output_dir / 'ski_overview.pmtiles'}")

    if do_resort:
        print(
            f"Resort tippecanoe zooms: -Z{args.resort_min_zoom} -z{args.resort_max_zoom} "
            f"(override with --resort-min-zoom / --resort-max-zoom if needed)",
            file=sys.stderr,
        )
        _tippecanoe_resort(
            use_docker=use_docker,
            docker_image=args.tippecanoe_docker_image,
            native_bin=args.tippecanoe,
            repo_root=REPO_ROOT,
            staging_dir=staging_resort,
            out_file=output_dir / "ski_resort_detail.pmtiles",
            min_zoom=args.resort_min_zoom,
            max_zoom=args.resort_max_zoom,
        )
        print(f"Wrote {output_dir / 'ski_resort_detail.pmtiles'}")


if __name__ == "__main__":
    main()
