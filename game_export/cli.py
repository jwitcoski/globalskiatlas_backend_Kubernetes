"""game_export CLI — optional additive stage."""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from shapely.geometry import mapping

from game_export.config import REPO_ROOT, default_config_path, load_resort_config
from game_export.coords import geom_to_local, geom_to_projected
from game_export.hazards import build_exclusions, piste_corridors
from game_export.inputs import resolve_inputs, scene_version
from game_export.manifest import write_attribution, write_manifest
from game_export.qa import validation_payload, write_overview_maps, write_reports, write_route_profiles
from game_export.routes import build_routes, write_json
from game_export.s3_inputs import default_s3_bucket
from game_export.terrain import export_terrain, sample_elev
from game_export.vectors import add_ski_area_polygon, collect_layers, write_local_geojson
from game_export.catalog import write_catalog

log = logging.getLogger("game_export")


@dataclass
class ExportResult:
    code: int
    scene_dir: Path | None = None
    approved: int = 0


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Export a browser-ready game scene from existing OSM + DEM pipeline outputs"
    )
    p.add_argument("--resort", default="montage_mountain_pa", help="config/resorts/<id>.yaml")
    p.add_argument("--config", type=Path, default=None, help="Override YAML path")
    p.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="Local pipeline output root. Implies --no-from-s3 unless --from-s3 is also set.",
    )
    p.add_argument("--cache-dir", type=Path, default=None, help="Skadi / DEM / S3 cache (default: ./cache)")
    p.add_argument(
        "--out-root",
        type=Path,
        default=None,
        help="Game scene root (default: output/game_scenes; with --data-root, <data-root>/game_scenes)",
    )
    p.add_argument("--dry-run", action="store_true", help="Resolve inputs and print plan only")
    p.add_argument("--force", action="store_true", help="Rebuild even if this scene_version directory exists")
    p.add_argument(
        "--from-s3",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Read GIS parquet/DEMs from S3 (default: on when --data-root is omitted)",
    )
    p.add_argument(
        "--s3-bucket",
        default=None,
        help="Pipeline output bucket (default: GAME_EXPORT_S3_BUCKET / S3_BUCKET / globalskiatlas-backend-k8s-output)",
    )
    p.add_argument(
        "--picked-batch",
        action="store_true",
        help="Export the 10 picked playable resorts plus Montage (not the full atlas)",
    )
    p.add_argument(
        "--fetch-skadi",
        action="store_true",
        help="If no GeoTIFF exists, download Mapzen Skadi tiles using the existing elevation helper",
    )
    p.add_argument(
        "--catalog-only",
        action="store_true",
        help="Rewrite output/game_scenes/catalog.json from local scene cakes",
    )
    p.add_argument(
        "--skip-catalog",
        action="store_true",
        help="Do not rewrite the local catalog after this export (batch uploader owns S3 catalog)",
    )
    p.add_argument(
        "--winter-sports-id",
        default=None,
        help="Build from config/resorts/_playable_candidates.json instead of YAML",
    )
    p.add_argument(
        "--clay-scene",
        action="store_true",
        help="Export a mesh-only clay scene under <data-root>/clay_scenes/ (wiki + homepage)",
    )
    p.add_argument(
        "--homepage-scene",
        action="store_true",
        help="Alias for --clay-scene (compat)",
    )
    p.add_argument(
        "--homepage-mesh-m",
        type=float,
        default=None,
        help="Homepage terrain vertex spacing in meters (default 12; target mesh <1 MB)",
    )
    return p.parse_args(argv)


def picked_resort_ids() -> list[str]:
    path = REPO_ROOT / "config" / "resorts" / "_picked_batch.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    ids = [str(r["resort_id"]) for r in raw]
    if "montage_mountain_pa" not in ids:
        ids.append("montage_mountain_pa")
    return ids


def main(argv=None) -> int:
    _setup_logging()
    args = parse_args(argv)
    import os

    def _default_data_root() -> Path:
        env = os.environ.get("DATA") or os.environ.get("GAME_EXPORT_DATA_ROOT")
        if env:
            return Path(env)
        if Path("/data").is_dir():
            return Path("/data")
        return REPO_ROOT / "output"

    def _default_cache() -> Path:
        env = os.environ.get("CACHE") or os.environ.get("GAME_EXPORT_CACHE_DIR")
        if env:
            return Path(env)
        if Path("/cache").is_dir():
            return Path("/cache")
        return REPO_ROOT / "cache"

    from_s3 = args.from_s3
    if from_s3 is None:
        from_s3 = args.data_root is None
    data_root = args.data_root if args.data_root is not None else _default_data_root()
    cache_dir = args.cache_dir or _default_cache()
    if args.out_root is not None:
        out_root = args.out_root
    elif from_s3:
        out_root = REPO_ROOT / "output" / "game_scenes"
    else:
        out_root = data_root / "game_scenes"
    s3_bucket = args.s3_bucket or default_s3_bucket()

    if args.catalog_only:
        dest = write_catalog(out_root)
        print(f"Catalog written: {dest}")
        return 0

    if args.clay_scene or args.homepage_scene:
        from game_export.homepage import DEFAULT_MESH_RESOLUTION_M, export_homepage_scene

        cfg_path = args.config or default_config_path(args.resort)
        if not cfg_path.is_file():
            log.error("Config not found: %s", cfg_path)
            return 2
        cfg = load_resort_config(cfg_path)
        mesh_m = args.homepage_mesh_m if args.homepage_mesh_m is not None else DEFAULT_MESH_RESOLUTION_M
        if args.dry_run:
            print(
                f"DRY RUN clay scene: resort={cfg.resort_id} mesh={mesh_m}m "
                f"out={data_root / 'clay_scenes' / cfg.resort_id}"
            )
            return 0
        try:
            scene = export_homepage_scene(
                cfg,
                data_root=data_root,
                cache_dir=cache_dir,
                out_root=data_root,
                from_s3=from_s3,
                s3_bucket=s3_bucket,
                fetch_skadi=args.fetch_skadi,
                mesh_resolution_m=mesh_m,
                force=args.force,
            )
        except (FileNotFoundError, RuntimeError, PermissionError) as e:
            log.error("%s", e)
            return 1
        glb = scene / "terrain" / "terrain-mesh.glb"
        print(f"Clay scene written: {scene}")
        if glb.is_file():
            print(f"Terrain mesh: {glb} ({glb.stat().st_size:,} bytes)")
        return 0

    if args.picked_batch:
        codes = []
        for resort_id in picked_resort_ids():
            log.info("=== picked batch: %s ===", resort_id)
            codes.append(
                _export_resort(
                    args,
                    resort_id=resort_id,
                    cfg_path=None,
                    data_root=data_root,
                    cache_dir=cache_dir,
                    out_root=out_root,
                    from_s3=from_s3,
                    s3_bucket=s3_bucket,
                ).code
            )
        failed = [i for i, c in enumerate(codes) if c != 0]
        if failed:
            names = picked_resort_ids()
            log.error("Failed: %s", ", ".join(names[i] for i in failed))
            return 1
        return 0

    cfg = None
    if args.winter_sports_id:
        from game_export.config import config_from_candidate

        cand_path = REPO_ROOT / "config" / "resorts" / "_playable_candidates.json"
        payload = json.loads(cand_path.read_text(encoding="utf-8"))
        wid = str(args.winter_sports_id).strip()
        row = next(
            (r for r in payload.get("candidates") or [] if str(r.get("winter_sports_id")) == wid),
            None,
        )
        if not row:
            log.error("winter_sports_id %s not in %s", wid, cand_path)
            return 2
        cfg = config_from_candidate(row)
    cfg_path = None if cfg is not None else (args.config or default_config_path(args.resort))
    return _export_resort(
        args,
        resort_id=args.resort if cfg is None else cfg.resort_id,
        cfg_path=cfg_path,
        data_root=data_root,
        cache_dir=cache_dir,
        out_root=out_root,
        from_s3=from_s3,
        s3_bucket=s3_bucket,
        cfg=cfg,
    ).code


def _export_resort(
    args,
    *,
    resort_id: str,
    cfg_path,
    data_root: Path,
    cache_dir: Path,
    out_root: Path,
    from_s3: bool,
    s3_bucket: str,
    cfg=None,
) -> ExportResult:
    if cfg is None:
        cfg_path = cfg_path or default_config_path(resort_id)
        if not cfg_path.is_file():
            log.error("Config not found: %s", cfg_path)
            return ExportResult(2)
        cfg = load_resort_config(cfg_path)

    log.info(
        "Resort %s (%s) region=%s id=%s source=%s",
        cfg.resort_id,
        cfg.display_name,
        cfg.region,
        cfg.winter_sports_id,
        f"s3://{s3_bucket}" if from_s3 else data_root,
    )
    try:
        inputs = resolve_inputs(
            data_root,
            cfg,
            cache_dir,
            fetch_skadi=args.fetch_skadi,
            from_s3=from_s3,
            s3_bucket=s3_bucket,
        )
    except (FileNotFoundError, RuntimeError, PermissionError) as e:
        log.error("%s", e)
        return ExportResult(1)

    extra = list(inputs.input_files)
    if not extra:
        for name in ("osm_near_winter_sports.parquet", "pistes.parquet", "lifts.parquet"):
            extra.append(inputs.region_dir / name)
    version = scene_version(cfg, inputs.dem_path, extra)
    out = out_root / cfg.resort_id / version
    log.info("Would write %s", out)
    if args.dry_run:
        print(f"DRY RUN ok. DEM={inputs.dem_path} source={inputs.dem_source_note}")
        print(f"Output would be: {out}")
        return ExportResult(0, out)
    if out.exists() and not args.force:
        log.info("Scene version already exists. Use --force to rebuild. %s", out)
        print(f"Existing scene: {out}")
        approved = 0
        graph_path = out / "gameplay" / "routes.graph.json"
        if graph_path.is_file():
            graph = json.loads(graph_path.read_text(encoding="utf-8"))
            approved = int((graph.get("counts") or {}).get("approved") or 0)
        return ExportResult(0 if approved else 3, out, approved)
    if out.exists() and args.force:
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "qa" / "route-screenshots").mkdir(parents=True, exist_ok=True)
    (out / "vectors").mkdir(exist_ok=True)
    (out / "gameplay").mkdir(exist_ok=True)

    terrain = export_terrain(inputs.dem_path, out, cfg)
    local = terrain["local"]
    to_proj = terrain["to_proj"]
    elev = terrain["elev"]
    transform = terrain["transform"]
    cell_m = terrain["cell_m"]

    layers, repairs = collect_layers(
        inputs.osm_nearby, inputs.pistes, inputs.lifts, to_proj, local, cfg, mode="game"
    )
    add_ski_area_polygon(layers, inputs.ski_polygon, to_proj, local, repairs)
    layer_files = {
        "pistes": "pistes.geojson",
        "lifts": "lifts.geojson",
        "buildings": "buildings.geojson",
        "water": "water.geojson",
        "forest": "forest.geojson",
        "roads": "roads.geojson",
        "cliffs": "cliffs.geojson",
        "grassland": "grassland.geojson",
        "parking": "parking.geojson",
        "ski_area": "ski-area.geojson",
        "barriers": "barriers.geojson",
    }
    for name, fname in layer_files.items():
        feats = layers.get(name) or []
        # Always write (even empty) so the client never hits S3/CloudFront 403 for
        # missing keys that the manifest still advertises.
        write_local_geojson(out / "vectors" / fname, feats, local, name)

    corridors = piste_corridors(layers.get("pistes") or [], cfg)
    write_local_geojson(out / "vectors" / "piste-corridors.geojson", corridors, local, "piste-corridors")

    exclusions = build_exclusions(
        layers,
        terrain["slope_deg"],
        transform,
        local,
        cfg,
        elev,
        cell_m,
        out / "vectors" / "exclusion-zones.geojson",
    )

    # Optional elevation points → local
    elev_local = None
    if inputs.elevation_points is not None and not inputs.elevation_points.empty:
        g = inputs.elevation_points.copy()
        g["geometry"] = g.geometry.apply(lambda geom: geom_to_local(geom_to_projected(geom, to_proj), local))
        elev_local = g

    route_pack = build_routes(
        layers.get("pistes") or [],
        layers.get("lifts") or [],
        elev,
        transform,
        local,
        cfg,
        elev_local,
    )
    approved_n = int(route_pack["graph"]["counts"]["approved"])
    if approved_n < 1:
        log.error("No approved downhill course for %s; not keeping scene", cfg.resort_id)
        shutil.rmtree(out, ignore_errors=True)
        parent = out_root / cfg.resort_id
        if parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
        return ExportResult(3, None, 0)
    write_json(out / "gameplay" / "routes.graph.json", route_pack["graph"])
    write_json(
        out / "gameplay" / "spawn-points.json",
        {
            "disclaimer": "Candidate arcade spawn points, not official lift/trail starts.",
            "points": route_pack["spawn"],
            "finishes": route_pack["finishes"],
        },
    )
    write_json(
        out / "gameplay" / "course-candidates.json",
        {"courses": route_pack["courses"]},
    )
    write_local_geojson(
        out / "vectors" / "route-centers.geojson",
        route_pack["center_features"],
        local,
        "route-centers",
    )
    write_json(
        out / "gameplay" / "terrain-query-metadata.json",
        {
            "heightfield": "terrain/heightfield-u16.bin",
            "collision_heightfield": "terrain/collision-heightfield-u16.bin",
            "coordinate_system": local.to_dict(),
            "sample": "See terrain/heightfield-metadata.json pixel_to_game",
        },
    )

    poly_proj = geom_to_projected(inputs.ski_polygon, to_proj)
    minx, miny, maxx, maxy = poly_proj.bounds
    aoi_stats = {
        "width_m": round(maxx - minx, 1),
        "height_m": round(maxy - miny, 1),
        "area_m2": round(poly_proj.area, 1),
        "scene_bounds_buffer_m": cfg.scene_bounds_buffer_m,
    }

    qa_dir = out / "qa"
    write_route_profiles(route_pack["routes_internal"], qa_dir)
    write_overview_maps(
        terrain["hillshade"],
        terrain["slope_deg"],
        transform,
        local,
        layers,
        route_pack["routes_internal"],
        elev_local,
        qa_dir,
        to_proj,
    )
    payload = validation_payload(
        cfg,
        inputs,
        local,
        terrain["terrain_meta"],
        layers,
        route_pack["graph"],
        route_pack["spawn"],
        exclusions,
        repairs,
        aoi_stats,
    )
    write_reports(qa_dir, payload)
    write_attribution(out)
    write_manifest(out, cfg, local, terrain["terrain_meta"], cfg.seed)
    if not getattr(args, "skip_catalog", False):
        write_catalog(out_root)

    print(f"Game scene written: {out}")
    print(
        "Summary: pistes={p} lifts={l} routes_approved={a} spawn={s} mesh_tris={t}".format(
            p=len(layers.get("pistes") or []),
            l=len(layers.get("lifts") or []),
            a=approved_n,
            s=len(route_pack["spawn"]),
            t=terrain["terrain_meta"]["mesh"]["triangle_count"],
        )
    )
    return ExportResult(0, out, approved_n)


if __name__ == "__main__":
    raise SystemExit(main())
