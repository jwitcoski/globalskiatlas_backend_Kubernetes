"""Lightweight homepage hero scene — terrain mesh only, fast to load."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from game_export import jsonutil
from game_export.config import GameExportConfig
from game_export.coords import LocalCRS
from game_export.inputs import resolve_inputs
from game_export.manifest import write_attribution
from game_export.homepage_vectors import write_homepage_vectors
from game_export.terrain import export_homepage_terrain

log = logging.getLogger("game_export")

HOMEPAGE_OUT_ROOT = "clay_scenes"
DEFAULT_MESH_RESOLUTION_M = 12.0
MAX_MESH_BYTES = 1_048_576


def write_homepage_manifest(
    out: Path,
    cfg: GameExportConfig,
    local: LocalCRS,
    terrain_meta: dict,
    *,
    mesh_resolution_m: float,
) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = {
        "scene_schema_version": "0.1.0-homepage",
        "scene_kind": "homepage_hero",
        "scene_id": cfg.resort_id,
        "winter_sports_id": cfg.winter_sports_id,
        "build_timestamp_utc": now,
        "display_name": cfg.display_name,
        "country": cfg.country,
        "location": cfg.approximate_location_name,
        "coordinate_system": local.to_dict(),
        "terrain": {
            "mesh": "terrain/terrain-mesh.glb",
            "mesh_metadata": "terrain/terrain-metadata.json",
            "elevation_min_m": terrain_meta.get("elevation_min_m"),
            "elevation_max_m": terrain_meta.get("elevation_max_m"),
            "vertex_spacing_m": mesh_resolution_m,
        },
        "vectors": {
            "piste_trails": "vectors/piste-trails.geojson",
            "lifts": "vectors/lifts.geojson",
            "forest": "vectors/forest.geojson",
            "ski_area_buffer": "vectors/ski-area-buffer.geojson",
        },
        "attribution": {
            "osm": "© OpenStreetMap contributors",
            "dem": "Mapzen Skadi / AWS Terrain Tiles",
            "license_notes": "OSM data ODbL; DEM from Mapzen Skadi. See attribution/ATTRIBUTION.md",
        },
        "disclaimer": (
            "Decorative homepage terrain derived from open data. Not an official resort map, "
            "trail map, navigation tool, or safety product."
        ),
    }
    (out / "scene-manifest.json").write_text(jsonutil.dumps(manifest), encoding="utf-8")
    return manifest


def export_homepage_scene(
    cfg: GameExportConfig,
    *,
    data_root: Path,
    cache_dir: Path,
    out_root: Path,
    from_s3: bool,
    s3_bucket: str,
    fetch_skadi: bool = False,
    mesh_resolution_m: float = DEFAULT_MESH_RESOLUTION_M,
    force: bool = False,
) -> Path:
    """Export a mesh-only homepage hero scene for one resort."""
    import shutil

    inputs = resolve_inputs(
        data_root,
        cfg,
        cache_dir,
        fetch_skadi=fetch_skadi,
        from_s3=from_s3,
        s3_bucket=s3_bucket,
    )
    out = out_root / HOMEPAGE_OUT_ROOT / cfg.resort_id
    if out.exists():
        if not force:
            glb = out / "terrain" / "terrain-mesh.glb"
            if glb.is_file():
                log.info("Clay scene already exists: %s", out)
                return out
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    res = float(mesh_resolution_m)
    terrain = None
    mesh_bytes = 0
    while res <= 48.0:
        terrain = export_homepage_terrain(
            inputs.dem_path,
            out,
            cfg,
            mesh_resolution_m=res,
        )
        mesh_bytes = int(terrain["terrain_meta"]["mesh"]["byte_size"])
        if mesh_bytes <= MAX_MESH_BYTES:
            break
        log.warning(
            "Homepage mesh is %s bytes at %sm (limit %s) — coarsening",
            f"{mesh_bytes:,}",
            res,
            f"{MAX_MESH_BYTES:,}",
        )
        res = round(res * 1.35, 1)
    if mesh_bytes > MAX_MESH_BYTES:
        raise RuntimeError(
            f"Homepage mesh is {mesh_bytes:,} bytes (limit {MAX_MESH_BYTES:,}) "
            f"even at {res}m spacing."
        )
    mesh_resolution_m = res

    vector_counts = write_homepage_vectors(
        out,
        cfg=cfg,
        inputs=inputs,
        local=terrain["local"],
        elev=terrain["elev"],
        transform=terrain["transform"],
        to_proj=terrain["to_proj"],
    )

    write_homepage_manifest(
        out,
        cfg,
        terrain["local"],
        terrain["terrain_meta"],
        mesh_resolution_m=mesh_resolution_m,
    )
    write_attribution(out)
    (out / "README.md").write_text(
        "# Clay 3D scene\n\n"
        "Decorative terrain mesh plus OSM piste/lift vectors for the homepage hero and wiki 3D Map tab. "
        "Not a playable game scene — load the full `game_scenes` cake to ski.\n",
        encoding="utf-8",
    )
    log.info(
        "Clay scene %s: mesh=%s bytes tris=%s trails=%s lifts=%s forest=%s",
        out,
        f"{mesh_bytes:,}",
        terrain["terrain_meta"]["mesh"]["triangle_count"],
        vector_counts["piste_trails"],
        vector_counts["lifts"],
        vector_counts.get("forest", 0),
    )
    return out
