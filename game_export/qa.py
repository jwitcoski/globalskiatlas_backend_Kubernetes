"""Validation reports and QA figures."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from shapely.geometry import LineString, shape

from game_export.coords import LocalCRS
from game_export import jsonutil


def write_route_profiles(routes_internal: list, qa_dir: Path) -> int:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    out = qa_dir / "route-profiles"
    out.mkdir(parents=True, exist_ok=True)
    n = 0
    for r in routes_internal:
        if r.get("status") != "approved":
            continue
        samples = r.get("_samples") or []
        dist = [0.0]
        elevs = []
        prev = None
        for x, y, z in samples:
            if prev is not None:
                dist.append(dist[-1] + float(np.hypot(x - prev[0], y - prev[1])))
            prev = (x, y)
            elevs.append(z if z is not None else np.nan)
        if len(elevs) < 2:
            continue
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.plot(dist, elevs, color="#1f4e79")
        ax.set_xlabel("Distance (m)")
        ax.set_ylabel("Elevation (m)")
        ax.set_title(f"{r['id']}  drop={r.get('vertical_drop_m')} m  [{r.get('name') or ''}]")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(out / f"{r['id'].replace(':', '_')}.png", dpi=110)
        plt.close(fig)
        n += 1
    return n


def write_overview_maps(
    hillshade,
    slope_deg,
    transform,
    local: LocalCRS,
    layers: dict,
    routes_internal: list,
    elevation_points,
    qa_dir: Path,
    to_proj,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.collections import LineCollection

    rows, cols = hillshade.shape
    extent = (0, cols, rows, 0)  # row0 north

    def local_to_colrow(east_m, north_m):
        e = east_m + local.origin_easting_m
        n = north_m + local.origin_northing_m
        c = (e - transform.c) / abs(transform.a)
        r = (transform.f - n) / abs(transform.e)
        return c, r

    def draw_lines(ax, features, color, lw=1.0):
        segs = []
        for f in features:
            g = shape(f["geometry"])
            lines = g.geoms if g.geom_type == "MultiLineString" else ([g] if g.geom_type == "LineString" else [])
            for ln in lines:
                pts = [local_to_colrow(x, y) for x, y in ln.coords]
                segs.append(pts)
        if segs:
            ax.add_collection(LineCollection(segs, colors=color, linewidths=lw))

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(hillshade, cmap="gray", extent=extent)
    draw_lines(ax, layers.get("pistes") or [], "#d62728", 1.4)
    draw_lines(ax, layers.get("lifts") or [], "#17becf", 1.2)
    ax.set_title("Hillshade + OSM pistes (red) + lifts (cyan)")
    ax.text(0.01, 0.01, "© OpenStreetMap contributors · prototype, not a trail map", transform=ax.transAxes, color="white", fontsize=7)
    fig.tight_layout()
    fig.savefig(qa_dir / "overview-hillshade-pistes.png", dpi=120)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(np.clip(slope_deg, 0, 60), cmap="YlOrRd", extent=extent)
    approved_f = []
    rejected_f = []
    for r in routes_internal:
        feat = {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": r.get("_centerline") or []},
        }
        if r.get("status") == "approved":
            approved_f.append(feat)
        else:
            rejected_f.append(feat)
    draw_lines(ax, approved_f, "#2ca02c", 1.6)
    draw_lines(ax, rejected_f, "#7f7f7f", 0.8)
    ax.set_title("Slope + routes (green=approved, gray=other)")
    fig.tight_layout()
    fig.savefig(qa_dir / "overview-slope-routes.png", dpi=120)
    plt.close(fig)

    # Simple 3D thumbnail
    shot_dir = qa_dir / "route-screenshots"
    shot_dir.mkdir(parents=True, exist_ok=True)
    if approved_f and approved_f[0]["geometry"]["coordinates"]:
        from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

        coords = approved_f[0]["geometry"]["coordinates"]
        xs = [c[0] for c in coords]
        ys = [c[1] for c in coords]
        fig = plt.figure(figsize=(7, 5))
        ax3 = fig.add_subplot(111, projection="3d")
        # downsample hillshade mesh
        step = max(1, hillshade.shape[0] // 80)
        Z = hillshade  # not elevation; use a coarse elev substitute from slope? skip
        ax3.plot(xs, ys, zs=0, color="red")
        ax3.set_title("DEBUG 3D thumbnail — primary route (XY local, Z unused)")
        fig.tight_layout()
        fig.savefig(shot_dir / "primary-route-thumbnail.png", dpi=100)
        plt.close(fig)


def validation_payload(
    cfg,
    inputs,
    local: LocalCRS,
    terrain_meta: dict,
    layers: dict,
    graph: dict,
    spawn: list,
    exclusions: list,
    repairs: list,
    aoi_stats: dict,
) -> dict:
    return {
        "scene_id": cfg.resort_id,
        "disclaimer": (
            "This asset is a derived prototype game scene from open data. "
            "It is not an official ski-resort map, trail map, navigation tool, or safety product. "
            "Not affiliated with or endorsed by Montage Mountain."
        ),
        "aoi": aoi_stats,
        "inputs": {
            "region_dir": str(inputs.region_dir),
            "dem_path": str(inputs.dem_path),
            "dem_note": inputs.dem_source_note,
            "dem_provider": "Mapzen Skadi / AWS Terrain Tiles (SRTM-style 1 arc-second)",
            "osm_attribution": "© OpenStreetMap contributors",
            "warnings": inputs.warnings + [r.get("reason") for r in repairs],
        },
        "coordinate_system": local.to_dict(),
        "terrain": {
            "heightfield": terrain_meta.get("heightfield"),
            "mesh": terrain_meta.get("mesh"),
        },
        "counts": {
            "pistes": len(layers.get("pistes") or []),
            "lifts": len(layers.get("lifts") or []),
            "buildings": len(layers.get("buildings") or []),
            "water": len(layers.get("water") or []),
            "forest": len(layers.get("forest") or []),
            "cliffs": len(layers.get("cliffs") or []),
            "exclusion_zones": len(exclusions),
            "spawn_points": len(spawn),
            "routes": graph.get("counts"),
        },
        "routes": graph.get("routes"),
        "osm_coverage_confidence": (
            "high"
            if len(layers.get("pistes") or []) >= 5 and len(layers.get("lifts") or []) >= 1
            else "limited — Montage Mountain OSM piste/lift mapping may be incomplete; routes are only as good as OSM."
        ),
        "geometry_repairs": repairs,
        "attribution": {
            "osm": "© OpenStreetMap contributors — https://www.openstreetmap.org/copyright (ODbL)",
            "dem": "Mapzen Skadi elevation tiles",
        },
    }


def write_reports(qa_dir: Path, payload: dict) -> None:
    qa_dir.mkdir(parents=True, exist_ok=True)
    (qa_dir / "validation-report.json").write_text(jsonutil.dumps(payload), encoding="utf-8")
    c = payload["counts"]
    rc = c.get("routes") or {}
    lines = [
        "# Game-scene validation report",
        "",
        payload["disclaimer"],
        "",
        f"- Scene: `{payload['scene_id']}`",
        f"- AOI: {payload['aoi']}",
        f"- DEM: {payload['inputs']['dem_path']}",
        f"- DEM note: {payload['inputs']['dem_note']}",
        f"- Projected CRS: {payload['coordinate_system']['projected_crs']}",
        f"- Local origin lon/lat: {payload['coordinate_system']['origin_longitude']}, {payload['coordinate_system']['origin_latitude']}",
        "",
        "## Counts",
        f"- Pistes: {c['pistes']}",
        f"- Lifts: {c['lifts']}",
        f"- Buildings: {c['buildings']}",
        f"- Water: {c['water']}",
        f"- Forest: {c['forest']}",
        f"- Cliffs: {c['cliffs']}",
        f"- Exclusion zones: {c['exclusion_zones']}",
        f"- Spawn points: {c['spawn_points']}",
        f"- Routes approved/review/rejected: {rc.get('approved')}/{rc.get('review_needed')}/{rc.get('rejected')}",
        "",
        "## OSM coverage",
        str(payload["osm_coverage_confidence"]),
        "",
        "## Route metrics",
        "",
        "| id | status | length_m | drop_m | mean_slope_% | uphill_frac | name |",
        "|----|--------|----------|--------|--------------|-------------|------|",
    ]
    for r in payload.get("routes") or []:
        lines.append(
            f"| {r.get('id')} | {r.get('status')} | {r.get('length_m')} | {r.get('vertical_drop_m')} | "
            f"{r.get('mean_slope_percent')} | {r.get('uphill_fraction')} | {r.get('name') or ''} |"
        )
    lines += [
        "",
        "## Attribution",
        payload["attribution"]["osm"],
        payload["attribution"]["dem"],
        "",
        "## Geometry repairs",
        jsonutil.dumps(payload.get("geometry_repairs") or []),
    ]
    (qa_dir / "validation-report.md").write_text("\n".join(lines), encoding="utf-8")
