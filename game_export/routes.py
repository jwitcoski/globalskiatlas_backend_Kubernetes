"""Directed downhill route graph from OSM pistes + DEM samples."""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path

import numpy as np
from shapely.geometry import LineString, MultiLineString, Point, Polygon, mapping, shape
from shapely.ops import linemerge

from game_export.config import GameExportConfig
from game_export.coords import LocalCRS, bearing_deg
from game_export.terrain import sample_elev
from game_export import jsonutil

log = logging.getLogger("game_export")


def densify_line(coords: list[tuple[float, float]], spacing: float) -> list[tuple[float, float]]:
    if len(coords) < 2:
        return coords
    out = [coords[0]]
    for a, b in zip(coords, coords[1:]):
        dx, dy = b[0] - a[0], b[1] - a[1]
        dist = math.hypot(dx, dy)
        n = max(1, int(math.floor(dist / spacing)))
        for i in range(1, n + 1):
            t = i / n
            out.append((a[0] + t * dx, a[1] + t * dy))
    return out


def _as_lines(geom) -> list[LineString]:
    if geom is None or geom.is_empty:
        return []
    t = geom.geom_type
    if t == "LineString":
        return [geom]
    if t == "MultiLineString":
        return list(geom.geoms)
    if t in ("Polygon", "MultiPolygon"):
        lines = []
        polys = geom.geoms if t == "MultiPolygon" else [geom]
        for p in polys:
            # Representative flowline: longest axis of minimum rotated rect is not
            # reliable; use exterior simplified then we'll sample downhill separately.
            ext = LineString(p.exterior.coords)
            lines.append(ext)
        return lines
    return []


def polygon_descent_flowline(poly, elev, transform, local: LocalCRS, spacing: float) -> LineString | None:
    """Cheap downhill walk constrained to polygon, seeded at highest vertex."""
    coords = list(poly.exterior.coords)
    best = None
    best_z = -1e9
    for x, y in coords:
        e, n = x + local.origin_easting_m, y + local.origin_northing_m
        z = sample_elev(elev, transform, e, n)
        if np.isfinite(z) and z > best_z:
            best_z = z
            best = (x, y)
    if best is None:
        return None
    path = [best]
    x, y = best
    for _ in range(400):
        z0 = sample_elev(
            elev, transform, x + local.origin_easting_m, y + local.origin_northing_m
        )
        nxt = None
        best_drop = 0.0
        for ang in range(0, 360, 30):
            rad = math.radians(ang)
            nx, ny = x + spacing * math.sin(rad), y + spacing * math.cos(rad)
            if not poly.contains(Point(nx, ny)) and not poly.touches(Point(nx, ny)):
                continue
            z1 = sample_elev(
                elev, transform, nx + local.origin_easting_m, ny + local.origin_northing_m
            )
            if not np.isfinite(z1):
                continue
            drop = z0 - z1
            if drop > best_drop:
                best_drop = drop
                nxt = (nx, ny)
        if nxt is None or best_drop < 0.05:
            break
        if math.hypot(nxt[0] - path[0][0], nxt[1] - path[0][1]) < 1 and len(path) > 5:
            break
        path.append(nxt)
        x, y = nxt
    if len(path) < 2:
        return None
    return LineString(path)


def metrics_for_line(line: LineString, elev, transform, local: LocalCRS, cfg: GameExportConfig) -> dict:
    pts = densify_line(list(line.coords), cfg.route_sample_spacing_m)
    zs = []
    samples = []
    nodata = 0
    for x, y in pts:
        e, n = x + local.origin_easting_m, y + local.origin_northing_m
        z = sample_elev(elev, transform, e, n)
        if not np.isfinite(z):
            nodata += 1
            zs.append(None)
        else:
            zs.append(z)
        samples.append((x, y, z if np.isfinite(z) else None))
    finite = [z for z in zs if z is not None]
    warnings = []
    if nodata:
        warnings.append(f"{nodata} DEM no-data samples along path")
    if len(finite) < 2:
        return {
            "status": "rejected",
            "reject_reasons": ["no_dem_samples"],
            "warnings": warnings,
            "samples": samples,
            "length_m": float(line.length),
        }
    # Prefer downhill: reverse if net climb
    if finite[-1] > finite[0]:
        pts = list(reversed(pts))
        zs = list(reversed(zs))
        samples = list(reversed(samples))
        finite = [z for z in zs if z is not None]
        warnings.append("reversed_geometry_to_downhill")

    length = 0.0
    uphill = 0.0
    drops = []
    bearings = []
    prev = None
    prev_z = None
    for (x, y), z in zip(pts, zs):
        if prev is not None:
            d = math.hypot(x - prev[0], y - prev[1])
            length += d
            if z is not None and prev_z is not None:
                dz = z - prev_z
                if dz > 0:
                    uphill += d
                if d > 0:
                    drops.append((-dz / d) * 100.0)  # slope percent downhill positive
                    bearings.append(bearing_deg(prev[0], prev[1], x, y))
        prev = (x, y)
        prev_z = z
    start_z, end_z = finite[0], finite[-1]
    vdrop = start_z - end_z
    uphill_frac = (uphill / length) if length else 1.0
    mean_slope = float(np.mean(drops)) if drops else 0.0
    max_slope = float(np.max(drops)) if drops else 0.0
    curv = 0.0
    if len(bearings) >= 2:
        diffs = []
        for a, b in zip(bearings, bearings[1:]):
            d = abs(b - a) % 360
            diffs.append(min(d, 360 - d))
        curv = float(np.mean(diffs))
    reasons = []
    status = "approved"
    if length < cfg.route_min_length_m:
        reasons.append("short_length")
        status = "review_needed"
    if vdrop < cfg.route_min_vertical_drop_m:
        reasons.append("low_vertical_drop")
        status = "rejected" if vdrop < cfg.route_min_vertical_drop_m * 0.4 else "review_needed"
    if uphill_frac > cfg.route_max_uphill_fraction:
        reasons.append("uphill_fraction")
        status = "review_needed" if uphill_frac < cfg.route_max_uphill_fraction * 2 else "rejected"
    if nodata:
        status = "review_needed" if status == "approved" else status
        reasons.append("dem_nodata")
    if vdrop <= 0:
        reasons.append("no_downhill")
        status = "rejected"
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return {
        "status": status,
        "reject_reasons": reasons,
        "warnings": warnings,
        "length_m": round(length, 2),
        "start_elevation_m": round(start_z, 2),
        "end_elevation_m": round(end_z, 2),
        "vertical_drop_m": round(vdrop, 2),
        "mean_slope_percent": round(mean_slope, 2),
        "max_slope_percent": round(max_slope, 2),
        "uphill_fraction": round(uphill_frac, 4),
        "mean_bearing_change_deg": round(curv, 2),
        "bounds": [min(xs), min(ys), max(xs), max(ys)],
        "start_local": {"east_m": pts[0][0], "north_m": pts[0][1]},
        "end_local": {"east_m": pts[-1][0], "north_m": pts[-1][1]},
        "centerline": pts,
        "samples": samples,
    }


def build_routes(
    piste_features: list,
    lift_features: list,
    elev,
    transform,
    local: LocalCRS,
    cfg: GameExportConfig,
    elevation_points,
) -> dict:
    routes = []
    center_feats = []
    for i, feat in enumerate(piste_features):
        geom = shape(feat["geometry"])
        props = feat.get("properties") or {}
        lines = _as_lines(geom)
        if geom.geom_type in ("Polygon", "MultiPolygon"):
            fl = polygon_descent_flowline(
                geom if geom.geom_type == "Polygon" else max(geom.geoms, key=lambda g: g.area),
                elev,
                transform,
                local,
                cfg.route_sample_spacing_m,
            )
            if fl is not None:
                lines = [fl]
        for j, line in enumerate(lines):
            if line.length < 5:
                continue
            m = metrics_for_line(line, elev, transform, local, cfg)
            rid = f"route:{props.get('id', i)}:{j}"
            rec = {
                "id": rid,
                "source_feature_id": props.get("id"),
                "osm_id": props.get("osm_id"),
                "name": props.get("name"),
                "piste_difficulty": (props.get("tags") or {}).get("piste:difficulty")
                or (props.get("tags") or {}).get("difficulty"),
                "directed": True,
                "direction": "downhill",
                **{k: v for k, v in m.items() if k not in ("centerline", "samples")},
            }
            rec["_centerline"] = m.get("centerline") or list(line.coords)
            rec["_samples"] = m.get("samples") or []
            routes.append(rec)
            center_feats.append(
                {
                    "type": "Feature",
                    "geometry": mapping(LineString(rec["_centerline"])),
                    "properties": {
                        "id": rid,
                        "status": rec["status"],
                        "name": rec.get("name"),
                        "piste_difficulty": rec.get("piste_difficulty"),
                        "vertical_drop_m": rec.get("vertical_drop_m"),
                        "length_m": rec.get("length_m"),
                    },
                }
            )

    # Connect endpoints
    nodes = []
    edges = []
    for r in routes:
        cl = r["_centerline"]
        nodes.append({"id": f"{r['id']}:start", "route_id": r["id"], "kind": "start", **_pt(cl[0], r.get("start_elevation_m"))})
        nodes.append({"id": f"{r['id']}:end", "route_id": r["id"], "kind": "end", **_pt(cl[-1], r.get("end_elevation_m"))})
        edges.append(
            {
                "id": r["id"],
                "from": f"{r['id']}:start",
                "to": f"{r['id']}:end",
                "status": r["status"],
            }
        )
    thresh = cfg.route_connect_endpoint_m
    for a in nodes:
        for b in nodes:
            if a["id"] == b["id"]:
                continue
            d = math.hypot(a["east_m"] - b["east_m"], a["north_m"] - b["north_m"])
            if d > thresh:
                continue
            za, zb = a.get("elevation_m"), b.get("elevation_m")
            if za is None or zb is None:
                continue
            if za + 2 < zb:
                continue  # would be uphill link
            edges.append(
                {
                    "id": f"link:{a['id']}->{b['id']}",
                    "from": a["id"],
                    "to": b["id"],
                    "status": "approved" if d < thresh * 0.5 else "review_needed",
                    "kind": "endpoint_link",
                    "distance_m": round(d, 2),
                }
            )

    approved = [r for r in routes if r["status"] == "approved"]
    spawn = []
    if approved:
        top = max(approved, key=lambda r: r.get("start_elevation_m") or -1e9)
        sx, sy = top["_centerline"][0]
        gx, gz = local.to_game_xz(sx + local.origin_easting_m, sy + local.origin_northing_m)
        spawn.append(
            {
                "id": "spawn:highest_approved_start",
                "kind": "route_start",
                "route_id": top["id"],
                "local": {"east_m": sx, "north_m": sy},
                "game": {"x": gx, "y": top["start_elevation_m"], "z": gz},
                "reason": "highest start of an approved downhill route",
            }
        )
    # lift-top heuristic
    for feat in lift_features:
        geom = shape(feat["geometry"])
        if geom.is_empty:
            continue
        coords = list(geom.coords) if geom.geom_type == "LineString" else []
        if len(coords) < 2:
            continue
        z0 = sample_elev(elev, transform, coords[0][0] + local.origin_easting_m, coords[0][1] + local.origin_northing_m)
        z1 = sample_elev(elev, transform, coords[-1][0] + local.origin_easting_m, coords[-1][1] + local.origin_northing_m)
        if not (np.isfinite(z0) and np.isfinite(z1)):
            continue
        topc = coords[0] if z0 >= z1 else coords[-1]
        tz = max(z0, z1)
        gx, gz = local.to_game_xz(topc[0] + local.origin_easting_m, topc[1] + local.origin_northing_m)
        spawn.append(
            {
                "id": f"spawn:lift:{feat['properties'].get('id')}",
                "kind": "lift_top",
                "local": {"east_m": topc[0], "north_m": topc[1]},
                "game": {"x": gx, "y": float(tz), "z": gz},
                "reason": "higher endpoint of mapped aerialway",
            }
        )
    if elevation_points is not None and not elevation_points.empty:
        for _, row in elevation_points.iterrows():
            if str(row.get("point_type") or "").lower() != "summit":
                continue
            # points still in WGS84; skip here — caller may pass already converted. Ignore if lon/lat.
            pass

    finishes = []
    for r in approved:
        ex, ey = r["_centerline"][-1]
        gx, gz = local.to_game_xz(ex + local.origin_easting_m, ey + local.origin_northing_m)
        finishes.append(
            {
                "id": f"finish:{r['id']}",
                "route_id": r["id"],
                "local": {"east_m": ex, "north_m": ey},
                "game": {"x": gx, "y": r.get("end_elevation_m"), "z": gz},
            }
        )

    courses = []
    for r in approved:
        courses.append(
            {
                "id": f"course:{r['id']}",
                "route_ids": [r["id"]],
                "status": "candidate",
                "vertical_drop_m": r.get("vertical_drop_m"),
                "length_m": r.get("length_m"),
                "name": r.get("name"),
                "note": "Derived from OSM + DEM. Not an official run.",
            }
        )

    graph = {
        "directed": True,
        "disclaimer": "Prototype ski-route graph from OSM geometries and DEM samples. Not official trails.",
        "nodes": nodes,
        "edges": edges,
        "routes": [{k: v for k, v in r.items() if not k.startswith("_")} for r in routes],
        "counts": {
            "approved": sum(1 for r in routes if r["status"] == "approved"),
            "review_needed": sum(1 for r in routes if r["status"] == "review_needed"),
            "rejected": sum(1 for r in routes if r["status"] == "rejected"),
        },
    }
    return {
        "graph": graph,
        "routes_internal": routes,
        "center_features": center_feats,
        "spawn": spawn,
        "finishes": finishes,
        "courses": courses,
    }


def _pt(xy, z):
    return {"east_m": xy[0], "north_m": xy[1], "elevation_m": z}


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(jsonutil.dumps(obj), encoding="utf-8")
