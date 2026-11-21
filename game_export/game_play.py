"""Convert a game scene export into the Hemu Ski game's own data files (the ``game_play/`` S3 folder).

Per resort this writes ``world.json``, ``detail.u16``, ``backdrop.u16`` and ``diorama/{world.json,terrain.u16}``.
Game axes: x = east, z = -north, y = elevation (same local metres as the scene export).
"""
from __future__ import annotations

import io
import json
import math
import re
import urllib.request
from pathlib import Path
from typing import Optional

import numpy as np
from PIL import Image

# Only these files of a scene export are needed; the rest (meshes, slope maps, ...) is skipped when downloading.
SCENE_FILES = (
    "scene-manifest.json",
    "terrain/heightfield-metadata.json",
    "terrain/heightfield-u16.bin",
    "vectors/pistes.geojson",
    "vectors/lifts.geojson",
    "vectors/buildings.geojson",
)

DIFFICULTY = {"novice": "easy", "easy": "easy", "intermediate": "intermediate",
              "advanced": "advanced", "expert": "expert", "extreme": "expert"}
LIFT_TYPES = {"chair_lift", "gondola", "magic_carpet", "drag_lift", "t-bar", "platter",
              "j-bar", "cable_car", "mixed_lift", "rope_tow", "zip_line"}
BACKDROP_SIZE, BACKDROP_CELLS, BACKDROP_ZOOM = 40000, 320, 10
LODGE_NAMES = (("lodge", 0), ("ski school", 1), ("rental", 2))


def tags_of(props):
    tags = dict(props.get("tags") or {})
    for k, v in re.findall(r'"([^"]+)"=>"([^"]*)"', tags.get("other_tags") or ""):
        tags.setdefault(k, v)
    return tags


def sample(grid, xs, zs, x0, z0, step):
    """Bilinear sample of a row-major grid whose [0,0] sits at (x0, z0), clamped at the edges."""
    fx = np.clip((xs - x0) / step, 0, grid.shape[1] - 1.0001)
    fz = np.clip((zs - z0) / step, 0, grid.shape[0] - 1.0001)
    ix, iz = fx.astype(int), fz.astype(int)
    tx, tz = fx - ix, fz - iz
    return ((grid[iz, ix] * (1 - tx) + grid[iz, ix + 1] * tx) * (1 - tz)
            + (grid[iz + 1, ix] * (1 - tx) + grid[iz + 1, ix + 1] * tx) * tz)


def to_u16(h):
    lo, hi = float(h.min()), float(h.max())
    q = np.round((h - lo) / ((hi - lo) or 1) * 65535).astype("<u2")
    return q, lo, hi


def load_heightfield(scene: Path):
    meta = json.loads((scene / "terrain/heightfield-metadata.json").read_text())
    raw = np.fromfile(scene / "terrain" / meta["filename"], dtype="<u2").reshape(meta["rows"], meta["cols"])
    elev = meta["elevation_offset_m"] + raw.astype("float32") * meta["elevation_scale_m"]
    bad = raw == meta["nodata_uint16"]
    if bad.any():
        elev[bad] = elev[~bad].min() if (~bad).any() else 0
    return meta, elev


def build_detail(meta, elev, cell):
    cs = meta["cell_size_m"]
    b = meta["bounds_local_m"]
    east0, north1 = b["min_east_m"], b["max_north_m"]
    # Row 0 is north, so game z grows with the row index.
    grid_x0, grid_z0 = east0 + cs / 2, -(north1 - cs / 2)
    w, d = b["max_east_m"] - east0, b["max_north_m"] - b["min_north_m"]
    cells = int(math.ceil(max(w, d) / cell / 2) * 2)
    size = cells * cell
    center = [round(east0 + w / 2, 1), round(-(north1 - d / 2), 1)]
    axis = np.arange(cells + 1) * cell - size / 2
    xs, zs = np.meshgrid(axis + center[0], axis + center[1])
    h = sample(elev, xs, zs, grid_x0, grid_z0, cs)
    return h, {"size": size, "cells": cells, "center": center}


def _fetch_tile(x: int, y: int, cache: Optional[Path]) -> np.ndarray:
    path = cache / f"{BACKDROP_ZOOM}_{x}_{y}.png" if cache else None
    if path and path.exists():
        data = path.read_bytes()
    else:
        url = f"https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{BACKDROP_ZOOM}/{x}/{y}.png"
        with urllib.request.urlopen(url, timeout=60) as r:
            data = r.read()
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
    px = np.asarray(Image.open(io.BytesIO(data)).convert("RGB"), dtype="float32")
    return px[..., 0] * 256 + px[..., 1] + px[..., 2] / 256 - 32768


def fetch_backdrop(lon, lat, cache: Optional[Path] = None):
    n = 2 ** BACKDROP_ZOOM
    half = BACKDROP_SIZE / 2
    mx, my = 111320 * math.cos(math.radians(lat)), 110540
    tile_x = lambda lo: (lo + 180) / 360 * n
    tile_y = lambda la: (1 - np.arcsinh(np.tan(np.radians(la))) / math.pi) / 2 * n
    tx0, tx1 = int(tile_x(lon - half / mx)), int(tile_x(lon + half / mx))
    ty0, ty1 = int(tile_y(lat + half / my)), int(tile_y(lat - half / my))
    mosaic = np.zeros(((ty1 - ty0 + 1) * 256, (tx1 - tx0 + 1) * 256), dtype="float32")
    for ty in range(ty0, ty1 + 1):
        for tx in range(tx0, tx1 + 1):
            mosaic[(ty - ty0) * 256:(ty - ty0 + 1) * 256, (tx - tx0) * 256:(tx - tx0 + 1) * 256] = _fetch_tile(tx, ty, cache)
    axis = np.arange(BACKDROP_CELLS + 1) * (BACKDROP_SIZE / BACKDROP_CELLS) - half
    xs, zs = np.meshgrid(axis, axis)
    fx = (tile_x(lon + xs / mx) - tx0) * 256
    fy = (tile_y(lat - zs / my) - ty0) * 256
    return sample(mosaic, fx, fy, 0, 0, 1)


def line_features(path: Path, keep):
    if not path.exists():
        return
    for f in json.loads(path.read_text(encoding="utf8"))["features"]:
        if f["geometry"]["type"] != "LineString":
            continue
        t = tags_of(f["properties"])
        if keep(t):
            yield f, t, [[round(x, 1), round(-y, 1)] for x, y in f["geometry"]["coordinates"]]


def convert_vectors(scene: Path):
    runs = [{"id": int(f["properties"]["osm_id"]), "name": t.get("name"),
             "difficulty": DIFFICULTY.get(t.get("piste:difficulty"), "intermediate"), "points": pts}
            for f, t, pts in line_features(scene / "vectors/pistes.geojson",
                                           lambda t: t.get("piste:type", "downhill") == "downhill")]
    lifts = [{"id": int(f["properties"]["osm_id"]), "name": t.get("name"), "type": t["aerialway"], "points": pts}
             for f, t, pts in line_features(scene / "vectors/lifts.geojson",
                                            lambda t: t.get("aerialway") in LIFT_TYPES)]
    return runs, lifts


def polygon_centroid(geom):
    ring = geom["coordinates"][0] if geom["type"] == "Polygon" else geom["coordinates"][0][0]
    return sum(p[0] for p in ring) / len(ring), sum(p[1] for p in ring) / len(ring)


def find_lodges(scene: Path, h, detail, lifts, cell):
    """Service buildings (lodge, ski school, rentals) become diorama lodges that face their nearest lift."""
    path = scene / "vectors/buildings.geojson"
    if not path.exists():
        return []
    half = detail["size"] / 2
    x0, z0 = detail["center"][0] - half, detail["center"][1] - half
    height = lambda x, z: float(sample(h, np.asarray(x), np.asarray(z), x0, z0, cell))
    found = []
    for f in json.loads(path.read_text(encoding="utf8"))["features"]:
        name = tags_of(f["properties"]).get("name") or ""
        rank = next((r for key, r in LODGE_NAMES if key in name.lower()), None)
        if rank is not None and f["geometry"]["type"] in ("Polygon", "MultiPolygon"):
            cx, cy = polygon_centroid(f["geometry"])
            found.append((rank, name, cx, -cy))
    chosen = []
    for rank, name, x, z in sorted(found):
        if all(math.hypot(x - c[2], z - c[3]) > 150 for c in chosen) and len(chosen) < 2:
            chosen.append((rank, name, x, z))

    bottoms = []
    for lift in lifts:
        a, b = lift["points"][0], lift["points"][-1]
        low = min((a, b), key=lambda p: height(p[0], p[1]))
        bottoms.append((lift, low))
    lodges = []
    for _, name, x, z in chosen:
        lift, (bx, bz) = min(bottoms, key=lambda lb: math.hypot(lb[1][0] - x, lb[1][1] - z)) if bottoms else (None, (x, z + 1))
        d = math.hypot(bx - x, bz - z) or 1
        lodges.append({"id": re.sub(r"\W+", "_", name.lower()).strip("_"), "zh": name, "en": "Service Centre",
                       "ele": round(height(x, z)), "x": round(x, 1), "z": round(z, 1),
                       "faceTo": [round(x + (bx - x) / d * 130, 1), round(z + (bz - z) / d * 130, 1)],
                       "roof": "#5f8064" if len(lodges) % 2 == 0 else "#4a5661",
                       "lift": (lift.get("name") or "the lift") if lift else "the lift"})
    return lodges


def write_diorama(out: Path, world, cs, h, cell):
    """The diorama table is cropped to the resort footprint (inside the real terrain), centred on 0,0."""
    d = out / "diorama"
    d.mkdir(parents=True, exist_ok=True)
    pts = [p for item in world["runs"] + world["lifts"] for p in item["points"]]
    pts += [[l["x"], l["z"]] for l in world["lodges"]]
    xs_, zs_ = [p[0] for p in pts], [p[1] for p in pts]
    cx, cz = round((min(xs_) + max(xs_)) / 2, 1), round((min(zs_) + max(zs_)) / 2, 1)
    extent = max(max(xs_) - min(xs_), max(zs_) - min(zs_))
    dh = world["detail"]
    half = dh["size"] / 2
    size_m = min(extent * 1.4 + 200, dh["size"] - 2 * cell,
                 2 * (half - max(abs(cx - dh["center"][0]), abs(cz - dh["center"][1]))))
    cells = max(2, min(640, int(size_m / cell / 2) * 2))
    size = cells * cell if cells < 640 else size_m
    axis = np.linspace(-size / 2, size / 2, cells + 1)
    gx, gz = np.meshgrid(axis + cx, axis + cz)
    elev = sample(h, gx, gz, dh["center"][0] - half, dh["center"][1] - half, cell)
    q, lo, hi = to_u16(elev)
    q.tofile(d / "terrain.u16")
    shift = lambda items: [{**i, "points": [[round(x - cx, 1), round(z - cz, 1)] for x, z in i["points"]]}
                           for i in items]
    lat = cs["origin_latitude"]
    dio = {k: world[k] for k in ("source", "origin", "title", "subtitle")}
    dio.update(
        center={"x": cx, "z": cz,
                "lonLat": [cs["origin_longitude"] + cx / (111320 * math.cos(math.radians(lat))),
                           lat - cz / 110540]},
        detail={"file": "terrain.u16", "size": round(size, 1), "cells": cells, "min": lo, "max": hi},
        lodges=world["lodges"], runs=shift(world["runs"]), lifts=shift(world["lifts"]))
    (d / "world.json").write_text(json.dumps(dio, ensure_ascii=False, separators=(",", ":")), encoding="utf8")


def _display_name(catalog_name: Optional[str], manifest: dict, resort_id: str) -> str:
    """Catalog names can be bare numeric ids for resorts without a name; prefer the manifest's then."""
    for cand in (catalog_name, manifest.get("display_name")):
        cand = (cand or "").replace(" \u2014 Prototype", "").strip()
        if cand and not cand.isdigit():
            return cand
    return resort_id.split("_", 1)[0]


def convert_scene(scene: Path, out: Path, resort_id: str, *, catalog_name: Optional[str] = None,
                  cell: float = 4.0, tile_cache: Optional[Path] = None) -> dict:
    """Write the game files for one scene into ``out`` and return its map-registry entry."""
    manifest = json.loads((scene / "scene-manifest.json").read_text(encoding="utf8"))
    out.mkdir(parents=True, exist_ok=True)
    cs = manifest["coordinate_system"]
    name = _display_name(catalog_name, manifest, resort_id)

    runs, lifts = convert_vectors(scene)
    if not runs:
        raise ValueError("no downhill runs")

    meta, elev = load_heightfield(scene)
    h, detail = build_detail(meta, elev, cell)
    q, lo, hi = to_u16(h)
    q.tofile(out / "detail.u16")
    detail.update(file="detail.u16", min=lo, max=hi)

    bq, blo, bhi = to_u16(fetch_backdrop(cs["origin_longitude"], cs["origin_latitude"], tile_cache))
    bq.tofile(out / "backdrop.u16")
    location = str(manifest.get("location") or "").strip()
    country = str(manifest.get("country") or "").strip()
    if country and country.casefold() in location.casefold():
        country = ""
    where = ", ".join(p for p in (location, country) if p)
    world = {"source": {"elevation": manifest["attribution"]["dem"], "pistes": manifest["attribution"]["osm"]},
             "name": name,
             "origin": {"lon": cs["origin_longitude"], "lat": cs["origin_latitude"]},
             "detail": detail,
             "backdrop": {"file": "backdrop.u16", "size": BACKDROP_SIZE, "cells": BACKDROP_CELLS,
                          "min": blo, "max": bhi},
             "runs": runs, "lifts": lifts,
             "lodges": find_lodges(scene, h, detail, lifts, cell),
             "title": name, "subtitle": where}
    (out / "world.json").write_text(json.dumps(world, ensure_ascii=False, separators=(",", ":")), encoding="utf8")
    write_diorama(out, world, cs, h, cell)
    return {"id": resort_id, "name": name, "subtitle": where,
            "lon": cs["origin_longitude"], "lat": cs["origin_latitude"],
            "runs": len(runs), "lifts": len(lifts), "min": round(detail["min"]), "max": round(detail["max"])}
