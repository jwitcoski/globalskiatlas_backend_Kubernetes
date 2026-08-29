"""Scan exported game scenes into a playable mountain catalog."""
from __future__ import annotations

import json
from pathlib import Path

from game_export.config import default_config_path, load_resort_config
from game_export.jsonutil import dumps

CATALOG_DISCLAIMER = (
    "Prototype scenes from OpenStreetMap + Mapzen Skadi. "
    "Not official maps, not for navigation or safety."
)


def catalog_entry_from_scene(scene: Path, resort_dir_name: str | None = None) -> dict:
    resort_dir_name = resort_dir_name or scene.parent.name
    man = json.loads((scene / "scene-manifest.json").read_text(encoding="utf-8"))
    country = man.get("country") or ""
    location = man.get("location") or ""
    name = str(man.get("display_name") or resort_dir_name).replace(" — Prototype", "")
    cs = man.get("coordinate_system") or {}
    origin = cs.get("local_origin") or {}
    lon = origin.get("longitude", cs.get("origin_longitude"))
    lat = origin.get("latitude", cs.get("origin_latitude"))
    cfg_path = default_config_path(resort_dir_name)
    if cfg_path.is_file():
        try:
            cfg = load_resort_config(cfg_path)
            country = country or cfg.country
            location = location or cfg.approximate_location_name
            name = name or cfg.display_name
        except Exception:
            pass
    entry = {
        "id": man.get("scene_id") or resort_dir_name,
        "name": name,
        "country": country,
        "location": location,
        "path": f"{resort_dir_name}/{scene.name}",
        "scene_version": scene.name,
    }
    wid = man.get("winter_sports_id")
    if wid:
        entry["winter_sports_id"] = str(wid)
    if lon is not None and lat is not None:
        entry["lon"] = float(lon)
        entry["lat"] = float(lat)
    return entry


def merge_catalog_resorts(existing: list[dict], incoming: list[dict]) -> list[dict]:
    by_id: dict[str, dict] = {}
    for row in existing:
        rid = str(row.get("id") or "")
        if rid:
            by_id[rid] = dict(row)
    for row in incoming:
        rid = str(row.get("id") or "")
        if not rid:
            continue
        prev = by_id.get(rid) or {}
        merged = {**prev, **row}
        by_id[rid] = merged
    return [by_id[k] for k in sorted(by_id)]


def write_catalog(out_root: Path) -> Path:
    resorts = []
    for resort_dir in sorted(p for p in out_root.iterdir() if p.is_dir() and p.name != "playable"):
        versions = sorted(
            (p for p in resort_dir.iterdir() if p.is_dir() and (p / "scene-manifest.json").is_file()),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not versions:
            continue
        resorts.append(catalog_entry_from_scene(versions[0], resort_dir.name))
    payload = {
        "disclaimer": CATALOG_DISCLAIMER,
        "resorts": resorts,
    }
    dest = out_root / "catalog.json"
    dest.write_text(dumps(payload), encoding="utf-8")
    return dest
