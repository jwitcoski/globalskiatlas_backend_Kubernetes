"""Load resort YAML and path defaults."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESORT_YAML = REPO_ROOT / "config" / "resorts" / "montage_mountain_pa.yaml"


@dataclass
class GameExportConfig:
    resort_id: str
    display_name: str
    winter_sports_id: str
    region: str
    state: str
    country: str
    approximate_location_name: str
    game_style: str
    seed: int
    status: str
    target_crs: str
    terrain_tile_size_m: float
    terrain_mesh_resolution_m: float
    heightfield_resolution_m: float
    collision_heightfield_resolution_m: float
    route_min_vertical_drop_m: float
    route_max_uphill_fraction: float
    route_min_length_m: float
    piste_corridor_default_half_width_m: float
    piste_corridor_min_half_width_m: float
    piste_corridor_max_half_width_m: float
    scene_bounds_buffer_m: float
    route_sample_spacing_m: float
    route_connect_endpoint_m: float
    steep_hazard_degrees: float
    building_buffer_m: float
    water_buffer_m: float
    road_buffer_m: float
    cliff_buffer_m: float
    terrain_boundary_buffer_m: float
    highway_hazard_types: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def region_dir_parts(self) -> tuple[str, ...]:
        return tuple(self.region.strip("/").split("/"))


def config_from_mapping(data: dict[str, Any], *, source: str = "") -> GameExportConfig:
    required = ("resort_id", "winter_sports_id", "region")
    missing = [k for k in required if not data.get(k)]
    if missing:
        where = source or "config"
        raise ValueError(f"{where}: missing required keys {missing}")
    return GameExportConfig(
        resort_id=str(data["resort_id"]),
        display_name=str(data.get("display_name") or data["resort_id"]),
        winter_sports_id=str(data["winter_sports_id"]).strip(),
        region=str(data["region"]).strip(),
        state=str(data.get("state") or ""),
        country=str(data.get("country") or ""),
        approximate_location_name=str(data.get("approximate_location_name") or ""),
        game_style=str(data.get("game_style") or "classic_arcade"),
        seed=int(data.get("seed") or 0),
        status=str(data.get("status") or "prototype"),
        target_crs=str(data.get("target_crs") or "auto_utm"),
        terrain_tile_size_m=float(data.get("terrain_tile_size_m") or 256),
        terrain_mesh_resolution_m=float(data.get("terrain_mesh_resolution_m") or 4),
        heightfield_resolution_m=float(data.get("heightfield_resolution_m") or 2),
        collision_heightfield_resolution_m=float(
            data.get("collision_heightfield_resolution_m") or 4
        ),
        route_min_vertical_drop_m=float(data.get("route_min_vertical_drop_m") or 25),
        route_max_uphill_fraction=float(data.get("route_max_uphill_fraction") or 0.10),
        route_min_length_m=float(data.get("route_min_length_m") or 75),
        piste_corridor_default_half_width_m=float(
            data.get("piste_corridor_default_half_width_m") or 18
        ),
        piste_corridor_min_half_width_m=float(
            data.get("piste_corridor_min_half_width_m") or 12
        ),
        piste_corridor_max_half_width_m=float(
            data.get("piste_corridor_max_half_width_m") or 35
        ),
        scene_bounds_buffer_m=float(data.get("scene_bounds_buffer_m") or 300),
        route_sample_spacing_m=float(data.get("route_sample_spacing_m") or 8),
        route_connect_endpoint_m=float(data.get("route_connect_endpoint_m") or 25),
        steep_hazard_degrees=float(data.get("steep_hazard_degrees") or 45),
        building_buffer_m=float(data.get("building_buffer_m") or 8),
        water_buffer_m=float(data.get("water_buffer_m") or 6),
        road_buffer_m=float(data.get("road_buffer_m") or 6),
        cliff_buffer_m=float(data.get("cliff_buffer_m") or 10),
        terrain_boundary_buffer_m=float(data.get("terrain_boundary_buffer_m") or 8),
        highway_hazard_types=list(data.get("highway_hazard_types") or []),
        raw=data,
    )


def load_resort_config(path: Path) -> GameExportConfig:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return config_from_mapping(data, source=str(path))


def default_config_path(resort_id: str) -> Path:
    return REPO_ROOT / "config" / "resorts" / f"{resort_id}.yaml"


def slugify_resort(name: str, country: str) -> str:
    base = f"{name} {country}".strip()
    s = re.sub(r"[^a-zA-Z0-9]+", "_", base).strip("_").lower()
    return (s[:48] or "resort")


def unique_resort_id(slug: str, winter_sports_id: str, used: Iterable[str]) -> str:
    taken = set(used)
    if slug not in taken:
        return slug
    return f"{slug[:32]}_{winter_sports_id}"[:64]


@lru_cache(maxsize=1)
def _default_gameplay_mapping() -> dict[str, Any]:
    data = yaml.safe_load(DEFAULT_RESORT_YAML.read_text(encoding="utf-8")) or {}
    skip = {
        "resort_id",
        "display_name",
        "winter_sports_id",
        "region",
        "state",
        "country",
        "approximate_location_name",
    }
    return {k: v for k, v in data.items() if k not in skip}


def config_from_candidate(row: dict[str, Any], *, used_ids: Iterable[str] = ()) -> GameExportConfig:
    wid = str(row.get("winter_sports_id") or "").strip()
    if not wid:
        raise ValueError("candidate missing winter_sports_id")
    region = str(row.get("region") or "").strip()
    if not region:
        raise ValueError(f"candidate {wid}: missing region")
    name = str(
        row.get("english_name")
        or row.get("display_name")
        or row.get("name")
        or f"resort_{wid}"
    ).strip()
    # Prefer Latin slug when OSM name is non-ASCII (e.g. Armenian script).
    slug_name = str(row.get("english_name") or name).strip() or name
    country = str(row.get("country") or "")
    slug = slugify_resort(slug_name, country)
    resort_id = unique_resort_id(slug, wid, used_ids)
    data = dict(_default_gameplay_mapping())
    data.update(
        {
            "resort_id": resort_id,
            "display_name": name,
            "winter_sports_id": wid,
            "region": region,
            "state": str(row.get("state") or ""),
            "country": country,
            "approximate_location_name": str(
                row.get("approximate_location_name") or row.get("state") or country or ""
            ),
        }
    )
    return config_from_mapping(data, source=f"candidate:{wid}")
