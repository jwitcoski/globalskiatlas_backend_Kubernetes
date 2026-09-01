"""Refresh OSM vector GeoJSON in an existing scene without rebuilding DEM."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from game_export.config import default_config_path, load_resort_config
from game_export.coords import LocalCRS, make_transformers
from game_export.inputs import resolve_inputs
from game_export.vectors import add_ski_area_polygon, collect_layers, write_local_geojson


LAYER_FILES = {
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


def main() -> int:
    scene = Path(sys.argv[1])
    man = json.loads((scene / "scene-manifest.json").read_text(encoding="utf-8"))
    cs = man["coordinate_system"]
    local = LocalCRS(
        source_crs=cs["source_crs"],
        projected_crs=cs["projected_crs"],
        origin_easting_m=cs["origin_easting_m"],
        origin_northing_m=cs["origin_northing_m"],
        origin_longitude=cs["origin_longitude"],
        origin_latitude=cs["origin_latitude"],
    )
    cfg = load_resort_config(default_config_path("montage_mountain_pa"))
    inputs = resolve_inputs(REPO / "output", cfg, REPO / "cache", fetch_skadi=False)
    to_proj, _ = make_transformers(local.projected_crs)
    layers, repairs = collect_layers(
        inputs.osm_nearby, inputs.pistes, inputs.lifts, to_proj, local, cfg
    )
    add_ski_area_polygon(layers, inputs.ski_polygon, to_proj, local, repairs)
    vec = scene / "vectors"
    vec.mkdir(exist_ok=True)
    written = {}
    for name, fname in LAYER_FILES.items():
        feats = layers.get(name) or []
        write_local_geojson(vec / fname, feats, local, name)
        written[name] = len(feats)
    man.setdefault("vectors", {}).update(
        {
            "pistes": "vectors/pistes.geojson",
            "lifts": "vectors/lifts.geojson",
            "buildings": "vectors/buildings.geojson",
            "water": "vectors/water.geojson",
            "forest": "vectors/forest.geojson",
            "roads": "vectors/roads.geojson",
            "cliffs": "vectors/cliffs.geojson",
            "grassland": "vectors/grassland.geojson",
            "parking": "vectors/parking.geojson",
            "ski_area": "vectors/ski-area.geojson",
            "barriers": "vectors/barriers.geojson",
            "piste_corridors": "vectors/piste-corridors.geojson",
            "exclusion_zones": "vectors/exclusion-zones.geojson",
            "route_centers": "vectors/route-centers.geojson",
        }
    )
    (scene / "scene-manifest.json").write_text(json.dumps(man, indent=2) + "\n", encoding="utf-8")
    print("wrote", written, "repairs", len(repairs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
