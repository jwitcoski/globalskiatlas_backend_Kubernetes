"""Scene manifest, README, attribution."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from game_export.coords import LocalCRS
from game_export import jsonutil


ATTRIBUTION_MD = """# Attribution

This prototype game scene is derived from open geodata.

## OpenStreetMap

© OpenStreetMap contributors

- Copyright and license: https://www.openstreetmap.org/copyright
- Database license: Open Database License (ODbL) 1.0  
  https://opendatacommons.org/licenses/odbl/

If you use these vector layers, you must provide OSM attribution as required by the ODbL.

## Elevation (DEM)

Mapzen Skadi tiles (AWS Terrain Tiles / Mapzen elevation), SRTM-style 1 arc-second HGT.

Source used by this repository's existing elevation stage:

`https://elevation-tiles-prod.s3.amazonaws.com/skadi`

See also AWS Terrain Tiles terms: https://registry.opendata.aws/terrain-tiles/

## Affiliation

This output is **not affiliated with, endorsed by, or approved by Montage Mountain** (or any resort operator).

## Safety

**Do not use this output for skiing, navigation, or safety.** It is a derived arcade-prototype terrain package. Mapped pistes may be incomplete, outdated, or geometrically inaccurate.
"""


SCENE_README = """# Montage Mountain prototype game scene

**Local-meter GeoJSON is not WGS84.** `geojson_x` = local east meters, `geojson_y` = local north meters.

Game axes for Three.js (Y-up):

- X = local east meters
- Y = elevation meters
- Z = **negative** local north meters

Heightfield reconstruction:

```
elevation_m = elevation_offset_m + uint16_value * elevation_scale_m
```

`uint16` nodata = 65535. Binary layout: row-major, row 0 = north, col 0 = west, little-endian.

This package is a **prototype**, not an official trail map.
"""


def write_attribution(out: Path) -> None:
    att = out / "attribution"
    att.mkdir(parents=True, exist_ok=True)
    (att / "ATTRIBUTION.md").write_text(ATTRIBUTION_MD, encoding="utf-8")
    (att / "sources.json").write_text(
        jsonutil.dumps(
            {
                "osm": {
                    "attribution": "© OpenStreetMap contributors",
                    "license": "ODbL 1.0",
                    "url": "https://www.openstreetmap.org/copyright",
                },
                "dem": {
                    "provider": "Mapzen Skadi / AWS Terrain Tiles",
                    "url": "https://elevation-tiles-prod.s3.amazonaws.com/skadi",
                    "license_notes": "Public elevation tiles; see AWS Open Data Terrain Tiles registry",
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def write_manifest(out: Path, cfg, local: LocalCRS, terrain_meta: dict, build_seed: int) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = {
        "scene_schema_version": "0.1.0",
        "scene_id": cfg.resort_id,
        "winter_sports_id": cfg.winter_sports_id,
        "scene_version": out.name,
        "build_timestamp_utc": now,
        "build_seed": build_seed,
        "status": cfg.status,
        "display_name": f"{cfg.display_name} — Prototype",
        "country": cfg.country,
        "location": cfg.approximate_location_name,
        "coordinate_system": local.to_dict(),
        "terrain": {
            "heightfield": "terrain/heightfield-u16.bin",
            "heightfield_metadata": "terrain/heightfield-metadata.json",
            "collision_heightfield": "terrain/collision-heightfield-u16.bin",
            "mesh": "terrain/terrain-mesh.glb",
            "elevation_min_m": terrain_meta.get("elevation_min_m"),
            "elevation_max_m": terrain_meta.get("elevation_max_m"),
        },
        "vectors": {
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
        },
        "gameplay": {
            "routes_graph": "gameplay/routes.graph.json",
            "spawn_points": "gameplay/spawn-points.json",
            "course_candidates": "gameplay/course-candidates.json",
            "terrain_query_metadata": "gameplay/terrain-query-metadata.json",
        },
        "qa": {
            "validation_report_json": "qa/validation-report.json",
            "validation_report_markdown": "qa/validation-report.md",
        },
        "attribution": {
            "osm": "© OpenStreetMap contributors",
            "dem": "Mapzen Skadi / AWS Terrain Tiles",
            "license_notes": "OSM data ODbL; DEM from Mapzen Skadi. See attribution/ATTRIBUTION.md",
        },
        "disclaimer": (
            "Prototype terrain scene derived from open data. Not an official resort map, "
            "trail map, navigation tool, or safety product."
        ),
    }
    (out / "scene-manifest.json").write_text(jsonutil.dumps(manifest), encoding="utf-8")
    (out / "README.md").write_text(SCENE_README, encoding="utf-8")
    return manifest
