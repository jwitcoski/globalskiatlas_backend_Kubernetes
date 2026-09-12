# Wiki region clay scenes (state / country)

Decorative 3D clay islands for wiki pages whose `pageType` is `state` or `country`. This is **not** a playable `game_scenes` cake and **not** a per-resort `clay_scenes/{resort_id}/` island.

Join key is wiki **`pageId`**. Do not put these entries in `clay_scenes/catalog.json` (that catalog joins on `winter_sports_id`).

## `region_id` slug rule

`region_id` **is** the wiki `pageId`:

| pageType | `pageId` / `region_id` |
| --- | --- |
| state | `state-{state-slug}-{country-slug}` |
| country | `country-{country-slug}` |

Slugs: lowercase, spaces → hyphens, keep `[a-z0-9-]`. Same helper as wiki ingest (`atlas/map_gen/wiki_page_id.py`).

Examples:

- `state-west-virginia-united-states-of-america`
- `country-united-states-of-america`

S3 / site path:

```
clay_scenes/regions/catalog.json
clay_scenes/regions/{region_id}/scene-manifest.json
clay_scenes/regions/{region_id}/terrain/terrain-mesh.glb
clay_scenes/regions/{region_id}/vectors/admin-boundary.geojson
clay_scenes/regions/{region_id}/vectors/resorts.geojson
clay_scenes/regions/{region_id}/vectors/highways.geojson
clay_scenes/regions/{region_id}/vectors/water.geojson
clay_scenes/regions/{region_id}/vectors/places.geojson
```

Highways, water, and places are **baked at export** from Natural Earth 10m (roads, river centerlines, lakes, populated places), clipped to the admin polygon, in the same `local_game_meters` CRS as `resorts.geojson`. The wiki client must not fetch OSM/Overpass for these layers.

Country scenes also include `vectors/admin-1.geojson`: downhill state/province polygons in that same local CRS. Each feature has wiki `pageId` (`state-{state-slug}-{country-slug}`), `has_region_scene`, and `scene` so a pick can load `clay_scenes/regions/{pageId}/scene-manifest.json`. Only admin-1 units that have downhill resorts (the same set as `--all-admin1`) are included.

Refresh context without rebuilding the mesh:

```powershell
python scripts/bake_region_clay.py --osm-only --state "West Virginia" --country "United States of America" --upload
```

Optional lookup: `clay_scenes/regions/by-page/{pageId}.json`.

`scene_kind` is `wiki_region`. Vectors are **local meters** (`geojson_x` = east, `geojson_y` = north), same contract as resort `ski-area-buffer.geojson`. Island rim = admin polygon, not a bounding box.

`terrain.height_exaggerate` (and `camera.height_exaggerate`) is a suggested vertical scale so rolling states are not pancakes. Frontend should prefer this over the resort default of `2`.

## First scene (West Virginia)

```powershell
python scripts/bake_region_clay.py --state "West Virginia" --country "United States of America" --force
python scripts/upload_region_clay_scenes.py --only state-west-virginia-united-states-of-america
```

Equivalent:

```powershell
python -m game_export --region-clay --state "West Virginia" --country "United States of America" --force --data-root output --no-from-s3
```

(`--fetch-skadi` is implied: region DEM always mosaics Mapzen Skadi tiles.)

## Punch out every state / province

Discovers admin-1 units (states, provinces, territories) that have downhill rows in `ski_areas_analyzed.parquet`, then bakes each `wiki_region` scene. Skips country/continent pages. Continues after a failed unit; progress is `config/clay_scenes/regions/_progress.json`.

```powershell
python scripts/bake_region_clay.py --all-admin1 --list
python scripts/bake_region_clay.py --all-admin1 --skip-existing
python scripts/bake_region_clay.py --all-admin1 --country "United States of America" --skip-existing --upload
python scripts/bake_region_clay.py --all-admin1 --country "Canada" --skip-existing
```

A full world run downloads Skadi tiles per polygon and can take hours. Use `--limit 3` first. Re-run with `--skip-existing` to resume. `--force` rebuilds meshes.

## Countries

Country islands use the Natural Earth admin-0 outline (dateline-unwrapped so the USA is not a globe-wide bbox), a **Lambert azimuthal** projection centered on the country, and **AWS Terrarium** DEM tiles instead of 1″ Skadi (the USA Skadi bbox would be ~19k tiles). Ski-area footprints are omitted at this scale; resort points still come from the downhill parquet.

```powershell
python scripts/bake_region_clay.py --country "United States of America" --force
python scripts/upload_region_clay_scenes.py --only country-united-states-of-america
```

Overnight, every downhill country (skips scenes that are already complete; USA is skipped):

```powershell
python -u scripts/bake_all_country_clay.py --list
python -u scripts/bake_all_country_clay.py
python -u scripts/bake_all_country_clay.py --upload
```

Progress is `config/clay_scenes/regions/_progress_countries.json`. Re-run the same command to resume. `--force` rebuilds every country.

Admin outline and context vectors come from Natural Earth 10m. State DEM is Skadi resampled to ~0.25–2 km so the Draco GLB stays in the few-MB range. Country DEM is Terrarium at zoom 4–6.
