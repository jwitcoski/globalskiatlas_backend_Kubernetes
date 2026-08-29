# Game-scene export — implementation plan

Additive, optional stage. Does **not** change OSM/PBF download, nearby extract, lifts/pistes extract, enrich, analyze, parquet conversion, 1000 ft buffer, translation, or elevation/contours generation.

## Existing pipeline (unchanged)

| Step | Entry | Output |
|------|--------|--------|
| Region run | `scripts/run_region_local.py` → Docker `Dockerfile.aws` → `scripts/run_iceland_pipeline_aws.sh` | `output/<continent>/<slug>/` |
| OSM winter_sports | `scripts/pbf_to_geojson.py` | `ski_areas.parquet` (via convert) |
| Nearby OSM | `scripts/extract_nearby_from_pbf.py` | `osm_near_winter_sports.parquet` |
| Lifts/pistes | `scripts/extract_lifts_and_pistes_from_pbf.py` | `lifts.parquet`, `pistes.parquet` |
| Buffer / contours / extrema | `scripts/ski_area_1000ft_buffer.py`, `scripts/ski_area_elevation_contours.py` | buffer, contours, elevation points; DEM CRS **EPSG:4326** int16 GeoTIFF **only if** `--save-dem` |
| DEM source | Mapzen Skadi (`https://elevation-tiles-prod.s3.amazonaws.com/skadi`), 1″ HGT, nodata `-32768` | `cache/skadi/` |

Canonical Montage Mountain (already documented in `atlas/map_gen/templates/README.md`):

- `winter_sports_id`: **45096232**
- `region`: **north-america/us/pennsylvania**
- Geofabrik PBF: `config/regions.yaml` slug `us/pennsylvania`

Per-region elevation **does not** pass `--save-dem`. Game export looks for an existing cropped GeoTIFF first, then a Skadi cache mosaic. It does not rewrite pipeline outputs.

## Reused modules (import only)

- `scripts/ski_area_elevation_contours.py`: `fetch_skadi_tile`, `_mosaic_tiles`, `_crop_dem_to_bbox`, `_write_dem_geotiff`, `HGT_NO_DATA`, `SKADI_BASE`, `_buffer_geom_meters` (via ski area AOI)
- `scripts/ski_area_1000ft_buffer.py` pattern: UTM zone from centroid (same math, reimplemented in `game_export/coords.py` so the buffer script is not modified)
- Pipeline parquets: `ski_areas.parquet`, `osm_near_winter_sports.parquet`, `pistes.parquet`, `lifts.parquet`, `ski_area_elevation_points.parquet` (optional)
- Config style: YAML like `config/atlas.yaml` / `config/regions.yaml`

## New files

| Path | Role |
|------|------|
| `config/resorts/montage_mountain_pa.yaml` | PoC resort + game thresholds |
| `game_export/__init__.py` | Package |
| `game_export/__main__.py` | `python -m game_export` |
| `game_export/cli.py` | CLI: `--resort`, `--dry-run`, `--force` |
| `game_export/config.py` | Load YAML, resolve paths |
| `game_export/inputs.py` | Locate OSM/DEM; fail if missing |
| `game_export/coords.py` | UTM + local origin + game axes |
| `game_export/terrain.py` | Heightfields, derivatives, mesh |
| `game_export/glb.py` | Minimal glTF 2.0 GLB writer |
| `game_export/vectors.py` | OSM → local GeoJSON layers |
| `game_export/routes.py` | Downhill graph, spawns, courses |
| `game_export/hazards.py` | Exclusion zones |
| `game_export/qa.py` | QA PNGs, validation reports |
| `game_export/manifest.py` | Manifest + attribution + scene README |
| `scripts/game_export.py` | Thin wrapper |
| `scripts/test_game_export_smoke.py` | Smoke test (synthetic fixture) |
| `docker-compose.game-export.yml` | Optional compose service |
| README section `Montage Mountain game-scene export` | Commands and contract |

No new Python packages required: NumPy, Rasterio, GeoPandas, Shapely, PyProj (via GeoPandas), Matplotlib. No NetworkX, trimesh, or SciPy.

## Output contract

`output/game_scenes/montage_mountain_pa/<scene_version>/` as specified in the task (manifest, terrain, vectors, gameplay, qa, attribution).

`scene_version` = `v0-` + 12-char SHA-256 of (config bytes + DEM fingerprint + OSM layer fingerprints). Same inputs reuse the same directory unless `--force`.

## Docker

```text
docker compose -f docker-compose.game-export.yml run --rm game-export
docker compose -f docker-compose.game-export.yml run --rm game-export --fetch-skadi
```

or

```text
docker run --rm -v %CD%/output:/data -v %CD%/cache:/cache -v %CD%/config:/app/config globalskiatlas-pipeline python -m game_export --resort montage_mountain_pa --data-root /data --cache-dir /cache
```

`Dockerfile.aws` only gains `COPY game_export/` and `COPY config/`; default `CMD` stays the Iceland/world pipeline.

## Limitations (Montage OSM)

Pennsylvania extract may have incomplete `piste:type=downhill` mapping. Routes are derived from whatever OSM geometry exists; missing trails are not invented. Status fields (`approved` / `review_needed` / `rejected`) make that explicit.
