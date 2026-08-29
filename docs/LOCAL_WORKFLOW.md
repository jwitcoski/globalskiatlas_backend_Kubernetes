# Local Pipeline Workflow (No AWS)

Run the ski atlas pipeline locally with Docker. Regions are defined in `config/regions.yaml`. Each region runs as a separate Docker task: download PBF from Geofabrik, run the 11-step pipeline, write output to `output/<continent>/<slug>/`.

---

## Quick Start

```powershell
# Build the pipeline image (once)
docker build -f Dockerfile.aws -t globalskiatlas-pipeline .

# List all regions
python scripts/run_region_local.py --list

# Run one region (e.g. Iceland, Austria, California)
python scripts/run_region_local.py --continent europe --slug iceland
python scripts/run_region_local.py --continent north-america --slug us/california

# Run all regions in a continent (e.g. Europe)
python scripts/run_region_local.py --continent europe

# Resume from a specific region (e.g. skip already-done regions)
python scripts/run_region_local.py --continent europe --from-slug germany/bayern/oberpfalz

# Dry run (print docker command only)
python scripts/run_region_local.py --continent europe --slug austria --dry-run
```

---

## How It Works

1. **Config** (`config/regions.yaml`): Defines all regions with Geofabrik PBF URLs. Large areas are split into sub-regions or use `cluster_dist_m` to avoid OOM (see below).
2. **Docker**: Each region runs in a container that downloads its PBF, runs the 11-step pipeline (extract winter_sports → osm_nearby → lifts/pistes → enrich → analyze → parquet → 1000 ft buffer → translate → elevation/contours → re-export CSV), and writes to a mounted volume.
3. **Output**: `output/<continent>/<slug>/` — e.g. `output/europe/germany/baden-wuerttemberg/tuebingen-regbez/` or `output/north-america/us/colorado/`.

---

## Output Structure

```
output/
  europe/
    austria/
    germany/
      baden-wuerttemberg/
        tuebingen-regbez/
        freiburg-regbez/
        ...
      bayern/
        oberpfalz/
        ...
    iceland/
    ...
  north-america/
    canada/
      alberta/
      british-columbia/
      ...
    us/
      california/
      colorado/
      ...
  combined/                    # After running combine script
    ski_areas.parquet
    ski_areas_analyzed.parquet
    ski_areas_elevation.parquet
    ski_area_contours.geojson / .parquet
    ski_area_elevation_points.geojson / .parquet
    ski_areas_1000ft_buffer.geojson / .parquet
    lifts.parquet
    pistes.parquet
    osm_near_winter_sports.parquet
    dems/                         # After elevation script --save-dem
      <region>/
        <winter_sports_id>.tif    # Cropped DEM per ski area (GeoTIFF)
    ...
```

**Outputs per region** (each `output/<continent>/<slug>/`): `ski_areas.parquet`, `ski_areas_analyzed.csv`, `ski_areas_analyzed.parquet`, `lifts.parquet`, `pistes.parquet`, `osm_near_winter_sports.parquet`, `ski_areas_elevation.parquet`, `ski_area_contours.geojson`, `ski_area_contours.parquet`, `ski_area_elevation_points.geojson`, `ski_area_elevation_points.parquet`, `ski_areas_1000ft_buffer.geojson`, `ski_areas_1000ft_buffer.parquet`.

---

## Combine Regions

After running one or more regions, merge into a single dataset. When present, the script also combines `ski_areas_elevation.parquet`, `ski_area_contours.parquet`, `ski_area_elevation_points.parquet`, and `ski_areas_1000ft_buffer.parquet`.

```powershell
python scripts/combine_regions.py
```

Or specify output dir and regions:

```powershell
python scripts/combine_regions.py -o output -r europe north-america
```

---

## Register as Iceberg tables (optional)

After combining, you can register the combined GeoParquet as **Iceberg tables** in AWS Glue so each pipeline run creates a new snapshot (monthly versioning, time travel). The frontend (DuckDB WASM → MapLibre) is unchanged; Iceberg is an additional layer for analytics and versioning.

```powershell
pip install -r requirements-iceberg.txt
python scripts/register_iceberg.py --s3-bucket YOUR_BUCKET --input-dir output/combined
```

Requires AWS credentials (env or profile) with Glue and S3 access. See [docs/ICEBERG.md](ICEBERG.md) for details, `--dry-run`, and optional pipeline hook.

---

## Translate resort names (optional)

After combining, translate non-Latin ski area names to English using **googletrans**. Fills `english_name` when missing; skips US, Canada, UK, Australia, New Zealand, Ireland. Uses `cache/name_translations.json` to avoid re-translating.

```powershell
pip install googletrans==4.0.2   # or pip install -r requirements.txt
python scripts/translate_resort_names.py -i output/combined/ski_areas_analyzed.parquet -o output/combined/ski_areas_analyzed.parquet --cache cache/name_translations.json
```

Input: `output/combined/ski_areas_analyzed.parquet`. Adds or fills `english_name`. Use `--limit 100` to test. `ski_areas_analyzed` is the main table; display `english_name` or `name` when joining other layers. Per-region runs do this inside the pipeline (step 9); the pipeline also re-exports `ski_areas_analyzed.csv` from the parquet after the elevation step so the CSV includes `elevation_low_m`, `elevation_high_m`, `ski_north_angle`.

---

## Elevation and contours

After combining regions, you can add elevation (min/max) and contour lines per ski area for atlas maps. Uses Mapzen Skadi DEM on AWS S3 (free, no account). Tiles are cached under `cache/skadi/`.

```powershell
# Requires: pip install -r requirements.txt (geopandas, rasterio, matplotlib, etc.)
python scripts/ski_area_elevation_contours.py
```

Input: `output/combined/ski_areas.parquet` (polygon or point geometry). Outputs: `ski_areas_elevation.parquet` (elevation_low_m, elevation_high_m) and `ski_area_contours.geojson` / `.parquet`. Join to `ski_areas_analyzed` on `(winter_sports_id, region)`. Use `--limit N` to test on a few areas first. Add `--save-dem` to write a cropped DEM GeoTIFF per ski area under `output/combined/dems/<region>/<winter_sports_id>.tif` for later use (e.g. hillshade, different contour intervals).

### Batched elevation (disk-safe)

OSM preflight (no DEM download), then Docker per region, optional S3, then delete the Skadi cache. Default candidates: downhill resort, ≥1 downhill piste, longest trail ≥ 75 m. Unnamed trails are allowed.

```powershell
python scripts/elevation_preflight.py --report-only
python scripts/elevation_preflight.py --region north-america/us/virginia
python scripts/run_elevation_batches.py --region north-america/us/virginia --no-upload --keep-dems
# Overnight (all candidate regions that have local ski_areas.parquet):
python scripts/run_elevation_batches.py --upload
```

Uses image `globalskiatlas-pipeline`. `--upload` writes elevation parquet/geojson and `dems/**/*.tif` to `s3://globalskiatlas-backend-k8s-output/<region>/` (overwrite in place). This is **not** playable game-scene export.

**TODO after overnight elevation finishes:** free ~20 GB by deleting regional `output/` trees (`europe`, `asia`, `north-america`, `africa`, `australia-oceania`, `south-america`, leftover `**/cache/**/*.hgt`). **Keep** `output/combined/`, `atlas_work/`, `output/game_scenes/` (scene cakes only; the skier UI is in GlobalSkiAtlas_2), and all source (`atlas/`, `config/`, `scripts/`, `game_export/`). Do not delete `output/combined`.

---

## OOM Avoidance (Large Regions)

The pipeline downloads PBFs and extracts OSM data around ski areas. Large PBFs (e.g. 600 MB+) or many ski areas in one cluster can cause out-of-memory failures. We use two strategies:

| Strategy | When | Example |
|----------|------|---------|
| **Sub-regions** | Geofabrik has smaller extracts | Baden-Württemberg (601 MB) → 4 Bezirke (115–197 MB each); Bayern (793 MB) → 7 Bezirke; Netherlands (1.3 GB) → 12 provinces |
| **cluster_dist_m** | No sub-regions; need smaller OSM extracts | Italy nord-est/nord-ovest (30000 m); Russia central-fed-district (20000 m); US ski states + Canadian provinces (25000 m) |

`cluster_dist_m` splits ski areas into clusters by distance; each cluster gets a separate OSM extract. Smaller value = more clusters = less memory per run.

See `config/regions.yaml` and `docs/RUN_BY_REGION.md` for details.

---

## Prerequisites

- Docker Desktop
- Python 3.11+ (for `run_region_local.py` and `combine_regions.py`)

Optional: `pip install pyyaml` if not already installed.
