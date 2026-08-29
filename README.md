# Global Ski Atlas — Data Pipeline & GeoParquet

This repository is the data pipeline for [Global Ski Atlas](https://globalskiatlas.com). It ingests OpenStreetMap PBFs by region, produces ski areas, lifts, and pistes as GeoJSON and **GeoParquet**. Combined outputs in `output/combined/` are used by the atlas and are suitable for **ArcGIS**, analytics (DuckDB, Spark), and open standards.

Part of [Vector Scope AI](https://vectorscopeai.com) / **VectorLedger** — collaborative map editing with full version history.

## Quick start

- **Run by region:** See [docs/RUN_BY_REGION.md](docs/RUN_BY_REGION.md) for Docker and world-pipeline usage.
- **Local workflow:** See [docs/LOCAL_WORKFLOW.md](docs/LOCAL_WORKFLOW.md).
- **Mapper workflow (QGIS):** See [docs/MAPPER_WORKFLOW.md](docs/MAPPER_WORKFLOW.md).
- **CI/CD (local → GitHub → Actions → AWS):** See [docs/WORKFLOW_SETUP.md](docs/WORKFLOW_SETUP.md) to establish build-and-push to ECR and optional ECS runs.

## Outputs

- **`output/combined/*.parquet`** — GeoParquet (ski_areas, lifts, pistes, ski_areas_analyzed, etc.). Use in ArcGIS Pro, ArcGIS Online, Experience Builder, DuckDB, Spark, or any tool that reads GeoParquet.
- **GeoJSON export** — Run `scripts/export_combined_to_geojson.py` to export Parquet → GeoJSON for MapTiler, ArcGIS, or other web map use (output in `output/combinedmaptiler/` by default).

## Versioned outputs (optional)

For versioned GeoParquet snapshots (e.g. for VectorLedger or audit), copy combined output to a dated path or S3 prefix after each run (e.g. `output/combined/YYYY-MM-DD/` or `s3://bucket/combined/YYYY-MM-DD/`). See [docs/VERSIONED_OUTPUTS.md](docs/VERSIONED_OUTPUTS.md) if you add that doc.

**Iceberg (optional):** After combine, run `scripts/register_iceberg.py` to register combined Parquet as Iceberg tables in AWS Glue. Each run creates a new snapshot (monthly versioning, time travel). See [docs/ICEBERG.md](docs/ICEBERG.md).

## Links

- [Global Ski Atlas](https://globalskiatlas.com) — live atlas
- [Vector Scope AI / VectorLedger](https://vectorscopeai.com)
- [globalskiatlas](https://github.com/jwitcoski/globalskiatlas) — web app repo

Combined GeoParquet and exported GeoJSON can be used in **ArcGIS Pro**, **ArcGIS Online**, or **Experience Builder** for visualization and editing workflows.

## Montage Mountain game-scene export

Optional additive stage. It does **not** replace OSM/DEM ingestion, contours, or high/low points.

**Default inputs are S3** (`s3://globalskiatlas-backend-k8s-output/<region>/`): regional parquet (ski areas, pistes, lifts, nearby OSM) and cropped DEM GeoTIFFs from the elevation upload. Elevation-only regional prefixes often lack pistes/lifts; those fall back to `combined/*.parquet` and are clipped to the resort. AWS credentials with `s3:GetObject` are required (regional prefixes are **not** public; only `combined/*` is). Objects cache under `cache/s3_game_export/`. The Three.js skier lives in **GlobalSkiAtlas_2** and loads published cakes from `s3://globalskiatlas-backend-k8s-output/game_scenes/`.

**Local disk override:** `--data-root output` (implies `--no-from-s3`), plus a cropped DEM or `--fetch-skadi`.

If OSM or DEM inputs are missing, the command exits with a clear error. Pipeline folders are not overwritten.

**Commands (from repo root):**

```powershell
# After pip install -r requirements.txt — needs AWS creds (env or profile)
python -m game_export --resort montage_mountain_pa --dry-run
python -m game_export --picked-batch
python scripts/game_export.py --resort montage_mountain_pa --dry-run

# Local GIS only (does not use S3)
python -m game_export --resort montage_mountain_pa --data-root output
python scripts/test_game_export_smoke.py

# Docker (pass through AWS_* from the host)
docker build -f Dockerfile.aws -t globalskiatlas-pipeline .
docker compose -f docker-compose.game-export.yml run --rm game-export --resort montage_mountain_pa --dry-run
docker compose -f docker-compose.game-export.yml run --rm game-export --picked-batch
# If no cropped DEM GeoTIFF exists on S3, fetch Mapzen Skadi like the elevation stage:
docker compose -f docker-compose.game-export.yml run --rm game-export --fetch-skadi
```

`--picked-batch` exports the 10 live playable resorts in `config/resorts/_picked_batch.json` plus Montage — not the full ~2000 ski areas.

**Output:** `output/game_scenes/<resort_id>/<scene_version>/`  
Version id is `v0-` + a content hash of config + DEM + OSM fingerprints (S3 ETags when inputs came from the bucket). Use `--force` to rebuild the same version.

**Coordinate convention (Three.js Y-up):** game X = local east m, Y = elevation m, Z = negative local north m. Vector GeoJSON in `vectors/` uses local east/north meters and is **not** WGS84 — see `scene-manifest.json` and the scene `README.md`.

**Limitations:** OSM downhill pistes at Montage may be incomplete. Routes are tagged `approved` / `review_needed` / `rejected`. Not an official map or safety product.

The browser game is not in this repo. Scene cakes (manifest, heightfield, mesh, vectors) are consumed by the GlobalSkiAtlas_2 playable client.
