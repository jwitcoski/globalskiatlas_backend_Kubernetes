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
