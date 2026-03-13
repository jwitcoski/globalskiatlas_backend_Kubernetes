# Iceberg registration (optional)

After [combine_regions.py](../scripts/combine_regions.py) writes `output/combined/*.parquet`, you can register those outputs as **Apache Iceberg tables** in AWS Glue. Each run of the registration script **overwrites** the table with the current combined data, creating a **new Iceberg snapshot**. Run it monthly (or after each full pipeline) to get versioning and time travel.

---

## Setup checklist

1. **Combined data**  
   Have Parquet in `output/combined/` (run the pipeline + `combine_regions.py` first, or use `--input-dir` to point elsewhere).

2. **AWS**  
   - **S3 bucket** for Iceberg data and metadata (e.g. same bucket as pipeline output or a dedicated one). The script will write under `s3://bucket/iceberg/<database>/<table_name>/`.  
   - **Glue**: use the default Glue Data Catalog in your region (no extra setup).  
   - **IAM**: credentials (env vars or profile) with:
     - **Glue**: `glue:CreateDatabase`, `glue:CreateTable`, `glue:GetTable`, `glue:UpdateTable`, `glue:GetDatabase`.  
     - **S3**: read from input dir (if on S3) and read/write to `s3://bucket/iceberg/*`.

3. **Local Python**  
   ```powershell
   pip install -r requirements-iceberg.txt
   ```  
   (Adds PyIceberg, PyArrow, pandas, boto3.)

4. **Run**  
   ```powershell
   python scripts/register_iceberg.py --s3-bucket YOUR_BUCKET
   ```  
   Use `--dry-run` first to confirm tables and paths. Optionally: `--database ski_atlas`, `--region us-east-1`, `--tables ski_areas_analyzed ski_areas lifts pistes`.

---

## Prerequisites

- Combined GeoParquet already in `output/combined/` (or another directory you pass with `--input-dir`).
- AWS account with **Glue** (Data Catalog) and **S3** permissions.
- Python deps: `pip install -r requirements-iceberg.txt` (adds PyIceberg and boto3).

## Usage

```powershell
# Required: S3 bucket for Iceberg table data and metadata
python scripts/register_iceberg.py --s3-bucket my-atlas-bucket

# Custom input dir (default: output/combined)
python scripts/register_iceberg.py --s3-bucket my-atlas-bucket --input-dir output/combined

# Only specific tables (default: ski_areas_analyzed, ski_areas, lifts, pistes)
python scripts/register_iceberg.py --s3-bucket my-atlas-bucket --tables ski_areas_analyzed

# Dry run (print what would be done, no catalog writes)
python scripts/register_iceberg.py --s3-bucket my-atlas-bucket --dry-run

# Glue database name and region
python scripts/register_iceberg.py --s3-bucket my-bucket --database ski_atlas --region us-east-1
```

## What it does

1. Reads each combined Parquet file (e.g. `ski_areas_analyzed.parquet`, `ski_areas.parquet`, …).
2. For tables with geometry (ski_areas, lifts, pistes), converts geometry to **WKB binary** (`geometry_wkb` column) because Iceberg has no native geometry type.
3. Creates the Iceberg table in Glue if it does not exist (schema from the Parquet, location `s3://bucket/iceberg/database/table_name`).
4. **Overwrites** the table with the current data, creating a new snapshot. So one run per month = one snapshot per month; you can query the table "as of" a previous snapshot.

## AWS permissions

- **Glue**: `glue:CreateDatabase`, `glue:CreateTable`, `glue:GetTable`, `glue:UpdateTable`, `glue:GetDatabase`, and table metadata in Glue Data Catalog.
- **S3**: Read from the input dir (if on S3) and read/write to `s3://bucket/iceberg/` (table data and Iceberg metadata).

## Optional: run after combine in the pipeline

To run registration automatically after `combine_regions.py` when using the local world pipeline:

1. Set `REGISTER_ICEBERG=1` and `S3_BUCKET=your-bucket` in the environment.
2. [scripts/run_all_regions_local.ps1](../scripts/run_all_regions_local.ps1) already calls `register_iceberg.py` after combine when both are set; no code change needed.

The per-region Docker pipeline (steps 1–11) and `combine_regions.py` are unchanged; Iceberg registration is an extra step you run locally (or in CI) after combine.

## Query the tables (prove it works)

To run a quick read from the Iceberg tables (Glue catalog) and print row counts + sample resorts:

```powershell
python scripts/query_iceberg.py --s3-bucket globalskiatlas-backend-k8s-output
```

Use `--json` to output JSON (e.g. for an API or to embed on an about page). Uses the same AWS credentials and `requirements-iceberg.txt` as registration.

## Upload stats for the Download Data page

The Global Ski Atlas [Download Data](https://globalskiatlas.com/DownloadData.html) page can show live Iceberg row counts and sample resorts. A Lambda in the frontend stack reads JSON from S3 and serves it at `/api/iceberg-stats`. To populate that JSON:

1. **Deploy the frontend stack** with `IcebergStatsBucket` set to this bucket (e.g. `globalskiatlas-backend-k8s-output`). The Lambda gets read-only access to the bucket; no public read is required.

2. **Run the upload script** after your Iceberg tables are updated (e.g. after `register_iceberg.py` or after a full pipeline run):

   ```powershell
   python scripts/upload_iceberg_stats.py --s3-bucket globalskiatlas-backend-k8s-output
   ```

   This runs `query_iceberg.py --json` and uploads the result to `s3://bucket/iceberg-stats/latest.json`. Options: `--limit 20`, `--region us-east-1`, `--dry-run` (print JSON only, no upload).

3. **Optional: run on a schedule** (e.g. after combine + register in CI or cron): call `upload_iceberg_stats.py` with the same bucket. The Download Data page will then show current table counts, latest snapshot time, and sample resorts.

## Frontend

The atlas frontend (DuckDB WASM → MapLibre) does not change. It continues to consume the same Parquet files (from the same URLs or S3 prefix). Iceberg is for catalog, versioning, and server-side analytics (e.g. Athena, Spark, Vector Ledger).
