# Running the Pipeline by Region

The pipeline runs **one region at a time** using `config/regions.yaml`. Each region = one Geofabrik PBF → full 8-step pipeline → output in `output/<continent>/<slug>/`.

**Full continents** (Africa, South America, Australia–Oceania) have a single PBF each. **Large continents** (Europe, North America, Asia) are split into countries, states, or sub-regions so each run stays under ~1 GB and avoids OOM.

---

## Config (`config/regions.yaml`)

Regions are defined with `slug` and `pbf_url`. Optional `cluster_dist_m` reduces memory by clustering ski areas for OSM extraction (smaller = more clusters = less memory per run).

### Region Layout

- **North America**: Greenland, Mexico; **Canada** by province (e.g. `canada/alberta`, `canada/british-columbia`); **US** by state (e.g. `us/california`, `us/colorado`).
- **Europe**: One entry per country. Exceptions:
  - **Germany**: `germany/baden-wuerttemberg/tuebingen-regbez`, `germany/baden-wuerttemberg/freiburg-regbez`, … (4 sub-regions); `germany/bayern/oberpfalz`, … (7 sub-regions). Full Baden-Württemberg and Bayern OOM on local runs.
  - **Netherlands**: 12 provinces (e.g. `netherlands/flevoland`, `netherlands/utrecht`) — full 1.3 GB OOMs.
  - **Russia**: Federal districts (e.g. `russia/central-fed-district`).
  - **France**, **Italy**, **Spain**, **UK**: By macro region (e.g. `france/rhone-alpes`, `italy/nord-est`).
- **Asia**: **China** by province; **Japan** by subregion (e.g. `japan/hokkaido`); other countries as single region.

### OOM Avoidance

| Region Type | Approach | Example |
|-------------|----------|---------|
| **Sub-regions** | Use Geofabrik subdivisions instead of full region | Baden-Württemberg → tuebingen, freiburg, karlsruhe, stuttgart; Bayern → 7 Bezirke; Netherlands → 12 provinces |
| **cluster_dist_m** | Smaller clusters for OSM extract | Italy nord-est/nord-ovest: 30000; Russia central-fed-district: 20000; US (CA, CO, MI, NH, NY, PA, UT, VT), Canada (AB, BC, ON, QC): 25000; Czech, Finland, Georgia: 40000–50000 |

Crashes often occur at step 3 (Extracting OSM nearby) when a single cluster has many ski areas or a large PBF.

---

## Local Run (Docker)

1. **Build the pipeline image** (once):
   ```bash
   docker build -f Dockerfile.aws -t globalskiatlas-pipeline .
   ```

2. **List regions**:
   ```bash
   python scripts/run_region_local.py --list
   ```

3. **Run one region**:
   ```bash
   python scripts/run_region_local.py --continent europe --slug austria
   python scripts/run_region_local.py --continent north-america --slug us/colorado
   python scripts/run_region_local.py --continent asia --slug japan/hokkaido
   ```

4. **Run all regions in a continent**:
   ```bash
   python scripts/run_region_local.py --continent europe
   ```

5. **Resume from a region** (skip already-done):
   ```bash
   python scripts/run_region_local.py --continent europe --from-slug russia/central-fed-district
   ```

6. **Dry run** (print `docker run` command only):
   ```bash
   python scripts/run_region_local.py --continent europe --slug austria --dry-run
   ```

---

## World Pipeline (continent / country / state)

To run the **full world** (or a continent) with output and S3 organized by **continent → country → state**, use the world runner. It loops over every region in `config/regions.yaml` and runs the same 11-step pipeline per region, with `REGION` set to the full path (e.g. `europe/iceland`, `north-america/us/colorado`, `africa`).

**Scripts:**

- **`scripts/list_regions_for_pipeline.py`** — Lists regions (REGION, PBF_URL, optional cluster_dist_m). Optional `--continent` and `--slug` filters.
- **`scripts/run_world_pipeline_aws.sh`** — Runs the pipeline for each region; output under `$DATA_ROOT/<REGION>/` (e.g. `/data/europe/iceland/`). S3 uploads go to `s3://$S3_BUCKET/<REGION>/<YYYY-MM>/`.

**Requirements:** Same as the single-region pipeline: `/db`, `/data` (or `DATA_ROOT`), `/boundaries`, and the pipeline image. Run from repo root (e.g. in Docker or on a VM with the pipeline script and dependencies).

**Usage:**

```bash
# All regions (world)
./scripts/run_world_pipeline_aws.sh

# One continent
./scripts/run_world_pipeline_aws.sh --continent europe
./scripts/run_world_pipeline_aws.sh --continent north-america

# One region (continent + slug)
./scripts/run_world_pipeline_aws.sh --continent europe --slug iceland
./scripts/run_world_pipeline_aws.sh --continent north-america --slug us/colorado
```

**Environment:**

- `DATA_ROOT` — Base output directory (default `/data`). Each region writes to `$DATA_ROOT/<REGION>/`.
- `S3_BUCKET` — If set, each region’s output is synced to `s3://$S3_BUCKET/<REGION>/<YYYY-MM>/`.
- `DB`, `BOUNDARIES` — Same as `run_iceland_pipeline_aws.sh`.

The pipeline already enriches features with **Country** and **State** (admin 0/1) from Natural Earth in `enrich_geojson_properties.py` and `analyze_ski_areas.py`; the world runner adds **continent/country/state** organization via the `REGION` path and S3 prefix.

### Run locally overnight (Windows)

1. **Build the Docker image** (once):
   ```powershell
   docker build -f Dockerfile.aws -t globalskiatlas-pipeline .
   ```

2. **Keep the computer awake** and run all regions:
   ```powershell
   cd c:\Users\jwitc\Documents\GitHub\globalskiatlas_data
   .\scripts\run_all_regions_local.ps1 -PreventSleep
   ```
   `-PreventSleep` uses the Windows API to prevent sleep while the script runs (display may still turn off).

   **Without the flag**, prevent sleep manually:
   - **Settings** → **System** → **Power** → set **Screen and sleep** to **Never** (plugged in), or
   - Run in an elevated PowerShell: `powercfg /change standby-timeout-ac 0` (restore later with `powercfg /change standby-timeout-ac 30` for 30 minutes).

3. **Optional:** run only some continents to test or resume:
   ```powershell
   .\scripts\run_all_regions_local.ps1 -PreventSleep -Continents europe,asia
   ```

4. When all continents finish, the script runs `combine_regions.py`; combined outputs go to `output/combined/`.

---

## PBF Size and Runtimes

- **< 200 MB**: Typically finishes in minutes (e.g. Iceland ~60 MB, Liechtenstein, many sub-regions).
- **200–600 MB**: 10–30+ min (depends on ski area count and clustering).
- **> 600 MB**: High OOM risk without sub-regions or `cluster_dist_m`; use smaller extracts or increase cluster fragmentation.

See `docs/RUN_TIME_AND_NA_ESTIMATE.md` for calibration.

---

## Using the output elsewhere

Combined GeoParquet and exported GeoJSON can be used in **ArcGIS Pro**, **ArcGIS Online**, or **Experience Builder** for visualization and editing workflows.
