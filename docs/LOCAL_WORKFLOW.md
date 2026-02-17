# Local Pipeline Workflow (No AWS)

Run the ski atlas pipeline fully locally with Docker. Output goes to region-specific folders, then combine into a global dataset.

## Regions

| Region            | Compose file                         | PBF size | ~Time   |
|-------------------|--------------------------------------|----------|---------|
| North America     | docker-compose.north-america.yml     | ~16 GB   | 4–7 hr  |
| South America     | docker-compose.south-america.yml     | ~3.6 GB  | ~30 min |
| Africa            | docker-compose.africa.yml            | ~7 GB    | ~60 min |
| Europe            | docker-compose.europe.yml            | ~25 GB   | 6–10 hr |
| Australia/Oceania | docker-compose.australia-oceania.yml | ~400 MB  | ~15 min |
| Asia              | docker-compose.asia.yml              | ~8 GB    | ~90 min |

## Quick Start

```powershell
# Run one region
docker compose -f docker-compose.south-america.yml up --build

# Run all regions, then combine
.\run_all_regions.ps1

# Run specific regions only
.\run_all_regions.ps1 -Regions south-america,africa,australia-oceania
```

## Output Structure

```
output/
  north-america/
  south-america/
  africa/
  europe/
  australia-oceania/
  asia/
    ski_areas.parquet
    ski_areas_analyzed.parquet
    lifts.parquet
    pistes.parquet
    osm_near_winter_sports.parquet
  combined/          # After running combine script
    ...              # All regions merged, with 'region' column
```

## Combine Regions

After running one or more regions:

```powershell
python scripts/combine_regions.py
```

Or specify regions:

```powershell
python scripts/combine_regions.py -o output -r north-america south-america africa europe australia-oceania asia
```

## Prerequisites

- Docker Desktop
- Python 3.11+ with geopandas, pandas, pyarrow (for combine script)
