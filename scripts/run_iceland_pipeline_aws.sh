#!/bin/sh
# Full Iceland pipeline for AWS: download, extract, enrich, analyze, parquet, upload to S3.
# Expects: /db (PBF), /data (output), /boundaries (Natural Earth shapefiles)
# Env: S3_BUCKET, REGION (e.g. iceland), AWS_REGION (optional)

set -e

DB=${DB:-/db}
DATA=${DATA:-/data}
BOUNDARIES=${BOUNDARIES:-/boundaries}
PBF_URL=${PBF_URL:-https://download.geofabrik.de/europe/iceland-latest.osm.pbf}
REGION=${REGION:-iceland}

echo "=== Ski Atlas Iceland (AWS) ==="
echo "PBF: $PBF_URL | Output: $DATA | S3: s3://${S3_BUCKET:-}${S3_BUCKET:+/}$REGION/"

mkdir -p "$DB" "$DATA"

# 1. Download PBF
echo "[1/8] Downloading PBF..."
wget -q --progress=bar:force -O "$DB/planet.osm.pbf" "$PBF_URL"
echo "Download complete."

# 2. Extract winter_sports to ski_areas.geojson
echo "[2/8] Extracting winter_sports..."
python scripts/pbf_to_geojson.py "$DB/planet.osm.pbf" "$DATA/ski_areas.geojson"

# 3. Extract OSM data near ski areas
echo "[3/8] Extracting OSM nearby..."
python scripts/extract_nearby_from_pbf.py "$DB/planet.osm.pbf" "$DATA/ski_areas.geojson" -o "$DATA/osm_near_winter_sports.json"

# 4. Extract lifts and pistes
echo "[4/8] Extracting lifts and pistes..."
python scripts/extract_lifts_and_pistes_from_pbf.py "$DB/planet.osm.pbf" -o "$DATA"

# 5. Enrich GeoJSON (State, Country, Ski Area)
echo "[5/8] Enriching GeoJSON..."
python scripts/enrich_geojson_properties.py all -d "$DATA" -b "$BOUNDARIES"

# 6. Analyze ski areas
echo "[6/8] Analyzing ski areas..."
python analyze_ski_areas.py "$DATA/ski_areas.geojson" "$DATA/osm_near_winter_sports.json" -o "$DATA/ski_areas_analyzed.csv" -b "$BOUNDARIES"

# 7. Export to Parquet, remove large GeoJSON/JSON
echo "[7/8] Exporting to Parquet..."
python convert_to_geoparquet.py osm -i "$DATA/osm_near_winter_sports.json" -o "$DATA/osm_near_winter_sports.parquet"
python convert_to_geoparquet.py all -d "$DATA"
rm -f "$DATA/lifts.geojson" "$DATA/osm_near_winter_sports.json" "$DATA/pistes.geojson" "$DATA/ski_areas.geojson"
echo "Parquet export complete."

# 8. Upload to S3
if [ -n "$S3_BUCKET" ]; then
  echo "[8/8] Uploading to S3..."
  MONTH=$(date +%Y-%m)
  S3_PREFIX="s3://$S3_BUCKET/$REGION/$MONTH/"
  aws s3 sync "$DATA" "$S3_PREFIX" \
    --exclude "*" \
    --include "*.parquet" \
    --include "*.csv" \
    --no-progress
  echo "Uploaded to $S3_PREFIX"
else
  echo "[8/8] S3_BUCKET not set, skipping upload."
fi

echo "=== Done ==="
