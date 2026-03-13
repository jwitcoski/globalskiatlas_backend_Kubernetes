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

# 1. Download PBF (retry once if verification fails)
echo "[1/11] Downloading PBF..."
wget -q --progress=bar:force -O "$DB/planet.osm.pbf" "$PBF_URL"
for attempt in 1 2; do
  if [ -s "$DB/planet.osm.pbf" ] && ogrinfo -al -so "$DB/planet.osm.pbf" >/dev/null 2>&1; then
    echo "Download complete."
    break
  fi
  if [ "$attempt" -eq 1 ]; then
    echo "Verification failed; retrying download..."
    wget -q --progress=bar:force -O "$DB/planet.osm.pbf" "$PBF_URL"
  else
    echo "Error: PBF download failed verification (corrupt or unreadable)." >&2
    exit 1
  fi
done

# 2. Extract winter_sports to ski_areas.geojson
echo "[2/11] Extracting winter_sports..."
python scripts/pbf_to_geojson.py "$DB/planet.osm.pbf" "$DATA/ski_areas.geojson"

# 3. Extract OSM data near ski areas
# Smaller OSM_NEARBY_CLUSTER_DIST_M = more clusters, less memory (e.g. 100000 for Japan)
echo "[3/11] Extracting OSM nearby..."
python scripts/extract_nearby_from_pbf.py "$DB/planet.osm.pbf" "$DATA/ski_areas.geojson" -o "$DATA/osm_near_winter_sports.json" \
  ${OSM_NEARBY_CLUSTER_DIST_M:+--cluster-dist "$OSM_NEARBY_CLUSTER_DIST_M"}

# 4. Extract lifts and pistes
echo "[4/11] Extracting lifts and pistes..."
python scripts/extract_lifts_and_pistes_from_pbf.py "$DB/planet.osm.pbf" -o "$DATA"

# 5. Enrich GeoJSON (State, Country, Ski Area)
echo "[5/11] Enriching GeoJSON..."
python scripts/enrich_geojson_properties.py all -d "$DATA" -b "$BOUNDARIES"

# 6. Analyze ski areas
echo "[6/11] Analyzing ski areas..."
python analyze_ski_areas.py "$DATA/ski_areas.geojson" "$DATA/osm_near_winter_sports.json" -o "$DATA/ski_areas_analyzed.csv" -b "$BOUNDARIES" -r "$REGION"

# 7. Export to Parquet, remove large GeoJSON/JSON
echo "[7/11] Exporting to Parquet..."
python convert_to_geoparquet.py osm -i "$DATA/osm_near_winter_sports.json" -o "$DATA/osm_near_winter_sports.parquet"
python convert_to_geoparquet.py all -d "$DATA"
rm -f "$DATA/lifts.geojson" "$DATA/osm_near_winter_sports.json" "$DATA/pistes.geojson" "$DATA/ski_areas.geojson"
echo "Parquet export complete."

# 8. 1000 ft buffer outline for production maps (same radius as OSM nearby / contours)
echo "[8/11] 1000 ft buffer outline..."
python scripts/ski_area_1000ft_buffer.py -d "$DATA"

# 9. Translate resort names to English (fills english_name; uses cache in $DATA/cache if present)
# Requires: googletrans in the image (pip install googletrans==4.0.2 or in requirements.txt for Docker build).
echo "[9/11] Translating resort names..."
python scripts/translate_resort_names.py -i "$DATA/ski_areas_analyzed.parquet" -o "$DATA/ski_areas_analyzed.parquet" --cache "$DATA/cache/name_translations.json"

# 10. Elevation and contours (Skadi DEM; merges elevation + ski_north_angle into ski_areas_analyzed.parquet)
# Requires: ski_areas.parquet with polygon geometry; geopandas, rasterio, matplotlib in the image.
echo "[10/11] Elevation and contours..."
python scripts/ski_area_elevation_contours.py -i "$DATA/ski_areas.parquet" -o "$DATA" --cache-dir "$DATA/cache/skadi"
# Re-export CSV from parquet so ski_areas_analyzed.csv includes elevation_low_m, elevation_high_m, ski_north_angle
python -c "import pandas as pd; pd.read_parquet('$DATA/ski_areas_analyzed.parquet').to_csv('$DATA/ski_areas_analyzed.csv', index=False)"
echo "Updated ski_areas_analyzed.csv with elevation."

# 11. Upload to S3
if [ -n "$S3_BUCKET" ]; then
  echo "[11/11] Uploading to S3..."
  MONTH=$(date +%Y-%m)
  S3_PREFIX="s3://$S3_BUCKET/$REGION/$MONTH/"
  aws s3 sync "$DATA" "$S3_PREFIX" \
    --exclude "*" \
    --include "*.parquet" \
    --include "*.geojson" \
    --include "*.csv" \
    --no-progress
  echo "Uploaded to $S3_PREFIX"
else
  echo "[11/11] S3_BUCKET not set, skipping upload."
fi

echo "=== Done ==="
