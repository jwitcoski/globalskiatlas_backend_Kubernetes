#!/bin/sh
# Run the full ski atlas pipeline for the world (or a continent/slug), one region per PBF.
# Output and S3 are organized by continent/country/state (e.g. europe/iceland, north-america/us/colorado).
# Uses config/regions.yaml via scripts/list_regions_for_pipeline.py.
#
# Expects: same as run_iceland_pipeline_aws.sh (PBF download, /data, /boundaries, optional S3_BUCKET).
# Env: DATA_ROOT (base output dir; default /data), REGION filter optional.
#
# Usage:
#   ./scripts/run_world_pipeline_aws.sh
#   ./scripts/run_world_pipeline_aws.sh --continent europe
#   ./scripts/run_world_pipeline_aws.sh --continent north-america --slug us/colorado

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PIPELINE_SCRIPT="$SCRIPT_DIR/run_iceland_pipeline_aws.sh"
LIST_SCRIPT="$SCRIPT_DIR/list_regions_for_pipeline.py"

DATA_ROOT="${DATA_ROOT:-/data}"
DB="${DB:-/db}"
BOUNDARIES="${BOUNDARIES:-/boundaries}"

# Optional filters (passed to list_regions_for_pipeline.py)
CONTINENT=""
SLUG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --continent) CONTINENT="$2"; shift 2 ;;
    --slug)      SLUG="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [ ! -f "$PIPELINE_SCRIPT" ]; then
  echo "Pipeline script not found: $PIPELINE_SCRIPT" >&2
  exit 1
fi
if [ ! -f "$LIST_SCRIPT" ]; then
  echo "List script not found: $LIST_SCRIPT" >&2
  exit 1
fi

# Build list of regions: REGION\tPBF_URL\tCLUSTER_DIST_M
LIST_ARGS=""
[ -n "$CONTINENT" ] && LIST_ARGS="$LIST_ARGS --continent $CONTINENT"
[ -n "$SLUG" ]      && LIST_ARGS="$LIST_ARGS --slug $SLUG"
ROWS=$(python "$LIST_SCRIPT" $LIST_ARGS) || { echo "Failed to list regions." >&2; exit 1; }

if [ -z "$ROWS" ]; then
  echo "No regions matched (continent=$CONTINENT slug=$SLUG)." >&2
  exit 1
fi

COUNT=$(echo "$ROWS" | wc -l)
echo "=== World pipeline: $COUNT region(s) (continent/country/state) ==="
echo "DATA_ROOT=$DATA_ROOT | S3_BUCKET=${S3_BUCKET:-<unset>}"
echo ""

N=0
echo "$ROWS" | while IFS="$(printf '\t')" read -r REGION PBF_URL CLUSTER_DIST_M; do
  [ -z "$REGION" ] && continue
  N=$((N + 1))
  echo "--- [$N/$COUNT] $REGION ---"
  export PBF_URL
  export REGION
  export DB
  export BOUNDARIES
  export DATA="${DATA_ROOT}/${REGION}"
  if [ -n "$CLUSTER_DIST_M" ]; then
    export OSM_NEARBY_CLUSTER_DIST_M="$CLUSTER_DIST_M"
  else
    unset OSM_NEARBY_CLUSTER_DIST_M
  fi
  mkdir -p "$DATA" "$DB"
  if (cd "$REPO_ROOT" && "$PIPELINE_SCRIPT"); then
    :
  else
    echo "Pipeline failed for region: $REGION" >&2
    exit 1
  fi
done
echo "=== Done ($COUNT region(s)) ==="
