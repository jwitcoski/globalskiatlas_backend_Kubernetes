#!/bin/sh
# Upload staged clay scenes from deploy_clay/ using the ECS task role.
set -eu
BUCKET="${S3_BUCKET:-globalskiatlas-backend-k8s-output}"
PREFIX="${CLAY_PREFIX:-clay_scenes}"
ROOT="${DEPLOY_CLAY_ROOT:-/app/deploy_clay}"

if [ ! -d "$ROOT" ]; then
  echo "Missing $ROOT"
  exit 1
fi

uploaded=0
for scene in "$ROOT"/*/; do
  [ -d "$scene" ] || continue
  rid=$(basename "$scene")
  echo "Uploading $rid -> s3://$BUCKET/$PREFIX/$rid/"
  find "$scene" -type f | while read -r f; do
    rel=${f#"$scene"}
    key="$PREFIX/$rid/$rel"
    case "$f" in
      *.json) ct="application/json; charset=utf-8" ;;
      *.geojson) ct="application/geo+json" ;;
      *.glb) ct="model/gltf-binary" ;;
      *.md) ct="text/markdown; charset=utf-8" ;;
      *) ct="application/octet-stream" ;;
    esac
    echo "  put $key"
    aws s3api put-object \
      --bucket "$BUCKET" \
      --key "$key" \
      --body "$f" \
      --content-type "$ct" \
      --cache-control "public, max-age=300" \
      --server-side-encryption AES256 \
      >/dev/null
  done
  uploaded=$((uploaded + 1))
done

if [ -f "$ROOT/catalog.json" ]; then
  echo "Uploading catalog.json"
  aws s3api put-object \
    --bucket "$BUCKET" \
    --key "$PREFIX/catalog.json" \
    --body "$ROOT/catalog.json" \
    --content-type "application/json; charset=utf-8" \
    --cache-control "public, max-age=60" \
    --server-side-encryption AES256 \
    >/dev/null
fi

echo "Uploaded $uploaded scene(s) to s3://$BUCKET/$PREFIX/"
test "$uploaded" -gt 0
