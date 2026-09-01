#!/usr/bin/env python3
"""Upload empty FeatureCollection stubs for vector layers missing on S3.

Prevents CloudFront/S3 403 when the manifest advertises a layer the bake skipped.
"""
from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError

BUCKET = "globalskiatlas-backend-k8s-output"
PREFIX = "game_scenes/"
OPTIONAL = ("forest", "cliffs", "grassland", "barriers")
WORKERS = 16


def empty_fc(layer: str) -> bytes:
    return json.dumps(
        {"type": "FeatureCollection", "layer": layer, "features": []},
        separators=(",", ":"),
    ).encode("utf-8")


def list_scene_prefixes(s3) -> list[str]:
    """Return game_scenes/<resort>/<ver>/ prefixes that have a scene-manifest."""
    out = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX, Delimiter="/"):
        for common in page.get("CommonPrefixes") or []:
            resort = common["Prefix"]
            for vp in paginator.paginate(Bucket=BUCKET, Prefix=resort, Delimiter="/"):
                for ver in vp.get("CommonPrefixes") or []:
                    out.append(ver["Prefix"])
    return out


def ensure_stub(s3, scene_prefix: str, layer: str) -> str:
    key = f"{scene_prefix}vectors/{layer}.geojson"
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return "exists"
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        # This bucket returns 403 (not 404) for missing keys.
        if code not in {"404", "403", "NoSuchKey", "NotFound", "AccessDenied"}:
            raise
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=empty_fc(layer),
        ContentType="application/geo+json",
        CacheControl="public, max-age=300",
    )
    return "wrote"


def main() -> int:
    s3 = boto3.client("s3")
    scenes = list_scene_prefixes(s3)
    print(f"scenes={len(scenes)} layers={OPTIONAL}", flush=True)
    wrote = exists = errors = 0
    jobs = [(sp, layer) for sp in scenes for layer in OPTIONAL]
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = {pool.submit(ensure_stub, s3, sp, layer): (sp, layer) for sp, layer in jobs}
        for i, fut in enumerate(as_completed(futs), 1):
            sp, layer = futs[fut]
            try:
                r = fut.result()
                if r == "wrote":
                    wrote += 1
                else:
                    exists += 1
            except Exception as e:
                errors += 1
                print(f"ERR {sp}vectors/{layer}.geojson: {e}", flush=True)
            if i % 200 == 0 or i == len(futs):
                print(f"progress {i}/{len(futs)} wrote={wrote} exists={exists} err={errors}", flush=True)
    print(f"done wrote={wrote} exists={exists} err={errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
