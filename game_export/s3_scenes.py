"""Upload game-scene cakes and merge the public S3 catalog (not the GIS pantry)."""
from __future__ import annotations

import json
import logging
import mimetypes
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from game_export.catalog import CATALOG_DISCLAIMER, merge_catalog_resorts
from game_export.s3_inputs import DEFAULT_AWS_REGION, default_s3_bucket, make_s3_client

log = logging.getLogger("game_export")

PREFIX = "game_scenes"
PROGRESS_KEY = f"{PREFIX}/_progress.json"
CATALOG_KEY = f"{PREFIX}/catalog.json"
EXTRA_TYPES = {
    ".js": "text/javascript; charset=utf-8",
    ".mjs": "text/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".geojson": "application/geo+json",
    ".bin": "application/octet-stream",
    ".glb": "model/gltf-binary",
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".md": "text/markdown; charset=utf-8",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".svg": "image/svg+xml",
    ".woff2": "font/woff2",
}


def content_type(path: Path) -> str:
    extra = EXTRA_TYPES.get(path.suffix.lower())
    if extra:
        return extra
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def skip_relative(rel: Path) -> bool:
    return "qa" in rel.parts


def iter_scene_files(root: Path):
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if skip_relative(rel):
            continue
        yield p, rel


def public_object_url(bucket: str, key: str, region: str = DEFAULT_AWS_REGION) -> str:
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"


def public_head_ok(url: str, timeout: int = 30) -> bool:
    try:
        req = Request(url, method="HEAD")
        with urlopen(req, timeout=timeout) as resp:
            return 200 <= getattr(resp, "status", 200) < 300
    except (HTTPError, URLError, TimeoutError, OSError):
        return False


def empty_progress() -> dict[str, Any]:
    return {"done": [], "failed": [], "skipped": []}


def _norm_id_list(values) -> list[str]:
    out = []
    seen = set()
    for v in values or []:
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def normalize_progress(raw: Optional[dict]) -> dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    return {
        "done": _norm_id_list(data.get("done")),
        "failed": _norm_id_list(data.get("failed")),
        "skipped": _norm_id_list(data.get("skipped")),
    }


def download_json(s3, bucket: str, key: str) -> Optional[dict]:
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return json.loads(body.decode("utf-8"))


def upload_json(s3, bucket: str, key: str, payload: dict, *, cache_seconds: int = 60) -> None:
    body = json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
        CacheControl=f"public, max-age={cache_seconds}",
    )


def upload_scene_tree(
    s3,
    bucket: str,
    scene_dir: Path,
    *,
    prefix: str = PREFIX,
    dry_run: bool = False,
) -> str:
    rel = f"{scene_dir.parent.name}/{scene_dir.name}"
    files = list(iter_scene_files(scene_dir))
    log.info("Upload %s files for %s", len(files), rel)
    if dry_run:
        return f"{prefix}/{rel}"
    for i, (f, frel) in enumerate(files, start=1):
        key = f"{prefix}/{rel}/{frel.as_posix()}"
        extra = {"CacheControl": "public, max-age=300"}
        if f.name in {"index.html", "catalog.json"} or f.suffix.lower() in {".js", ".html"}:
            extra["CacheControl"] = "public, max-age=60"
        s3.upload_file(
            str(f),
            bucket,
            key,
            ExtraArgs={"ContentType": content_type(f), **extra},
        )
        if i == 1 or i % 40 == 0 or i == len(files):
            log.info("  %s/%s %s", i, len(files), key)
    return f"{prefix}/{rel}"


def merge_and_upload_catalog(
    s3,
    bucket: str,
    incoming: list[dict],
    *,
    prefix: str = PREFIX,
    dry_run: bool = False,
) -> dict:
    key = f"{prefix}/catalog.json"
    existing = download_json(s3, bucket, key) or {}
    resorts = merge_catalog_resorts(existing.get("resorts") or [], incoming)
    payload = {
        "disclaimer": existing.get("disclaimer") or CATALOG_DISCLAIMER,
        "resorts": resorts,
    }
    log.info("S3 catalog %s resorts (was %s)", len(resorts), len(existing.get("resorts") or []))
    if not dry_run:
        upload_json(s3, bucket, key, payload, cache_seconds=60)
    return payload


def load_progress(s3, bucket: str) -> dict[str, Any]:
    return normalize_progress(download_json(s3, bucket, PROGRESS_KEY))


def save_progress(s3, bucket: str, progress: dict, *, dry_run: bool = False) -> dict:
    payload = normalize_progress(progress)
    if not dry_run:
        upload_json(s3, bucket, PROGRESS_KEY, payload, cache_seconds=60)
    return payload


def make_scenes_client():
    return make_s3_client()


def bucket_name(override: Optional[str] = None) -> str:
    return override or default_s3_bucket()
