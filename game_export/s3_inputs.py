"""Download pipeline GIS objects from S3 into a local cache. Never writes back to S3."""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger("game_export")

DEFAULT_BUCKET = "globalskiatlas-backend-k8s-output"
DEFAULT_AWS_REGION = "us-east-1"
ETAG_SUFFIX = ".s3etag"

_IAM_HINT = (
    "Need IAM s3:GetObject on this key. Regional prefixes on "
    "globalskiatlas-backend-k8s-output are not public (only combined/* is)."
)


def default_s3_bucket() -> str:
    return (
        os.environ.get("GAME_EXPORT_S3_BUCKET")
        or os.environ.get("S3_BUCKET")
        or DEFAULT_BUCKET
    )


def parquet_key_candidates(region: str, name: str, *, allow_combined: bool) -> list[str]:
    """Regional object first; combined only when allow_combined (ski_areas polygons)."""
    region = region.strip("/")
    keys = [f"{region}/{name}"]
    if allow_combined:
        keys.append(f"combined/{name}")
    return keys


def dem_key_candidates(region: str, winter_sports_id: str) -> list[str]:
    """Match elevation writer + s3 sync layout, then combined fallbacks."""
    region = region.strip("/")
    wid = winter_sports_id
    return [
        f"{region}/dems/{region}/{wid}.tif",
        f"{region}/dems/{wid}.tif",
        f"combined/dems/{region}/{wid}.tif",
        f"combined/dems/{wid}.tif",
    ]


def etag_sidecar(path: Path) -> Path:
    return path.with_name(path.name + ETAG_SUFFIX)


def read_cached_etag(path: Path) -> Optional[str]:
    side = etag_sidecar(path)
    if not path.is_file() or not side.is_file():
        return None
    return side.read_text(encoding="utf-8").strip() or None


def write_cached_etag(path: Path, etag: str) -> None:
    etag_sidecar(path).write_text(etag.strip(), encoding="utf-8")


def normalize_etag(raw: str) -> str:
    return (raw or "").strip().strip('"')


def cache_path_for_key(cache_dir: Path, key: str) -> Path:
    return cache_dir / "s3_game_export" / key.replace("\\", "/")


def make_s3_client(region_name: Optional[str] = None):
    import boto3

    return boto3.client(
        "s3",
        region_name=region_name or os.environ.get("AWS_REGION") or DEFAULT_AWS_REGION,
    )


def _is_missing(exc: BaseException) -> bool:
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    return code in {"404", "NoSuchKey", "NotFound"}


def _is_forbidden(exc: BaseException) -> bool:
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    return code in {"403", "AccessDenied"}


def head_s3_object(s3, bucket: str, key: str) -> Optional[dict]:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if _is_missing(exc):
            return None
        if _is_forbidden(exc):
            raise PermissionError(
                f"s3://{bucket}/{key}: access denied. {_IAM_HINT}"
            ) from exc
        raise


def ensure_s3_object(s3, bucket: str, key: str, dest: Path) -> Optional[Path]:
    """Return dest if the object exists (download or valid cache). None if missing."""
    meta = head_s3_object(s3, bucket, key)
    if meta is None:
        return None
    etag = normalize_etag(str(meta.get("ETag") or ""))
    dest.parent.mkdir(parents=True, exist_ok=True)
    cached = read_cached_etag(dest)
    size = int(meta.get("ContentLength") or 0)
    if dest.is_file() and cached == etag and (size == 0 or dest.stat().st_size == size):
        log.info("S3 cache hit %s <- s3://%s/%s", dest, bucket, key)
        return dest
    log.info("Downloading s3://%s/%s -> %s", bucket, key, dest)
    try:
        s3.download_file(bucket, key, str(dest))
    except Exception as exc:
        if _is_forbidden(exc):
            raise PermissionError(
                f"s3://{bucket}/{key}: access denied. {_IAM_HINT}"
            ) from exc
        if _is_missing(exc):
            return None
        raise
    if etag:
        write_cached_etag(dest, etag)
    return dest


def fetch_first_s3_object(
    s3,
    bucket: str,
    keys: list[str],
    cache_dir: Path,
) -> Optional[Path]:
    last_perm: Optional[PermissionError] = None
    for key in keys:
        dest = cache_path_for_key(cache_dir, key)
        try:
            hit = ensure_s3_object(s3, bucket, key, dest)
        except PermissionError as exc:
            last_perm = exc
            log.warning("%s", exc)
            continue
        if hit is not None:
            return hit
    if last_perm is not None:
        raise last_perm
    return None
