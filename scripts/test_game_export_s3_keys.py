#!/usr/bin/env python3
"""S3 key-order helpers for game_export (no AWS calls)."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from game_export.s3_inputs import dem_key_candidates, parquet_key_candidates


def test_parquet_regional_before_combined():
    keys = parquet_key_candidates(
        "europe/slovenia", "ski_areas.parquet", allow_combined=True
    )
    assert keys == [
        "europe/slovenia/ski_areas.parquet",
        "combined/ski_areas.parquet",
    ]
    regional_only = parquet_key_candidates(
        "europe/slovenia", "pistes.parquet", allow_combined=False
    )
    assert regional_only == ["europe/slovenia/pistes.parquet"]
    assert "combined/pistes.parquet" not in regional_only


def test_dem_nested_then_flat_then_combined():
    keys = dem_key_candidates("north-america/us/pennsylvania", "45096232")
    assert keys == [
        "north-america/us/pennsylvania/dems/north-america/us/pennsylvania/45096232.tif",
        "north-america/us/pennsylvania/dems/45096232.tif",
        "combined/dems/north-america/us/pennsylvania/45096232.tif",
        "combined/dems/45096232.tif",
    ]


def test_ensure_s3_object_cache_hit(tmp_path: Path | None = None):
    from game_export.s3_inputs import ensure_s3_object, write_cached_etag

    class FakeS3:
        def __init__(self):
            self.downloads = 0

        def head_object(self, Bucket, Key):
            return {"ETag": '"abc123"', "ContentLength": 4}

        def download_file(self, bucket, key, dest):
            self.downloads += 1
            Path(dest).write_bytes(b"data")

    s3 = FakeS3()
    dest = (tmp_path or Path(".")) / "ski_areas.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"data")
    write_cached_etag(dest, "abc123")
    hit = ensure_s3_object(s3, "bucket", "europe/slovenia/ski_areas.parquet", dest)
    assert hit == dest
    assert s3.downloads == 0


def main() -> int:
    import tempfile

    test_parquet_regional_before_combined()
    test_dem_nested_then_flat_then_combined()
    with tempfile.TemporaryDirectory() as td:
        test_ensure_s3_object_cache_hit(Path(td))
    print("S3 KEY TESTS OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
