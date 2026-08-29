#!/usr/bin/env python3
"""Unit tests for candidate configs and S3 catalog merge (no AWS)."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from game_export.catalog import merge_catalog_resorts
from game_export.config import config_from_candidate, slugify_resort, unique_resort_id


def test_slug_and_collision():
    a = slugify_resort("Vogel", "Slovenia")
    assert a == "vogel_slovenia"
    used = {a}
    assert unique_resort_id(a, "1", used) == "vogel_slovenia_1"


def test_config_from_candidate_defaults():
    cfg = config_from_candidate(
        {
            "winter_sports_id": "608654682",
            "region": "africa",
            "name": "AfriSki",
            "state": "Butha-Buthe",
            "country": "Lesotho",
        }
    )
    assert cfg.resort_id == "afriski_lesotho"
    assert cfg.winter_sports_id == "608654682"
    assert cfg.region == "africa"
    assert cfg.heightfield_resolution_m == 2
    assert "motorway" in cfg.highway_hazard_types


def test_merge_catalog_keeps_existing():
    existing = [{"id": "montage_mountain_pa", "path": "old/v0"}]
    incoming = [
        {"id": "montage_mountain_pa", "path": "new/v1", "winter_sports_id": "45096232"},
        {"id": "afriski_lesotho", "path": "afriski_lesotho/v0"},
    ]
    merged = merge_catalog_resorts(existing, incoming)
    by_id = {r["id"]: r for r in merged}
    assert by_id["montage_mountain_pa"]["path"] == "new/v1"
    assert "afriski_lesotho" in by_id
    assert len(merged) == 2


def main() -> int:
    test_slug_and_collision()
    test_config_from_candidate_defaults()
    test_merge_catalog_keeps_existing()
    print("CANDIDATE / CATALOG MERGE TESTS OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
