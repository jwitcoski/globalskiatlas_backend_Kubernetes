"""Patching one winter_sports_id drops the old row and keeps the new one."""
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from run_one_resort import (
    _maplark_adapt_query,
    _osm_host_label,
    replace_resort_rows,
    wiki_page_id_from_analyzed,
    wiki_stat_attributes,
    wiki_stat_update,
)


def test_replace_one_id():
    existing = pd.DataFrame({"winter_sports_id": ["1", "45096232"], "name": ["Other", "Old"]})
    incoming = pd.DataFrame({"winter_sports_id": ["45096232"], "name": ["Montage"]})
    out = replace_resort_rows(existing, incoming, "45096232")
    assert list(out["name"]) == ["Other", "Montage"]
    gone = replace_resort_rows(existing, None, "45096232")
    assert list(gone["winter_sports_id"]) == ["1"]


def test_maplark_adapt_query():
    os.environ["MAPLARK_MAX_AROUND_M"] = "22000"
    q = (
        '[out:json];(way["landuse"="winter_sports"]["name"~"Aspen",i]'
        "(around:40000,39.18,-106.82););out ids;"
    )
    adapted = _maplark_adapt_query(q)
    assert ',i' not in adapted
    assert "(around:22000,39.18,-106.82)" in adapted
    assert '["name"~"Aspen"]' in adapted


def test_wiki_stats_leave_the_article_alone():
    row = {
        "name": "Montage Mountain",
        "english_name": "Montage",
        "state": "Pennsylvania",
        "country": "United States of America",
        "winter_sports_id": "45096232",
        "downhill_trails": 26,
        "total_lifts": 7,
        "elevation_high_m": 500,
        "elevation_low_m": 200,
        "centroid_lat": 41.3,
        "centroid_lon": -75.6,
        "website": "",
    }
    assert wiki_page_id_from_analyzed(row) == "montage-mountain-pennsylvania"
    attrs = wiki_stat_attributes(row)
    assert attrs["downhillTrails"] == {"S": "26"}
    assert attrs["totalLifts"] == {"S": "7"}
    assert attrs["verticalDropM"] == {"N": "300"}
    assert attrs["highElevationM"] == {"N": "500"}
    assert "content" not in attrs
    assert "website" not in attrs
    request = wiki_stat_update("montage-mountain-pennsylvania", row, "2026-11-22T00:00:00+00:00")
    assert "content = if_not_exists(content, :stub)" in request["UpdateExpression"]
    assert request["Key"] == {"pageId": {"S": "montage-mountain-pennsylvania"}}
    with_essay = wiki_stat_update(
        "montage-mountain-pennsylvania", row, "2026-11-22T00:00:00+00:00", content="# Montage\n\nA hill."
    )
    assert "content = :essay" in with_essay["UpdateExpression"]
    assert with_essay["ExpressionAttributeValues"][":essay"] == {"S": "# Montage\n\nA hill."}


def test_osm_host_label():
    assert _osm_host_label("https://overpass-api.de/api/interpreter") == "Overpass (de)"
    assert _osm_host_label("https://overpass.kumi.systems/api/interpreter") == "Overpass (kumi)"
    assert _osm_host_label("https://overpass.openstreetmap.fr/api/interpreter") == "Overpass (fr)"
    assert _osm_host_label("https://api.maplark.com/api/interpreter") == "MapLark"


if __name__ == "__main__":
    test_replace_one_id()
    test_maplark_adapt_query()
    test_wiki_stats_leave_the_article_alone()
    test_osm_host_label()
    print("check ok")
