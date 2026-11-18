"""Patching one winter_sports_id drops the old row and keeps the new one."""
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from run_one_resort import _maplark_adapt_query, replace_resort_rows


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


if __name__ == "__main__":
    test_replace_one_id()
    test_maplark_adapt_query()
    print("check ok")
