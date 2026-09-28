"""Patching one winter_sports_id drops the old row and keeps the new one."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from run_one_resort import replace_resort_rows


def test_replace_one_id():
    existing = pd.DataFrame({"winter_sports_id": ["1", "45096232"], "name": ["Other", "Old"]})
    incoming = pd.DataFrame({"winter_sports_id": ["45096232"], "name": ["Montage"]})
    out = replace_resort_rows(existing, incoming, "45096232")
    assert list(out["name"]) == ["Other", "Montage"]
    gone = replace_resort_rows(existing, None, "45096232")
    assert list(gone["winter_sports_id"]) == ["1"]


if __name__ == "__main__":
    test_replace_one_id()
    print("check ok")
