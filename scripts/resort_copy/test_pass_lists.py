"""Pass roster harvest and name matching. python scripts/resort_copy/test_pass_lists.py"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.pass_chips import PassChip  # noqa: E402
from resort_copy.pass_lists import (  # noqa: E402
    atlas_index,
    extract_listed_resorts,
    harvest_pass_list,
    match_listed,
    merge_rosters,
    write_rosters,
)


IKON_HTML = """
<html><body>
<a href="/en/destinations/killington-resort">Killington</a>
<a href="/en/destinations/palisades-tahoe">Palisades Tahoe</a>
<a href="/en/pass/buy">Buy a pass</a>
<table><tr><td>Destination</td><td>Opening Date</td></tr>
<tr><td>Killington</td><td>OPEN</td></tr>
<tr><td>Alta Ski Area</td><td>Nov 20</td></tr>
</table>
</body></html>
"""


class PassListTests(unittest.TestCase):
    def test_extracts_destination_links_not_buy_pages(self) -> None:
        rows = extract_listed_resorts(IKON_HTML, "https://www.ikonpass.com/en/destinations")
        names = {row["listed_name"] for row in rows}
        self.assertIn("Killington", names)
        self.assertIn("Palisades Tahoe", names)
        self.assertIn("Alta Ski Area", names)
        self.assertNotIn("OPEN", names)
        js_rows = extract_listed_resorts(
            'script=["killington-resort","palisades-tahoe","alta-ski-area","big-sky-resort","jackson-hole","mammoth-mountain","steamboat","winter-park"]',
            "https://www.ikonpass.com/en/destinations",
        )
        self.assertTrue(any("Killington" in row["listed_name"] for row in js_rows))
        self.assertTrue(any("killington-resort" in row["listed_url"] for row in rows))
        self.assertFalse(any("buy" in row["listed_url"] for row in rows))

    def test_killington_matches_killington_resort(self) -> None:
        atlas = [
            {
                "winter_sports_id": "1",
                "english_name": "Killington Resort",
                "name": "Killington",
                "state": "Vermont",
                "country": "United States of America",
            },
            {
                "winter_sports_id": "2",
                "english_name": "Stowe Mountain Resort",
                "name": "Stowe",
                "state": "Vermont",
                "country": "United States of America",
            },
        ]
        chip = PassChip("ikon", "Ikon", r"ikon", ("ikonpass.com",), ("https://www.ikonpass.com/en/destinations",))
        listed = harvest_pass_list(chip, html_by_url={"https://www.ikonpass.com/en/destinations": IKON_HTML})
        merged = merge_rosters(listed, atlas)
        killington = next(row for row in merged if "Killington" in row["listed_name"])
        self.assertEqual(killington["status"], "matched")
        self.assertEqual(killington["winter_sports_id"], "1")
        self.assertEqual(killington["atlas_name"], "Killington Resort")
        hit, score, rivals = match_listed("Killington", atlas_index(atlas))
        self.assertGreaterEqual(score, 0.86)
        self.assertEqual(hit["winter_sports_id"], "1")
        self.assertEqual(rivals, [])

    def test_writes_chip_overlay(self) -> None:
        merged = [
            {
                "pass_key": "ikon",
                "pass_label": "Ikon",
                "listed_name": "Killington",
                "listed_url": "https://www.ikonpass.com/en/destinations/killington-resort",
                "list_url": "https://www.ikonpass.com/en/destinations",
                "status": "matched",
                "match_score": 1.0,
                "winter_sports_id": "1",
                "atlas_name": "Killington Resort",
                "state": "Vermont",
                "country": "United States",
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            write_rosters(merged, directory)
            text = (directory / "chips.jsonl").read_text(encoding="utf-8")
            self.assertIn('"ikon": true', text)
            self.assertIn("Killington Resort", (directory / "merged.csv").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
