"""Tests for the resort URL reference layer.

Run: python scripts/resort_copy/test_url_refs.py
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.url_refs import (  # noqa: E402
    DEFAULT_SHEET,
    UrlRef,
    UrlRefIndex,
    classify_url,
    home_url_for,
    read_sheet_name_links,
    refs_from_sheet,
    seed_url_for,
    write_refs,
)


class ClassifyTests(unittest.TestCase):
    def test_kinds(self) -> None:
        self.assertEqual(classify_url("https://skiloveland.com/season-passes/"), "official")
        self.assertEqual(classify_url("https://www.facebook.com/cloudmont"), "facebook")
        self.assertEqual(classify_url("https://www.epicpass.com/ski-resorts/x"), "pass_network")
        self.assertEqual(classify_url("https://wainwright.armymwr.com/programs/birch"), "community")

    def test_home_url_normalizes_deep_links(self) -> None:
        self.assertEqual(home_url_for("https://skiloveland.com/season-passes/"), "https://skiloveland.com/")
        self.assertEqual(
            home_url_for("https://www.facebook.com/cloudmont"),
            "https://www.facebook.com/cloudmont",
        )
        self.assertIsNone(seed_url_for(
            UrlRef(
                name="X",
                state="CO",
                country="United States of America",
                url="https://www.epicpass.com/destinations/vail",
                home_url="https://www.epicpass.com/",
                host="epicpass.com",
                kind="pass_network",
                source="pass_roster",
            )
        ))


class SheetTests(unittest.TestCase):
    @unittest.skipUnless(DEFAULT_SHEET.is_file(), "sheet xlsx not vendored")
    def test_reads_name_hyperlinks(self) -> None:
        rows = read_sheet_name_links(DEFAULT_SHEET)
        self.assertGreater(len(rows), 400)
        by_name = {r["name"]: r for r in rows}
        self.assertIn("Cloudmont", by_name)
        self.assertIn("facebook.com", by_name["Cloudmont"]["url"])
        loveland = [r for r in rows if "Loveland" in r["name"]]
        self.assertTrue(loveland)
        self.assertTrue(any("skiloveland.com" in r["url"] for r in loveland))

    @unittest.skipUnless(DEFAULT_SHEET.is_file(), "sheet xlsx not vendored")
    def test_index_lookup_known_misses(self) -> None:
        refs = refs_from_sheet(DEFAULT_SHEET)
        with tempfile.TemporaryDirectory() as tmp:
            write_refs(refs, Path(tmp))
            idx = UrlRefIndex.load(Path(tmp) / "refs.jsonl")
        dry = idx.lookup(name="Dry Hill", state="New York")
        self.assertIsNotNone(dry)
        self.assertIn("skidryhill.com", seed_url_for(dry) or "")
        whiteface = idx.lookup(name="Whiteface Mountain Ski Center", state="New York")
        # containment / exact sheet name
        if whiteface is None:
            whiteface = idx.lookup(name="Whiteface", state="New York")
        self.assertIsNotNone(whiteface)
        self.assertIn("whiteface.com", seed_url_for(whiteface) or "")


if __name__ == "__main__":
    unittest.main()
