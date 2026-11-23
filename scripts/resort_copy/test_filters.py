"""Filter-table tests. Run: python scripts/resort_copy/test_filters.py"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.filters import (  # noqa: E402
    extract_filters,
    filter_row,
    price_band,
    resort_passes,
    resort_price,
    resort_website,
    select_rows,
)


class FilterTests(unittest.TestCase):
    def test_snowtubing_rentals_and_instructors(self) -> None:
        notes = [
            {"topic": "other", "text": "The resort runs a snow tubing hill on weekends."},
            {"topic": "other", "text": "The rental shop rents skis and snowboards."},
            {"topic": "other", "text": "Ski instructors teach group lessons at the ski school."},
        ]
        flags = extract_filters(notes=notes)
        self.assertTrue(flags["snowtubing"]["value"])
        self.assertTrue(flags["ski_rentals"]["value"])
        self.assertTrue(flags["ski_instructors"]["value"])

    def test_snowmaking_and_waterpark_are_not_snowtubing(self) -> None:
        notes = [
            {
                "topic": "other",
                "text": (
                    "Camelback Mountain Resort usually opens in December with notable snowmaking "
                    "facilities on ski slopes, primarily relying on portable snow guns."
                ),
            },
            {
                "topic": "lodging",
                "text": "Aquatopia Indoor Waterpark has a virtual reality slide and a mountain coaster.",
            },
        ]
        flags = extract_filters(notes=notes)
        self.assertIsNone(flags["snowtubing"]["value"])

    def test_vacation_rental_is_not_a_ski_rental(self) -> None:
        notes = [{"topic": "lodging", "text": "Condo rentals and vacation rentals sit in the village."}]
        flags = extract_filters(notes=notes)
        self.assertIsNone(flags["ski_rentals"]["value"])

    def test_explicit_no_is_false_and_silence_is_unknown(self) -> None:
        notes = [{"topic": "other", "text": "There is no snow tubing at the area."}]
        flags = extract_filters(notes=notes)
        self.assertFalse(flags["snowtubing"]["value"])
        self.assertIsNone(flags["ski_instructors"]["value"])

    def test_key_point_yes_counts(self) -> None:
        flags = extract_filters(key_points={"ski_school": {"value": "yes"}})
        self.assertTrue(flags["ski_instructors"]["value"])

    def test_select_requires_true(self) -> None:
        rows = [
            {"resort": "A", "snowtubing": True, "ski_rentals": True, "ski_instructors": False},
            {"resort": "B", "snowtubing": True, "ski_rentals": None, "ski_instructors": True},
            {"resort": "C", "snowtubing": None, "ski_rentals": True, "ski_instructors": True},
        ]
        both = select_rows(rows, ["snowtubing", "ski_rentals"], match_all=True)
        self.assertEqual([row["resort"] for row in both], ["A"])
        either = select_rows(rows, ["snowtubing", "ski_instructors"], match_all=False)
        self.assertEqual([row["resort"] for row in either], ["A", "B", "C"])

    def test_price_bands_and_passes(self) -> None:
        self.assertEqual(price_band(50), "$")
        self.assertEqual(price_band(100), "$$")
        self.assertEqual(price_band(200), "$$$")
        self.assertEqual(price_band(201), "$$$$")
        band, _shown = resort_price(key_points={"lift_ticket": {"value": "$34.00 CAD (day)"}})
        self.assertEqual(band, "$")
        band, _shown = resort_price(key_points={"lift_ticket": {"value": "€265 for a 6-day adult pass"}})
        self.assertEqual(band, "$")
        band, _shown = resort_price(key_points={"lift_ticket": {"value": "Adult day price: NZD $149"}})
        self.assertEqual(band, "$$")
        band, _shown = resort_price(key_points={"lift_ticket": {"value": "A Mountain Access Card costs $6"}})
        self.assertIsNone(band)
        passes = resort_passes(
            {"pass_products": [{"value": "Epic Pass"}, {"value": "Ikon Pass"}, {"value": "Mountain Collective"}]}
        )
        self.assertEqual(passes, [])
        official = resort_passes(
            {
                "website": {"value": "https://killington.com/"},
                "pass_products": [
                    {"value": "Ikon", "sources": ["https://killington.com/lift-tickets"]},
                    {"value": "Indy", "sources": ["https://www.indyskipass.com/our-resorts"]},
                    {"value": "Mountain Collective", "sources": ["https://mtninfo.com/passes/"]},
                ],
            }
        )
        self.assertEqual(official, ["Ikon"])
        self.assertEqual(resort_passes({"pass_affiliation": {"value": "Nord Pass"}}), ["Nord Pass"])
        self.assertEqual(resort_passes({"pass_affiliation": {"value": "Local area pass: Not found in search results."}}), [])
        self.assertEqual(
            resort_website({"website": {"value": "https://www.fallscreek.com.au/winter"}}),
            "https://www.fallscreek.com.au/winter",
        )

    def test_source_is_official_only_on_the_resort_host(self) -> None:
        snippets = [
            {"id": "D1", "url": "https://skibeneoin.com/tickets", "title": "Tickets"},
            {"id": "D2", "url": "https://www.liftopia.com/ben-eoin", "title": "Ben Eoin tickets"},
        ]
        row = filter_row(
            resort="Ben Eoin",
            notes=[
                {"topic": "other", "text": "The rental shop rents skis.", "snippet_ids": ["D1"]},
                {"topic": "price", "text": "Adult day ticket is $34.00 CAD.", "snippet_ids": ["D2"]},
            ],
            snippets=snippets,
            key_points={"website": {"value": "https://skibeneoin.com/", "sources": ["https://skibeneoin.com/"]}},
        )
        self.assertEqual(row["sources"]["ski_rentals"]["kind"], "official")
        self.assertEqual(row["sources"]["ski_rentals"]["url"], "https://skibeneoin.com/tickets")
        self.assertEqual(row["sources"]["price_band"]["kind"], "third_party")
        self.assertIn("liftopia.com", row["sources"]["price_band"]["url"])
        self.assertEqual(row["sources"]["website"]["kind"], "official")


if __name__ == "__main__":
    unittest.main()
