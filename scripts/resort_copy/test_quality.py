"""Unit tests for resort-copy validators, the entity filter, and length checks.

Run: python scripts/resort_copy/test_quality.py
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.config import TIER_FACT_MIN, fact_min_for, write_floor_for  # noqa: E402
from resort_copy.ollama_client import _parse_json_loose  # noqa: E402
from resort_copy.queries import large_area_queries, topic_queries  # noqa: E402
from resort_copy.snippets import _drop_pages_about_other_places, _select_snippets  # noqa: E402
from generate_resort_copy_bedrock import _chunk_article, _wiki_title_score  # noqa: E402
from resort_copy.notes import _salvage_bullets  # noqa: E402
from resort_copy.validators import (  # noqa: E402
    deterministic_critique,
    distance_has_unit,
    drop_foreign_sentences,
    is_placeholder_name,
    length_report,
    note_char_target,
    note_chars,
    opening_year,
    ownership_kind,
    paragraph_budgets,
    price_has_currency,
    reconcile_distances,
    sanitize_notes,
    snippet_entity_decision,
)


class FieldTests(unittest.TestCase):
    def test_timestamp_is_not_an_opening_year(self) -> None:
        self.assertIsNone(opening_year("2026-11-10T15:53:12Z"))
        self.assertIsNone(opening_year("Ordino-Arcalis page scraped 2026-11-10 and the resort opened then"))

    def test_post_office_is_not_the_resort_opening(self) -> None:
        self.assertIsNone(opening_year("The Mount Buller post office opened in 1958."))

    def test_real_opening_year(self) -> None:
        self.assertEqual(opening_year("The resort opened in 1924."), 1924)
        self.assertIsNone(opening_year("The resort opened in 1840."))
        self.assertIsNone(opening_year("The resort opened in 2099."))

    def test_ownership_allow_list(self) -> None:
        self.assertEqual(ownership_kind("The resort is private."), "private")
        self.assertEqual(ownership_kind("The hill is a not-for-profit ski club."), "not-for-profit")
        self.assertEqual(ownership_kind("The area is municipal."), "municipal")
        self.assertIsNone(ownership_kind("Visitors walk to San Telmo for coffee."))

    def test_price_needs_currency(self) -> None:
        self.assertFalse(price_has_currency("Adult day ticket is 89."))
        self.assertTrue(price_has_currency("An adult day ticket is $89 CAD."))
        self.assertTrue(price_has_currency("A day pass costs 49 EUR."))

    def test_distance_needs_a_unit(self) -> None:
        self.assertFalse(distance_has_unit("Mount Buller is 248 from Melbourne."))
        self.assertTrue(distance_has_unit("Mount Buller is 248 km from Melbourne."))

    def test_sanitize_drops_the_bad_field_notes(self) -> None:
        notes = [
            {"topic": "year_started", "text": "2026-11-10T15:53:12Z", "snippet_ids": ["D1"]},
            {"topic": "year_started", "text": "The post office opened in 1958.", "snippet_ids": ["D2"]},
            {"topic": "public_or_private", "text": "Visitors walk to San Telmo.", "snippet_ids": ["D3"]},
            {"topic": "price", "text": "Adult day ticket is 89.", "snippet_ids": ["D4"]},
            {"topic": "access", "text": "It is 248 from Melbourne.", "snippet_ids": ["D5"]},
            {"topic": "scene", "text": "The most extraordinary ski resort in Andorra.", "snippet_ids": ["D6"]},
            {
                "topic": "pass",
                "text": "Ben Eoin is on the Epic Pass.",
                "snippet_ids": ["D7"],
            },
            {"topic": "year_started", "text": "The resort opened in 1983.", "snippet_ids": ["D8"]},
        ]
        snippets = [
            {"id": "D7", "url": "https://www.liftopia.com/ben-eoin", "text": "Epic Pass", "title": "Ben Eoin"},
            {"id": "D8", "url": "https://example.com/history", "text": "opened", "title": "History"},
        ]
        kept, errors = sanitize_notes(notes, name="Ben Eoin", snippets=snippets)
        reasons = {err["reason"] for err in errors}
        self.assertIn("year_rejected", reasons)
        self.assertIn("ownership_rejected", reasons)
        self.assertIn("price_missing_currency", reasons)
        self.assertIn("distance_missing_unit", reasons)
        self.assertIn("marketing", reasons)
        self.assertIn("pass_from_reseller", reasons)
        self.assertEqual([note["text"] for note in kept], ["The resort opened in 1983."])


class EntityTests(unittest.TestCase):
    def test_afriski_snippet_is_not_malbun(self) -> None:
        blob = "AfriSki in Lesotho is a ski resort in the Maluti Mountains."
        ok, reason = snippet_entity_decision(
            blob, "Malbun", country="Liechtenstein", state=None
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "name_missing")

    def test_country_mismatch_when_the_name_is_present(self) -> None:
        blob = "Malbun skiing is often confused with AfriSki in Lesotho."
        ok, reason = snippet_entity_decision(blob, "Malbun", country="Liechtenstein")
        self.assertFalse(ok)
        self.assertEqual(reason, "country_mismatch")

    def test_matching_snippet_kept_and_foreign_one_dropped(self) -> None:
        snippets = [
            {
                "id": "1",
                "title": "Malbun",
                "url": "https://en.wikipedia.org/wiki/Malbun",
                "text": "Malbun is a ski resort in Liechtenstein.",
                "fetched": True,
            },
            {
                "id": "2",
                "title": "AfriSki",
                "url": "https://example.com/afriski",
                "text": "AfriSki in Lesotho offers skiing.",
                "fetched": True,
            },
        ]
        kept, dropped = _drop_pages_about_other_places(
            snippets, "Malbun", country="Liechtenstein"
        )
        self.assertEqual(dropped, 1)
        self.assertEqual(len(kept), 1)
        self.assertIn("Liechtenstein", kept[0]["text"])

    def test_foreign_sentence_removed_from_prose(self) -> None:
        notes = [{"topic": "access", "text": "Malbun is in Liechtenstein."}]
        draft = "AfriSki in Lesotho draws visitors. Malbun is in Liechtenstein."
        cleaned = drop_foreign_sentences(draft, notes, name="Malbun", country="Liechtenstein")
        self.assertNotIn("AfriSki", cleaned)
        self.assertIn("Liechtenstein", cleaned)


class LengthAndCriticTests(unittest.TestCase):
    def test_fact_minimums(self) -> None:
        self.assertEqual(fact_min_for("small"), 6)
        self.assertGreaterEqual(fact_min_for("medium"), 20)
        self.assertGreaterEqual(fact_min_for("large"), 40)
        self.assertEqual(write_floor_for("small"), 6)
        self.assertEqual(write_floor_for("medium"), 22)
        self.assertEqual(write_floor_for("large"), 22)

    def test_wikipedia_article_is_split_and_the_ski_page_wins(self) -> None:
        self.assertGreater(
            _wiki_title_score("Killington Ski Resort", "Killington Resort Vermont"),
            _wiki_title_score("Killington, Vermont secession movement", "Killington Resort Vermont"),
        )
        self.assertGreater(
            _wiki_title_score("Camelback Mountain Resort", "Camelback Mountain Resort Pennsylvania"),
            _wiki_title_score("List of ski areas and resorts in the United States", "Camelback Mountain Resort Pennsylvania"),
        )
        chunks = _chunk_article("Intro about the resort.\n\n" + ("History paragraph. " * 80), size=200, limit=8)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= 200 or "\n" not in chunk for chunk in chunks))
        self.assertNotIn("mega", TIER_FACT_MIN)

    def test_budgets_cover_the_character_target(self) -> None:
        for tier, low in (("medium", 3700), ("large", 7400)):
            budgets = paragraph_budgets(tier)
            total = sum(budget for _name, budget in budgets)
            self.assertGreaterEqual(total, low - 200)
            self.assertGreaterEqual(len(budgets), 5 if tier == "medium" else 10)

    def test_short_page_is_flagged(self) -> None:
        report = length_report("A short page about the hill.", "small")
        self.assertTrue(report["short"])
        self.assertLess(report["character_pct"], 100)

    def test_note_target_is_the_upper_plate(self) -> None:
        self.assertEqual(note_char_target("small"), 1650)
        self.assertEqual(note_char_target("medium"), 4050)
        self.assertEqual(note_char_target("large"), 8100)
        self.assertEqual(note_chars([{"text": "abc"}, {"text": " de "}]), 5)

    def test_cut_off_bullet_list_keeps_complete_items(self) -> None:
        raw = (
            '{"bullets":['
            '{"topic":"price","text":"Adult day ticket is $89.","snippet_ids":["D1"]},'
            '{"topic":"season","text":"The season opens in December'
        )
        kept = _salvage_bullets(raw)
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["text"], "Adult day ticket is $89.")

    def test_distance_contradiction_is_rewritten_from_notes(self) -> None:
        notes = [{"topic": "access", "text": "Mount Buller is 248 km from Melbourne."}]
        draft = "The drive is 208 km from Melbourne."
        critique = deterministic_critique(draft, notes, name="Mount Buller", tier="medium")
        self.assertTrue(critique["contradictions"])
        fixed = reconcile_distances(draft, notes)
        self.assertIn("248", fixed)
        self.assertNotIn("208", fixed)

    def test_unsupported_number_and_no_freeform_mood_item(self) -> None:
        notes = [{"topic": "history", "text": "The resort opened in 1983."}]
        draft = "The resort opened in 1983. AfriSki has 12 lifts."
        critique = deterministic_critique(draft, notes, name="Caviahue", country="Argentina", tier="small")
        blob = " ".join(critique["unsupported"])
        self.assertIn("12", blob)
        self.assertNotIn("sentence about mood", blob)
        mood = deterministic_critique(
            "The fireplaces make the lodge cozy.",
            notes,
            name="Caviahue",
            tier="small",
        )
        self.assertTrue(any("fireplace" in item.lower() for item in mood["unsupported"]))

    def test_placeholder_and_topic_queries(self) -> None:
        self.assertTrue(is_placeholder_name("A venir")[0])
        self.assertTrue(is_placeholder_name("à venir")[0])
        self.assertFalse(is_placeholder_name("Malbun")[0])
        queries = topic_queries("Malbun", None, "Liechtenstein", ["year_started", "access"], tier="medium")
        self.assertTrue(any("founded" in query or "opened" in query for query in queries))
        self.assertTrue(any("wikipedia.org" in query for query in queries))
        self.assertTrue(large_area_queries("Cerro Catedral", "Rio Negro"))

    def test_json_fence_parses(self) -> None:
        payload = _parse_json_loose('```json\n{"beats":["one"]}\n```')
        self.assertEqual(payload["beats"], ["one"])
        with self.assertRaises(json.JSONDecodeError):
            _parse_json_loose("Expecting ',' delimiter {")

    def test_select_snippets_reports_drops(self) -> None:
        snippets = [
            {"id": "1", "title": "Tahko", "url": "https://tahko.example/a", "text": "Tahko ski resort in Finland opened in 1968."},
            {"id": "2", "title": "Other", "url": "https://example.com/b", "text": "A ski app for the Laurentians and Saint-Sauveur."},
        ]
        chosen, dropped = _select_snippets(snippets, limit=8, chars=400, name="Tahko", country="Finland")
        self.assertEqual(dropped, 1)
        self.assertEqual(len(chosen), 1)


if __name__ == "__main__":
    unittest.main()
