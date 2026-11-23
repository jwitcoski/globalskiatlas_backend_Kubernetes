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

from resort_copy.config import (  # noqa: E402
    NOTE_TEXT_MAX_WORDS,
    TIER_FACT_MIN,
    fact_min_for,
    search_ladder_for,
    write_char_floor_for,
    write_floor_for,
)
from resort_copy.writing import _stitch_unused_notes  # noqa: E402
from resort_copy.ollama_client import _parse_json_loose  # noqa: E402
from resort_copy.queries import large_area_queries, topic_queries  # noqa: E402
from resort_copy.snippets import _drop_pages_about_other_places, _select_snippets  # noqa: E402
from generate_resort_copy_bedrock import _chunk_article, _wiki_title_score  # noqa: E402
from resort_copy.notes import (  # noqa: E402
    _salvage_bullets,
    extend_notes_with_page_facts,
    merge_page_facts,
    split_page_windows,
)
from resort_copy.web_fetch import _page_body  # noqa: E402
from resort_copy.config import PAGE_FACT_WINDOW_CHARS, PAGE_BODY_MAX_CHARS  # noqa: E402
from resort_copy.stay22 import lodging_weekend, stay22_map_url, add_stay22_lodging_snippet  # noqa: E402
from resort_copy.stickies import stickies_for_tier  # noqa: E402
from resort_copy import topup  # noqa: E402
from resort_copy.topup import dedupe_notes, top_up_notes  # noqa: E402
from resort_copy.pipeline import _write_blockers  # noqa: E402
from datetime import date  # noqa: E402
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
    unused_notes,
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

    def test_sanitize_drops_empty_encyclopedia_and_duplicate_anchors(self) -> None:
        notes = [
            {"topic": "year_started", "text": "The resort opened in 1963.", "snippet_ids": ["A"]},
            {"topic": "year_started", "text": "Camelback opened in December 1963 after a land deal.", "snippet_ids": ["B"]},
            {"topic": "price", "text": "No specific information is provided about lift tickets.", "snippet_ids": ["C"]},
            {
                "topic": "other",
                "text": "Camelback has transient dominant vegetation types of Northern Hardwood (106).",
                "snippet_ids": ["D"],
            },
            {"topic": "who", "text": "a top destination for families, thrill-seekers and first-time visitors", "snippet_ids": ["E"]},
        ]
        kept, errors = sanitize_notes(notes, name="Camelback")
        reasons = {err["reason"] for err in errors}
        self.assertIn("empty_fact", reasons)
        self.assertIn("encyclopedia", reasons)
        self.assertIn("duplicate_anchor", reasons)
        self.assertIn("marketing", reasons)
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["topic"], "year_started")


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
        self.assertEqual(write_char_floor_for("small"), 1450)
        self.assertEqual(write_char_floor_for("medium"), 3700)
        self.assertEqual(write_char_floor_for("large"), 7400)
        self.assertEqual(search_ladder_for("small"), ("small", "medium"))
        self.assertEqual(search_ladder_for("medium"), ("medium", "large"))
        self.assertEqual(search_ladder_for("large"), ("large",))
        short_notes = [{"text": "Opened in 1968 as a club hill."}] * 22
        self.assertIn("note characters is below", _write_blockers(short_notes, "medium") or "")
        few = [{"text": "A " + ("long grounded visit fact about the ridge and the lodge. " * 20)}] * 5
        self.assertIn("notes is below", _write_blockers(few, "medium") or "")
        enough = [
            {"text": f"A {i} long grounded visit fact about the ridge and the lodge. " * 20}
            for i in range(22)
        ]
        self.assertIsNone(_write_blockers(enough, "medium"))

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
        self.assertEqual(note_char_target("small"), 3300)
        self.assertEqual(note_char_target("medium"), 8100)
        self.assertEqual(note_char_target("large"), 16200)
        self.assertEqual(note_chars([{"text": "abc"}, {"text": " de "}]), 5)

    def test_note_word_cap_is_triple_the_old_bullet(self) -> None:
        self.assertEqual(NOTE_TEXT_MAX_WORDS, 75)

    def test_long_notes_are_unused_until_their_content_lands(self) -> None:
        note = {
            "text": (
                "Rather than sitting on a single isolated slope, Killington spans six "
                "mountain peaks within the Green Mountains, which are over 900 million "
                "years old and worn into rounded peaks and deep valleys."
            )
        }
        self.assertTrue(unused_notes("Killington is a ski resort in Vermont.", [note]))
        self.assertFalse(
            unused_notes(
                "Killington spans six mountain peaks within the Green Mountains, "
                "which are over 900 million years old.",
                [note],
            )
        )

    def test_stitch_puts_leftover_notes_on_a_short_page(self) -> None:
        leftover = [
            {
                "text": "Day tickets are $89 for adults on a weekend, copied from the rate sheet."
            }
        ]
        page = _stitch_unused_notes("The hill opened in 1958.", leftover, "small")
        self.assertIn("$89", page)
        self.assertGreater(len(page), 40)

    def test_page_fact_remine_keeps_old_and_adds_new(self) -> None:
        prior = [{"topic": "price", "text": "Adult day tickets start at $89."}]
        more = [
            {"topic": "price", "text": "Adult day tickets start at $89."},
            {"topic": "other", "text": "Ski and snowboard rentals are at the base lodge."},
        ]
        merged = merge_page_facts(prior, more)
        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[1]["text"], "Ski and snowboard rentals are at the base lodge.")
        notes = extend_notes_with_page_facts(
            [{"topic": "setting", "text": "A ridge above the valley town.", "snippet_ids": ["W1"]}],
            [{"topic": "price", "text": "Adult day tickets start at $89.", "snippet_ids": ["D1"]}],
            name="Test Resort",
            state="Vermont",
            country="United States of America",
            fact_min=6,
            cap=28,
        )
        texts = {note["text"] for note in notes}
        self.assertIn("A ridge above the valley town.", texts)
        self.assertIn("Adult day tickets start at $89.", texts)

    def test_sticky_counts_grow_with_tier(self) -> None:
        self.assertEqual(len(stickies_for_tier("small")), 10)
        self.assertEqual(len(stickies_for_tier("medium")), 16)
        self.assertGreaterEqual(len(stickies_for_tier("large")), 16)
        self.assertEqual(stickies_for_tier("small")[0].id, "setting")

    def test_stay22_weekend_bookmarks_by_hemisphere(self) -> None:
        today = date(2026, 11, 19)
        north_in, north_out = lodging_weekend(lat=43.6, today=today)
        south_in, south_out = lodging_weekend(lat=-41.1, today=today)
        self.assertEqual(north_in.isoformat(), "2027-02-20")
        self.assertEqual(north_out.isoformat(), "2027-02-22")
        self.assertEqual(south_in.isoformat(), "2027-08-14")
        self.assertEqual(south_out.isoformat(), "2027-08-16")
        url = stay22_map_url(
            lat=43.6,
            lon=-72.8,
            name="Killington Resort",
            checkin=north_in,
            checkout=north_out,
        )
        self.assertIn("stay22.com", url)
        self.assertIn("checkin=2027-02-20", url)
        snippets: list[dict] = []
        add_stay22_lodging_snippet(
            snippets,
            name="Killington Resort",
            row={"centroid_lat": 43.6, "centroid_lon": -72.8},
            today=today,
        )
        self.assertEqual(len(snippets), 1)
        self.assertEqual(snippets[0]["source"], "stay22")

    def test_page_body_is_kept_and_split_into_fact_windows(self) -> None:
        blocks = [
            "Killington Resort opened with a single lift and a day ticket price of $5.",
            "Ski and snowboard rentals are in the Snowshed base lodge near the village.",
            "The Epic Pass is sold for this mountain according to the resort ticket desk.",
        ]
        # Pad so the stored body is longer than one excerpt window.
        blocks.extend(f"Scene paragraph {i} about the ridge and the lodge cafeteria." for i in range(40))
        body = _page_body(blocks, name="Killington Resort", about_resort=True)
        self.assertGreater(len(body), PAGE_FACT_WINDOW_CHARS)
        self.assertLessEqual(len(body), PAGE_BODY_MAX_CHARS)
        self.assertIn("$5", body)
        self.assertIn("rentals", body)
        windows = split_page_windows(body)
        self.assertGreater(len(windows), 1)
        self.assertTrue(all(len(window) <= PAGE_FACT_WINDOW_CHARS + 50 for window in windows))
        joined = "\n".join(windows)
        self.assertIn("$5", joined)

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


class TopUpTests(unittest.TestCase):
    """The sticky pass alone cannot reach the 22-note floor for medium and large plates."""

    def test_sticky_count_is_below_the_floor_so_top_up_is_required(self) -> None:
        from resort_copy.config import write_floor_for

        self.assertLess(len(stickies_for_tier("medium")), write_floor_for("medium"))
        self.assertLess(len(stickies_for_tier("large")), write_floor_for("large"))

    def test_dedupe_drops_the_same_sentence_lifted_by_three_stickies(self) -> None:
        same = (
            "Stowe Mountain Resort is a ski resort in the northeastern United States, "
            "near the town of Stowe in northern Vermont, on Mount Mansfield."
        )
        notes = [
            {"topic": "year_started", "text": "Stowe opened in 1933 with CCC trail cutting.", "snippet_ids": ["W1"]},
            {"topic": "review", "text": same, "snippet_ids": ["W1"]},
            {"topic": "other", "text": same + " It is the largest ski area in the state.", "snippet_ids": ["W1"]},
            {"topic": "scene", "text": same, "snippet_ids": ["W2"]},
            {"topic": "lodging", "text": "The Lodge at Spruce Peak is ski-in ski-out with a heated pool.", "snippet_ids": ["X3"]},
        ]
        kept = dedupe_notes(notes)
        self.assertEqual([n["topic"] for n in kept], ["year_started", "review", "lodging"])

    def test_dedupe_keeps_anchor_topics_that_overlap_a_color_note(self) -> None:
        notes = [
            {"topic": "other", "text": "The area opened in 1961 above the village of Tahko.", "snippet_ids": ["X1"]},
            {"topic": "year_started", "text": "The area opened in 1961 above the village of Tahko.", "snippet_ids": ["X1"], "value": 1961},
            {"topic": "year_started", "text": "Opened in 1961 above Tahko village.", "snippet_ids": ["X2"], "value": 1961},
        ]
        kept = dedupe_notes(notes)
        # An exact repeat goes; a near-duplicate anchor stays because anchors are never thinned.
        self.assertEqual(len(kept), 2)
        self.assertEqual(kept[1]["topic"], "year_started")

    def test_top_up_returns_at_once_when_the_pile_is_already_full(self) -> None:
        calls: list[str] = []
        original = topup.read_page_facts
        topup.read_page_facts = lambda *a, **k: calls.append("x") or []  # type: ignore[assignment]
        try:
            notes = [
                {
                    "topic": "year_started",
                    "text": (
                        "Tahko opened in 1968 as a club hill on the Nilsiä ridge, with volunteer "
                        "rope tows before the first chair and a later spa hotel at the base."
                    )
                    * 6,
                    "snippet_ids": ["X1"],
                    "value": 1968,
                },
                {
                    "topic": "setting",
                    "text": (
                        "Tahko sits above Lake Syväri in Nilsiä, Finland, with forested lake views "
                        "from the ridge and a long access road down to the village shoreline."
                    )
                    * 6,
                    "snippet_ids": ["X1"],
                },
                {
                    "topic": "lodging",
                    "text": (
                        "Tahko Spa hotel has an indoor pool, a sauna wing, and slope-side rooms "
                        "sold as weekend packages next to the Break Sokos plaza."
                    )
                    * 6,
                    "snippet_ids": ["X1"],
                },
                {
                    "topic": "scene",
                    "text": (
                        "The snow park hosts a freestyle week each March with local riders, "
                        "judges, and a torchlight descent on the Pehkubaari pitch."
                    )
                    * 6,
                    "snippet_ids": ["X1"],
                },
                {
                    "topic": "who",
                    "text": (
                        "Guides call the front side friendly for families and beginners, with "
                        "rental meeting points at the Break Sokos base lodge every morning."
                    )
                    * 6,
                    "snippet_ids": ["X1"],
                },
                {
                    "topic": "other",
                    "text": (
                        "Rentals and lessons meet at the Break Sokos base lodge, then ski school "
                        "groups ride the Tahko lifts to the mid-station classroom."
                    )
                    * 6,
                    "snippet_ids": ["X1"],
                },
            ]
            out, attempts, errors, windows, _mined = top_up_notes(
                notes,
                [{"id": "X1", "source": "searxng", "title": "Tahko", "url": "https://tahko.fi", "text": "Tahko ski resort", "body": "x" * 500, "fetched": True}],
                resort_label="Tahko, Finland",
                name="Tahko",
                state=None,
                country="Finland",
                tier="small",
                base_url="http://127.0.0.1:1",
                model="m",
                num_ctx=4096,
                timeout=1,
                page_limit=4,
                use_ddg=False,
                gap_topics=lambda notes: [],
            )
        finally:
            topup.read_page_facts = original  # type: ignore[assignment]
        self.assertEqual(calls, [])
        self.assertEqual((attempts, windows), (0, 0))
        self.assertEqual(len(out), 6)

    def test_top_up_mines_fetched_pages_and_tags_the_page_id(self) -> None:
        body = "\n".join(
            f"Tahko ski resort paragraph {i} about the slopes, lodges, and the Nilsiä village." for i in range(40)
        )
        pile = [
            {"id": "W1", "source": "wikipedia", "title": "Tahko", "url": "https://en.wikipedia.org/wiki/Tahko", "text": "Tahko ski resort in Nilsiä, Finland.", "body": body, "fetched": True},
            {"id": "X1", "source": "searxng", "title": "Tahko", "url": "https://tahko.fi/en", "text": "Tahko ski resort official site.", "body": body, "fetched": True},
        ]
        asked: list[str] = []

        facts_by_page = {
            "W1": [
                (
                    "Tahko Spa hotel has an indoor pool and a sauna wing for guests staying on the ridge. "
                    "Weekend packages include a slope-side room, breakfast, and a shuttle to the lifts."
                )
                * 5,
                (
                    "The snow park hosts a freestyle week each March with local riders and judges. "
                    "The course uses the Pehkubaari pitch and a night session after last chair."
                )
                * 5,
                (
                    "Guides call the front side friendly for families and beginners on the Nilsiä pitch. "
                    "Ski school groups meet below the first chair before riding to the mid-station."
                )
                * 5,
                (
                    "Rentals and lessons meet at the Break Sokos base lodge every winter morning. "
                    "Boots, helmets, and a locker key are issued at the plaza desk."
                )
                * 5,
            ],
            "X1": [
                (
                    "The Tahko Golf clubhouse serves lunch to skiers in winter beside the first fairway. "
                    "The kitchen stays open after the last lift and sells a slope-side soup."
                )
                * 5,
                (
                    "A torchlight descent closes the season on the Pehkubaari slope after last chair. "
                    "Staff light the pitch and skiers gather at the grill before walking down."
                )
                * 5,
            ],
        }

        def fake_read(page, **kwargs):
            asked.append(page["id"])
            return [{"topic": "other", "text": text} for text in facts_by_page[page["id"]]]

        original = topup.read_page_facts
        topup.read_page_facts = fake_read  # type: ignore[assignment]
        try:
            start = [
                {
                    "topic": "year_started",
                    "text": (
                        "Tahko opened in 1968 as a club hill, with volunteer rope tows before the first "
                        "chair and a later spa hotel at the Nilsiä base."
                    )
                    * 5,
                    "snippet_ids": ["W1"],
                    "value": 1968,
                },
                {
                    "topic": "setting",
                    "text": (
                        "Tahko sits above Lake Syväri in Nilsiä, Finland, with forested lake views from "
                        "the ridge and a long access road down to the village shoreline."
                    )
                    * 5,
                    "snippet_ids": ["W1"],
                },
            ]
            out, attempts, errors, windows, _mined = top_up_notes(
                start,
                pile,
                resort_label="Tahko, Finland",
                name="Tahko",
                state=None,
                country="Finland",
                tier="small",
                base_url="http://127.0.0.1:1",
                model="m",
                num_ctx=4096,
                timeout=1,
                page_limit=4,
                use_ddg=False,
                gap_topics=lambda notes: ["lodging"],
            )
        finally:
            topup.read_page_facts = original  # type: ignore[assignment]
        self.assertEqual(asked[0], "W1", "Wikipedia page is read first")
        self.assertGreaterEqual(len(out), 6, "small floor reached from page facts")
        self.assertEqual(attempts, 0, "no web search needed when the pile already has pages")
        self.assertGreater(windows, 0)
        self.assertLessEqual(windows, topup.TOPUP_WINDOWS_PER_PAGE * len(asked))
        mined = {n["text"]: n["snippet_ids"] for n in out[2:]}
        self.assertEqual(mined[facts_by_page["W1"][0]], ["W1"], "page facts cite the page they came from")
        self.assertTrue(all(ids in (["W1"], ["X1"]) for ids in mined.values()))

    def test_top_up_keeps_mining_when_short_of_the_floor(self) -> None:
        """24 windows stay available until the pantry target, not only the write floor."""
        body = "Tahko ski resort. " * 80
        pile = [
            {
                "id": f"X{i}",
                "source": "searxng",
                "title": "Tahko",
                "url": f"https://tahko.fi/page{i}",
                "text": "Tahko ski resort in Finland.",
                "body": body,
                "fetched": True,
            }
            for i in range(1, 6)
        ]
        asked: list[str] = []
        original = topup.read_page_facts
        orig_cap, orig_short = topup.TOPUP_MAX_WINDOWS, topup.TOPUP_MAX_WINDOWS_IF_SHORT
        topup.TOPUP_MAX_WINDOWS = 3
        topup.TOPUP_MAX_WINDOWS_IF_SHORT = 20

        def fake_read(page, **kwargs):
            asked.append(page["id"])
            n = len(asked)
            extras = [
                (
                    "Pehkubaari grill books Friday tables for ski-in diners and keeps a wood stove going until midnight.",
                    "Torchlight descent volunteers walk the pitch with lanterns after last chair in March.",
                ),
                (
                    "Break Sokos spa sells overnight rooms with indoor pool access and a locker for rental boots.",
                    "A village ski bus loops Nilsiä every half hour and drops families at the plaza.",
                ),
                (
                    "Syväri lakeside cabins sleep eight, with saunas facing the ice and a ski-bus stop at the dock.",
                    "Ice fishing weekends rent huts on the lake and serve pea soup in the clubhouse.",
                ),
                (
                    "The golf clubhouse restaurant plates elk stew in winter and seats groups beside the first fairway.",
                    "A midsummer slope concert uses the mid-station stage and a fireworks cue at dusk.",
                ),
                (
                    "Huoneisto apartments above the plaza include kitchens, drying rooms, and underground parking.",
                    "World cup slalom training sets gates on the steepest Nilsiä pitch each January.",
                ),
            ]
            a, b = extras[n - 1]
            pad_a = " ".join(f"cabindetail{n}a{i}" for i in range(40))
            pad_b = " ".join(f"nightdetail{n}b{i}" for i in range(40))
            return [
                {"topic": "other", "text": f"{a} {pad_a}"},
                {"topic": "other", "text": f"{b} {pad_b}"},
            ]

        topup.read_page_facts = fake_read  # type: ignore[assignment]
        try:
            out, attempts, errors, windows, _mined = top_up_notes(
                [{"topic": "year_started", "text": "Tahko opened in 1968 as a club hill.", "snippet_ids": ["X1"], "value": 1968}],
                pile,
                resort_label="Tahko, Finland",
                name="Tahko",
                state=None,
                country="Finland",
                tier="small",
                base_url="http://127.0.0.1:1",
                model="m",
                num_ctx=4096,
                timeout=1,
                page_limit=4,
                use_ddg=False,
                gap_topics=lambda notes: [],
            )
        finally:
            topup.read_page_facts = original  # type: ignore[assignment]
            topup.TOPUP_MAX_WINDOWS = orig_cap
            topup.TOPUP_MAX_WINDOWS_IF_SHORT = orig_short
        self.assertGreaterEqual(len(asked), 2, "kept reading after the 12-window prefer-stop")
        self.assertGreaterEqual(len(out), 6)
        self.assertEqual(attempts, 0)

    def test_top_up_skips_stay22_and_stops_empty_searches(self) -> None:
        stay = {
            "id": "P1",
            "source": "dataset",
            "title": "Stay22",
            "url": "https://www.stay22.com/map?address=Tahko",
            "text": "Map of hotels near Tahko ski resort. " * 20,
            "body": "Map of hotels near Tahko ski resort. " * 20,
            "fetched": True,
        }
        wiki = {
            "id": "W1",
            "source": "wikipedia",
            "title": "Tahko",
            "url": "https://en.wikipedia.org/wiki/Tahko",
            "text": "Tahko ski resort in Finland.",
            "body": "Tahko ski resort in Nilsiä, Finland. " * 40,
            "fetched": True,
        }
        asked: list[str] = []
        searches = {"n": 0}
        original = topup.read_page_facts
        orig_add = topup.add_web_snippets
        orig_fetch = topup.fetch_resort_pages

        def fake_read(page, **kwargs):
            asked.append(page["id"])
            return [{"topic": "lodging", "text": "Tahko Spa hotel has an indoor pool and a sauna wing for guests."}]

        def fake_add(snippets, queries, **kwargs):
            searches["n"] += 1
            return snippets

        def fake_fetch(*a, **k):
            return 0

        topup.read_page_facts = fake_read  # type: ignore[assignment]
        topup.add_web_snippets = fake_add  # type: ignore[assignment]
        topup.fetch_resort_pages = fake_fetch  # type: ignore[assignment]
        try:
            out, attempts, errors, windows, _mined = top_up_notes(
                [{"topic": "year_started", "text": "Tahko opened in 1968 as a club hill.", "snippet_ids": ["W1"], "value": 1968}],
                [stay, wiki],
                resort_label="Tahko, Finland",
                name="Tahko",
                state=None,
                country="Finland",
                tier="small",
                base_url="http://127.0.0.1:1",
                model="m",
                num_ctx=4096,
                timeout=1,
                page_limit=4,
                use_ddg=True,
                gap_topics=lambda notes: ["lodging", "prices"],
            )
        finally:
            topup.read_page_facts = original  # type: ignore[assignment]
            topup.add_web_snippets = orig_add  # type: ignore[assignment]
            topup.fetch_resort_pages = orig_fetch  # type: ignore[assignment]
        self.assertEqual(asked, ["W1"])
        self.assertLessEqual(searches["n"], 2, "pass/price search then one empty stop, not a retry storm")
        self.assertLessEqual(attempts, 2)


class BriefTests(unittest.TestCase):
    def test_filler_does_not_count_toward_the_plate(self) -> None:
        from resort_copy.brief import is_filler_note, page_brief, usable_note_chars

        filler = {
            "topic": "scene",
            "text": "Snow tubing is available at the resort, but no specific details are provided in the indexed pages.",
        }
        real = {
            "topic": "year_started",
            "text": "Killington Resort opened on December 13, 1958, after Perry H. Merrill developed the area with Preston Leete Smith.",
        }
        self.assertTrue(is_filler_note(filler))
        self.assertFalse(is_filler_note(real))
        self.assertLess(usable_note_chars([filler, real]), note_chars([filler, real]))

    def test_brief_gets_ikon_from_the_fact_card(self) -> None:
        from resort_copy.brief import compact_key_points, inject_fact_notes, page_brief

        points = compact_key_points(
            {
                "website": {"value": "https://killington.com/", "sources": ["https://killington.com/"]},
                "year_started": {
                    "value": "Killington Resort opened on December 13, 1958, after Perry H. Merrill developed the area."
                },
                "pass_products": [
                    {"value": "Ikon", "sources": ["https://killington.com/lift-tickets"], "snippet_ids": ["X29"]},
                    {"value": "Indy", "sources": ["https://www.indyskipass.com/our-resorts"]},
                ],
            }
        )
        self.assertEqual(points["year_started"]["value"], "1958")
        notes = inject_fact_notes(
            [{"topic": "setting", "text": "Killington Peak sits in the Green Mountains of Vermont, west of the town."}],
            points,
        )
        text = " ".join(str(n.get("text") or "") for n in notes)
        self.assertIn("Ikon", text)
        brief = page_brief(notes, name="Killington", key_points=points)
        self.assertIn("[passes]", brief)
        self.assertIn("Ikon", brief)
        self.assertNotIn("snippet_ids", brief)


if __name__ == "__main__":
    unittest.main()
