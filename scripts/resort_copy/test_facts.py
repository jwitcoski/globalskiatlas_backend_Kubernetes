"""Facts table tests using shapes from the 3070 harvest. python scripts/resort_copy/test_facts.py"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from http.client import HTTPConnection
from pathlib import Path
from threading import Thread

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.fact_store import connect, fact_stats, query_facts, replace_facts  # noqa: E402
from resort_copy.facts import fact_row, harvest_fact_rows  # noqa: E402
from resort_copy.facts_api import make_handler  # noqa: E402
from resort_copy.filters import parse_native_day_price  # noqa: E402
from http.server import ThreadingHTTPServer  # noqa: E402


class FactsTests(unittest.TestCase):
    def test_keeps_native_price_and_derives_usd(self) -> None:
        parsed = parse_native_day_price("$34.00 CAD (day)")
        self.assertEqual(parsed["amount"], 34.0)
        self.assertEqual(parsed["currency"], "CAD")
        self.assertAlmostEqual(parsed["usd"], 24.82)
        self.assertEqual(parsed["matched"], "$34.00 CAD")
        row = fact_row(
            key_points_record={
                "winterSportsId": "670137934",
                "resort": "Ben Eoin, Nova Scotia, Canada",
                "key_points": {"lift_ticket": {"value": "$34.00 CAD (day)", "sources": []}},
            }
        )
        self.assertEqual(row["lift_ticket_raw"], "$34.00 CAD (day)")
        self.assertEqual(row["price_band"], "$")
        self.assertNotIn("24", row["lift_ticket_raw"])

    def test_roundup_passes_are_empty_without_a_local_name(self) -> None:
        row = fact_row(
            key_points_record={
                "winterSportsId": "1",
                "key_points": {
                    "pass_products": [{"value": "Epic Pass"}, {"value": "Ikon Pass"}],
                },
            }
        )
        self.assertEqual(row["passes"], [])

    def test_local_and_ikon_can_both_be_stored(self) -> None:
        row = fact_row(
            filters={
                "winterSportsId": "2",
                "state": "Vermont",
                "passes": ["Ikon", "Vermont Skipass"],
            }
        )
        self.assertEqual(row["passes"], ["Ikon", "Vermont Skipass"])

    def test_null_amenity_is_unknown_not_false(self) -> None:
        row = fact_row(
            filters={
                "winterSportsId": "3",
                "night_skiing": None,
                "snowtubing": False,
                "ski_instructors": True,
            }
        )
        self.assertIsNone(row["night_skiing"])
        self.assertIs(row["snowtubing"], False)
        stats = fact_stats([row])
        self.assertEqual(stats["night_skiing"], {"true": 0, "false": 0, "unknown": 1})
        self.assertIsNone(stats["night_skiing_known_share_true"])

    def test_sql_filters_and_median_ignore_null_prices(self) -> None:
        rows = [
            fact_row(
                filters={"winterSportsId": "a", "country": "Canada", "state": "Nova Scotia", "passes": ["Ikon"]},
                key_points_record={
                    "winterSportsId": "a",
                    "key_points": {"lift_ticket": {"value": "C$78 Daily"}},
                },
            ),
            fact_row(
                filters={"winterSportsId": "b", "country": "Canada", "night_skiing": True},
                key_points_record={"winterSportsId": "b", "key_points": {"lift_ticket": {"value": "$34.00 CAD (day)"}}},
            ),
            fact_row(filters={"winterSportsId": "c", "country": "Canada", "night_skiing": None}),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(Path(tmp) / "facts.sqlite")
            replace_facts(conn, rows)
            ikon = query_facts(conn, pass_name="Ikon")
            self.assertEqual([r["winter_sports_id"] for r in ikon], ["a"])
            night = query_facts(conn, amenity="night_skiing")
            self.assertEqual([r["winter_sports_id"] for r in night], ["b"])
            conn.close()
        medians = fact_stats(rows)["median_lift_ticket_usd_by_country"]
        self.assertIn("Canada", medians)
        self.assertGreater(medians["Canada"], 20)

    def test_ingest_merges_filters_and_key_points(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            filters = root / "filters.jsonl"
            points = root / "key_points.jsonl"
            copy = root / "copy.jsonl"
            filters.write_text(
                json.dumps(
                    {
                        "winterSportsId": "1094522661",
                        "pageId": "jay-peak-resort-vermont",
                        "resort": "Jay Peak Resort",
                        "state": "Vermont",
                        "night_skiing": None,
                        "passes": ["Epic"],
                        "sources": {"passes": {"kind": "unknown", "url": ""}},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            points.write_text(
                json.dumps(
                    {
                        "winterSportsId": "1094522661",
                        "key_points": {
                            "year_started": {"value": "1958", "sources": ["https://en.wikipedia.org/wiki/Jay_Peak_Resort"]},
                            "glacier": {"value": "no"},
                            "lift_ticket": {"value": "€25, adult day price", "sources": []},
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            copy.write_text(
                json.dumps(
                    {
                        "winterSportsId": "1094522661",
                        "status": "insufficient_data",
                        "search_attempts": 0,
                        "notes": [{"text": "abc"}],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            harvested = harvest_fact_rows(filters_path=filters, key_points_path=points, copy_paths=[copy])
            self.assertEqual(len(harvested), 1)
            row = harvested[0]
            self.assertEqual(row["year_started"], 1958)
            self.assertIs(row["glacier"], False)
            self.assertEqual(row["lift_ticket_currency"], "EUR")
            self.assertEqual(row["lift_ticket_raw"], "€25, adult day price")
            self.assertEqual(row["copy_status"], "insufficient_data")
            self.assertEqual(row["note_count"], 1)
            self.assertEqual(row["sources"]["year_started"]["kind"], "wikipedia")

    def test_http_api(self) -> None:
        row = fact_row(
            filters={
                "winterSportsId": "52067284",
                "name": "Camelback",
                "resort": "Camelback Mountain Resort",
                "state": "Pennsylvania",
                "tier": "medium",
                "night_skiing": True,
                "price_band": "$$",
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "facts.sqlite"
            conn = connect(db)
            replace_facts(conn, [row])
            conn.close()
            server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(db))
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                port = server.server_address[1]
                client = HTTPConnection("127.0.0.1", port, timeout=5)
                client.request("GET", "/resorts/facts?state=Pennsylvania&amenity=night_skiing&tier=medium")
                payload = json.loads(client.getresponse().read())
                self.assertEqual(payload["n"], 1)
                self.assertEqual(payload["resorts"][0]["winter_sports_id"], "52067284")
                client.request("GET", "/resorts/facts/stats?state=Pennsylvania")
                stats = json.loads(client.getresponse().read())
                self.assertEqual(stats["night_skiing"]["true"], 1)
            finally:
                server.shutdown()


if __name__ == "__main__":
    unittest.main()
