"""Wiki locks survive a harvest. python scripts/resort_copy/test_facts_ddb.py"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.facts import fact_row  # noqa: E402
from resort_copy.facts_ddb import (  # noqa: E402
    apply_ai_row,
    apply_user_edit,
    create_table_request,
    push_facts,
    to_item,
)


class _Locked(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class _Client:
    def __init__(self, locked: set[str] | None = None) -> None:
        self.locked = locked or set()
        self.calls: list[dict] = []

    def update_item(self, **kwargs: object) -> None:
        expr = str(kwargs.get("ConditionExpression") or "")
        if "attribute_not_exists(#lock)" in expr:
            names = kwargs["ExpressionAttributeNames"]
            column = names["#v"]
            if column in self.locked:
                raise _Locked()
        self.calls.append(kwargs)


class FactsDdbTests(unittest.TestCase):
    def test_table_matches_wiki_key_and_skips_osm_counts(self) -> None:
        request = create_table_request()
        self.assertEqual(request["TableName"], "atlas-ResortFacts")
        self.assertEqual(request["KeySchema"][0]["AttributeName"], "winterSportsId")
        self.assertEqual(request["BillingMode"], "PAY_PER_REQUEST")
        names = {idx["IndexName"] for idx in request["GlobalSecondaryIndexes"]}
        self.assertEqual(names, {"pageId-index", "country-index", "state-index"})
        self.assertNotIn("downhillTrails", str(request))

    def test_unknown_is_omitted_false_is_stored(self) -> None:
        row = fact_row(
            filters={
                "winterSportsId": "52067284",
                "pageId": "camelback-mountain-resort-pennsylvania",
                "resort": "Camelback Mountain Resort",
                "state": "Pennsylvania",
                "night_skiing": True,
                "snowtubing": False,
                "childcare": None,
                "passes": ["Camelbeach Season Passes"],
            },
            key_points_record={
                "winterSportsId": "52067284",
                "key_points": {"lift_ticket": {"value": "$34.00 CAD (day)"}},
            },
        )
        item = to_item(row, stamp="2026-11-22T00:00:00+00:00")
        self.assertEqual(item["winterSportsId"], {"S": "52067284"})
        self.assertEqual(item["night_skiing"], {"BOOL": True})
        self.assertEqual(item["snowtubing"], {"BOOL": False})
        self.assertNotIn("childcare", item)
        self.assertEqual(item["lift_ticket_raw"], {"S": "$34.00 CAD (day)"})
        self.assertEqual(item["passes"], {"SS": ["Camelbeach Season Passes"]})

    def test_harvest_skips_a_wiki_locked_cell(self) -> None:
        client = _Client(locked={"night_skiing"})
        outcome = apply_ai_row(
            client,
            {
                "winter_sports_id": "1094522661",
                "page_id": "jay-peak-resort-vermont",
                "night_skiing": False,
                "ski_instructors": True,
            },
        )
        self.assertEqual(outcome["night_skiing"], "kept_user_value")
        self.assertEqual(outcome["ski_instructors"], "written")

    def test_wiki_edit_locks_the_cell(self) -> None:
        client = _Client()
        apply_user_edit(
            client,
            winter_sports_id="1094522661",
            column="night_skiing",
            value=True,
            note="checked the hill",
        )
        call = client.calls[-1]
        self.assertEqual(call["ExpressionAttributeValues"][":src"], {"S": "user"})
        self.assertEqual(call["ExpressionAttributeValues"][":locked"], {"BOOL": True})
        self.assertEqual(call["ExpressionAttributeValues"][":v"], {"BOOL": True})

    def test_push_uses_update_not_put(self) -> None:
        client = _Client()
        n = push_facts([{"winter_sports_id": "1", "night_skiing": True}], client=client)
        self.assertEqual(n, 1)
        self.assertTrue(all("Item" not in call for call in client.calls))
        self.assertEqual(client.calls[0]["Key"]["winterSportsId"], {"S": "1"})


if __name__ == "__main__":
    unittest.main()
