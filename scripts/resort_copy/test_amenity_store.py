"""AI updates skip a cell a user has locked. Run: python scripts/resort_copy/test_amenity_store.py"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.amenity_store import apply_ai_row, apply_user_edit, create_table_request  # noqa: E402


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


class AmenityStoreTests(unittest.TestCase):
    def test_table_key_is_the_resort_id(self) -> None:
        request = create_table_request()
        self.assertEqual(request["KeySchema"][0]["AttributeName"], "winterSportsId")
        self.assertEqual(request["BillingMode"], "PAY_PER_REQUEST")
        self.assertNotIn("downhillTrails", str(request))

    def test_ai_writes_an_open_cell_and_skips_a_locked_one(self) -> None:
        client = _Client(locked={"snowtubing"})
        outcome = apply_ai_row(
            client,
            {
                "winterSportsId": "way/1",
                "resort": "Fixture Hill",
                "pageId": "fixture-hill-victoria",
                "snowtubing": True,
                "ski_rentals": True,
                "evidence": {"snowtubing": "snow tubing hill", "ski_rentals": "rental shop"},
            },
        )
        self.assertEqual(outcome["snowtubing"], "kept_user_value")
        self.assertEqual(outcome["ski_rentals"], "written")

    def test_user_edit_locks_the_cell(self) -> None:
        client = _Client()
        apply_user_edit(client, winter_sports_id="way/1", column="ski_instructors", value=True, note="checked the site")
        call = client.calls[-1]
        self.assertEqual(call["ExpressionAttributeValues"][":src"], {"S": "user"})
        self.assertEqual(call["ExpressionAttributeValues"][":kind"], {"S": "user"})
        self.assertEqual(call["ExpressionAttributeValues"][":locked"], {"BOOL": True})
        self.assertEqual(call["ExpressionAttributeValues"][":v"], {"BOOL": True})


if __name__ == "__main__":
    unittest.main()
