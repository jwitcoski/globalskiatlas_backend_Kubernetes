"""DynamoDB table for harvested resort facts.

Partition key is winterSportsId, same as atlas-WikiPages and atlas-ResortAmenities.
pageId, country, and state are sparse GSIs. Trail count, vertical, and lift
count stay on the OSM table.

The wiki can change a cell and lock it. A harvest writes only unlocked cells,
and only when it has a sourced value. Unknown stays omitted. false is BOOL false.
RESORT_FACTS_PUSH=1 writes from the copy pipeline.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .filters import FILTER_COLUMNS


TABLE_NAME = os.environ.get("RESORT_FACTS_TABLE", "atlas-ResortFacts")
PAGE_INDEX = "pageId-index"
COUNTRY_INDEX = "country-index"
STATE_INDEX = "state-index"

IDENTITY_FIELDS = ("name", "country", "state", "tier")
OPS_FIELDS = ("copy_status", "note_count", "note_chars", "search_attempts")

WIKI_TEXT = (
    "website",
    "owner",
    "operator_group",
    "operator_status",
    "village_type",
    "visit_type",
    "famous_for",
    "on_hill_lodge",
    "lift_ticket_raw",
    "lift_ticket_currency",
    "price_band",
    "weekend_ticket_raw",
    "night_ticket_raw",
    "season_pass_raw",
    "drive",
    "parking",
    "season_open",
    "season_close",
    "sectors",
    "terrain",
)
WIKI_NUMBERS = ("year_started", "lift_ticket_amount", "lift_ticket_usd")
WIKI_LISTS = ("passes",)
WIKI_COLUMNS = (*FILTER_COLUMNS, *WIKI_TEXT, *WIKI_NUMBERS, *WIKI_LISTS)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _locked_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    return response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


def create_table_request(table_name: str = TABLE_NAME) -> dict[str, Any]:
    def gsi(name: str, key: str) -> dict[str, Any]:
        return {
            "IndexName": name,
            "KeySchema": [{"AttributeName": key, "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }

    return {
        "TableName": table_name,
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            {"AttributeName": "winterSportsId", "AttributeType": "S"},
            {"AttributeName": "pageId", "AttributeType": "S"},
            {"AttributeName": "country", "AttributeType": "S"},
            {"AttributeName": "state", "AttributeType": "S"},
        ],
        "KeySchema": [{"AttributeName": "winterSportsId", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [
            gsi(PAGE_INDEX, "pageId"),
            gsi(COUNTRY_INDEX, "country"),
            gsi(STATE_INDEX, "state"),
        ],
    }


def ensure_table(client: Any, table_name: str = TABLE_NAME) -> None:
    existing = client.list_tables().get("TableNames") or []
    if table_name in existing:
        return
    client.create_table(**create_table_request(table_name))


def _s(value: Any) -> dict[str, str] | None:
    text = str(value or "").strip()
    return {"S": text} if text else None


def _n(value: Any) -> dict[str, str] | None:
    if value is None or value == "":
        return None
    return {"N": str(Decimal(str(value)))}


def _names(column: str) -> dict[str, str]:
    return {
        "#v": column,
        "#src": f"{column}Source",
        "#kind": f"{column}SourceKind",
        "#url": f"{column}SourceUrl",
        "#lock": f"{column}Locked",
        "#ev": f"{column}Evidence",
        "#at": f"{column}UpdatedAt",
    }


def _cited(row: dict[str, Any], column: str) -> tuple[str, str, str]:
    raw = row.get("sources")
    item = raw.get(column) if isinstance(raw, dict) else None
    if not isinstance(item, dict):
        return "unknown", "", ""
    return (
        str(item.get("kind") or "unknown"),
        str(item.get("url") or "")[:500],
        str(item.get("value_shown") or "")[:500],
    )


def _ddb_value(column: str, value: Any) -> dict[str, Any] | None:
    if column in FILTER_COLUMNS:
        if value is True:
            return {"BOOL": True}
        if value is False:
            return {"BOOL": False}
        return None
    if column in WIKI_NUMBERS or column in ("note_count", "note_chars", "search_attempts"):
        return _n(value)
    if column == "passes":
        names = [str(name).strip() for name in (value or []) if str(name).strip()]
        if not names:
            return None
        return {"SS": sorted(set(names))}
    return _s(value)


def _resort_id(row: dict[str, Any]) -> str:
    return str(row.get("winter_sports_id") or row.get("winterSportsId") or "").strip()


def apply_ai_row(client: Any, row: dict[str, Any], *, table_name: str = TABLE_NAME) -> dict[str, str]:
    """Write harvested values. Locked wiki cells are left alone."""
    resort_id = _resort_id(row)
    if not resort_id:
        raise ValueError("winterSportsId is required")
    outcome: dict[str, str] = {}
    stamp = _now()
    _touch_identity(client, row, table_name=table_name, stamp=stamp)
    _write_ops(client, row, table_name=table_name, stamp=stamp)
    for column in WIKI_COLUMNS:
        encoded = _ddb_value(column, row.get(column))
        if encoded is None:
            continue
        kind, url, shown = _cited(row, column)
        try:
            client.update_item(
                TableName=table_name,
                Key={"winterSportsId": {"S": resort_id}},
                UpdateExpression="SET #v = :v, #src = :src, #kind = :kind, #url = :url, #ev = :ev, #at = :at",
                ConditionExpression="attribute_not_exists(#lock) OR #lock = :unlocked",
                ExpressionAttributeNames=_names(column),
                ExpressionAttributeValues={
                    ":v": encoded,
                    ":src": {"S": "ai"},
                    ":kind": {"S": kind},
                    ":url": {"S": url},
                    ":ev": {"S": shown},
                    ":at": {"S": stamp},
                    ":unlocked": {"BOOL": False},
                },
            )
            outcome[column] = "written"
        except Exception as exc:
            if _locked_error(exc):
                outcome[column] = "kept_user_value"
                continue
            raise
    return outcome


def apply_user_edit(
    client: Any,
    *,
    winter_sports_id: str,
    column: str,
    value: Any,
    table_name: str = TABLE_NAME,
    note: str = "",
) -> None:
    """Wiki/user write: set the cell and lock it. None clears the cell and the lock."""
    if column not in WIKI_COLUMNS:
        raise ValueError(f"unknown fact field {column}")
    resort_id = winter_sports_id.strip()
    if not resort_id:
        raise ValueError("winterSportsId is required")
    names = _names(column)
    if value is None:
        client.update_item(
            TableName=table_name,
            Key={"winterSportsId": {"S": resort_id}},
            UpdateExpression="REMOVE #v, #src, #kind, #url, #lock, #ev, #at",
            ExpressionAttributeNames=names,
        )
        return
    encoded = _ddb_value(column, value)
    if encoded is None:
        raise ValueError(f"empty value for {column}")
    stamp = _now()
    client.update_item(
        TableName=table_name,
        Key={"winterSportsId": {"S": resort_id}},
        UpdateExpression="SET #v = :v, #src = :src, #kind = :kind, #url = :url, #lock = :locked, #ev = :ev, #at = :at, updatedAt = :at",
        ExpressionAttributeNames=names,
        ExpressionAttributeValues={
            ":v": encoded,
            ":src": {"S": "user"},
            ":kind": {"S": "user"},
            ":url": {"S": ""},
            ":locked": {"BOOL": True},
            ":ev": {"S": note[:500]},
            ":at": {"S": stamp},
        },
    )


def _touch_identity(client: Any, row: dict[str, Any], *, table_name: str, stamp: str) -> None:
    values: dict[str, dict[str, str]] = {":updated": {"S": stamp}}
    sets = ["#updated = :updated"]
    attr_names = {"#updated": "updatedAt"}
    page_id = str(row.get("page_id") or row.get("pageId") or "").strip()
    if page_id:
        attr_names["#pageId"] = "pageId"
        values[":pageId"] = {"S": page_id}
        sets.append("#pageId = :pageId")
    for key in IDENTITY_FIELDS:
        text = str(row.get(key) or "").strip()
        if not text:
            continue
        attr_names[f"#{key}"] = key
        values[f":{key}"] = {"S": text}
        sets.append(f"#{key} = :{key}")
    client.update_item(
        TableName=table_name,
        Key={"winterSportsId": {"S": _resort_id(row)}},
        UpdateExpression="SET " + ", ".join(sets),
        ExpressionAttributeNames=attr_names,
        ExpressionAttributeValues=values,
    )


def _write_ops(client: Any, row: dict[str, Any], *, table_name: str, stamp: str) -> None:
    values: dict[str, dict[str, str]] = {":at": {"S": stamp}}
    sets = ["updatedAt = :at"]
    names: dict[str, str] = {}
    for field in OPS_FIELDS:
        if field in ("note_count", "note_chars", "search_attempts"):
            encoded = _n(row.get(field))
        else:
            encoded = _s(row.get(field))
        if not encoded:
            continue
        names[f"#{field}"] = field
        values[f":{field}"] = encoded
        sets.append(f"#{field} = :{field}")
    if len(sets) == 1:
        return
    client.update_item(
        TableName=table_name,
        Key={"winterSportsId": {"S": _resort_id(row)}},
        UpdateExpression="SET " + ", ".join(sets),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def to_item(row: dict[str, Any], *, stamp: str | None = None) -> dict[str, Any]:
    """Encoding used in tests. Live writes go through apply_ai_row so locks survive."""
    winter_id = _resort_id(row)
    if not winter_id:
        raise ValueError("winterSportsId is required")
    item: dict[str, Any] = {
        "winterSportsId": {"S": winter_id},
        "updatedAt": {"S": stamp or _now()},
    }
    page_id = _s(row.get("page_id") or row.get("pageId"))
    if page_id:
        item["pageId"] = page_id
    for field in (*IDENTITY_FIELDS, *WIKI_TEXT, "copy_status"):
        encoded = _s(row.get(field))
        if encoded:
            item[field] = encoded
    for field in (*WIKI_NUMBERS, "note_count", "note_chars", "search_attempts"):
        encoded = _n(row.get(field))
        if encoded:
            item[field] = encoded
    for column in FILTER_COLUMNS:
        encoded = _ddb_value(column, row.get(column))
        if encoded:
            item[column] = encoded
    encoded_passes = _ddb_value("passes", row.get("passes"))
    if encoded_passes:
        item["passes"] = encoded_passes
    return item


def push_facts(
    rows: list[dict[str, Any]],
    *,
    table_name: str = TABLE_NAME,
    client: Any = None,
) -> int:
    """Harvest write. Missing ids are skipped. Locked wiki cells are kept."""
    if client is None:
        client = __import__("boto3").client("dynamodb")
    written = 0
    for row in rows:
        if not _resort_id(row):
            continue
        apply_ai_row(client, row, table_name=table_name)
        written += 1
    return written
