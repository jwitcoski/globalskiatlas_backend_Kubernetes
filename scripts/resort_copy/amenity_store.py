"""DynamoDB table for resort amenities.

Partition key is winterSportsId. pageId is a secondary index so the wiki can
look a resort up. Trail count, vertical drop, and lift count stay in the OSM
and DEM table.

An AI update writes only cells it can support, and only when that cell is not
locked. A user update sets the cell and locks it, so the next AI pass leaves
it alone.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from .filters import FILTER_COLUMNS, PROFILE_COLUMNS

TABLE_NAME = os.environ.get("RESORT_AMENITY_TABLE", "atlas-ResortAmenities")
PAGE_INDEX = "pageId-index"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _locked_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    return response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


def create_table_request(table_name: str = TABLE_NAME) -> dict[str, Any]:
    """On-demand table. No trail, vertical, or lift attributes."""
    return {
        "TableName": table_name,
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            {"AttributeName": "winterSportsId", "AttributeType": "S"},
            {"AttributeName": "pageId", "AttributeType": "S"},
        ],
        "KeySchema": [{"AttributeName": "winterSportsId", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": PAGE_INDEX,
                "KeySchema": [{"AttributeName": "pageId", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    }


def ensure_table(client: Any, table_name: str = TABLE_NAME) -> None:
    existing = client.list_tables().get("TableNames") or []
    if table_name in existing:
        return
    client.create_table(**create_table_request(table_name))


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


def _cited(row: dict[str, Any], column: str) -> tuple[str, str]:
    raw = row.get("sources")
    item = raw.get(column) if isinstance(raw, dict) else None
    if not isinstance(item, dict):
        return "unknown", ""
    return str(item.get("kind") or "unknown"), str(item.get("url") or "")[:500]


def apply_ai_row(client: Any, row: dict[str, Any], *, table_name: str = TABLE_NAME) -> dict[str, str]:
    """Write AI values. Returns written or skipped for each column that had a value."""
    resort_id = str(row.get("winterSportsId") or "").strip()
    if not resort_id:
        raise ValueError("winterSportsId is required")
    outcome: dict[str, str] = {}
    stamp = _now()
    _touch_identity(client, row, table_name=table_name, stamp=stamp)
    for column in FILTER_COLUMNS:
        value = row.get(column)
        if value is None:
            continue
        evidence = ""
        raw_evidence = row.get("evidence")
        if isinstance(raw_evidence, dict):
            evidence = str(raw_evidence.get(column) or "")
        kind, url = _cited(row, column)
        try:
            client.update_item(
                TableName=table_name,
                Key={"winterSportsId": {"S": resort_id}},
                UpdateExpression="SET #v = :v, #src = :src, #kind = :kind, #url = :url, #ev = :ev, #at = :at",
                ConditionExpression="attribute_not_exists(#lock) OR #lock = :unlocked",
                ExpressionAttributeNames=_names(column),
                ExpressionAttributeValues={
                    ":v": {"BOOL": bool(value)},
                    ":src": {"S": "ai"},
                    ":kind": {"S": kind},
                    ":url": {"S": url},
                    ":ev": {"S": evidence[:500]},
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
    for column in PROFILE_COLUMNS:
        value = row.get(column)
        if value is None or value == "" or value == []:
            continue
        shown = ", ".join(value) if isinstance(value, list) else str(value)
        evidence = ""
        raw_evidence = row.get("evidence")
        if isinstance(raw_evidence, dict):
            evidence = str(raw_evidence.get(column) or "")
        kind, url = _cited(row, column)
        try:
            client.update_item(
                TableName=table_name,
                Key={"winterSportsId": {"S": resort_id}},
                UpdateExpression="SET #v = :v, #src = :src, #kind = :kind, #url = :url, #ev = :ev, #at = :at",
                ConditionExpression="attribute_not_exists(#lock) OR #lock = :unlocked",
                ExpressionAttributeNames=_names(column),
                ExpressionAttributeValues={
                    ":v": {"S": shown[:500]},
                    ":src": {"S": "ai"},
                    ":kind": {"S": kind},
                    ":url": {"S": url},
                    ":ev": {"S": evidence[:500]},
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
    value: bool | str | None,
    table_name: str = TABLE_NAME,
    note: str = "",
) -> None:
    """Set one cell and lock it. None clears the cell and the lock."""
    if column not in FILTER_COLUMNS and column not in PROFILE_COLUMNS:
        raise ValueError(f"unknown amenity {column}")
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
    client.update_item(
        TableName=table_name,
        Key={"winterSportsId": {"S": resort_id}},
        UpdateExpression="SET #v = :v, #src = :src, #kind = :kind, #url = :url, #lock = :locked, #ev = :ev, #at = :at, updatedAt = :at",
        ExpressionAttributeNames=names,
        ExpressionAttributeValues={
            ":v": {"BOOL": value} if column in FILTER_COLUMNS else {"S": str(value)[:500]},
            ":src": {"S": "user"},
            ":kind": {"S": "user"},
            ":url": {"S": ""},
            ":locked": {"BOOL": True},
            ":ev": {"S": note[:500]},
            ":at": {"S": _now()},
        },
    )


def _touch_identity(client: Any, row: dict[str, Any], *, table_name: str, stamp: str) -> None:
    values: dict[str, dict[str, str]] = {
        ":resort": {"S": str(row.get("resort") or "")},
        ":updated": {"S": stamp},
    }
    sets = ["#resort = :resort", "#updated = :updated"]
    attr_names = {"#resort": "resort", "#updated": "updatedAt"}
    for key, attr in (("pageId", "pageId"), ("country", "country"), ("state", "state"), ("tier", "tier")):
        text = str(row.get(key) or "").strip()
        if not text:
            continue
        token = f"#{key}"
        placeholder = f":{key}"
        attr_names[token] = attr
        values[placeholder] = {"S": text}
        sets.append(f"{token} = {placeholder}")
    client.update_item(
        TableName=table_name,
        Key={"winterSportsId": {"S": str(row.get("winterSportsId"))}},
        UpdateExpression="SET " + ", ".join(sets),
        ExpressionAttributeNames=attr_names,
        ExpressionAttributeValues=values,
    )


def push_rows(rows: list[dict[str, Any]], *, table_name: str = TABLE_NAME, client: Any = None) -> list[dict[str, str]]:
    """Push local filter rows. Missing winterSportsId rows are skipped."""
    if client is None:
        client = __import__("boto3").client("dynamodb")
    outcomes: list[dict[str, str]] = []
    for row in rows:
        if not str(row.get("winterSportsId") or "").strip():
            continue
        outcomes.append(apply_ai_row(client, row, table_name=table_name))
    return outcomes
