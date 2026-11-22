"""Build one comparable-facts row from filters.jsonl and key_points.jsonl.

Null means unknown. false / "no" only when a harvested source said the hill
does not have that thing. Article body is not stored here.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .config import KEY_POINTS_PATH, output_path
from .filters import (
    FILTER_COLUMNS,
    FILTERS_PATH,
    _MEGA_PASSES,
    _point_text,
    classify_source,
    parse_native_day_price,
    price_band,
    resort_passes,
)


FACTS_PATH = output_path("resort_facts", "facts.jsonl")

_TEXT_FIELDS = (
    "owner",
    "operator_group",
    "operator_status",
    "village_type",
    "visit_type",
    "famous_for",
    "on_hill_lodge",
    "drive",
    "parking",
    "season_open",
    "season_close",
    "sectors",
    "terrain",
)

_TICKET_RAW = (
    ("lift_ticket", "lift_ticket_raw"),
    ("weekend_ticket", "weekend_ticket_raw"),
    ("night_ticket", "night_ticket_raw"),
    ("season_pass", "season_pass_raw"),
)

_MEGA_FOR_ROUNDUP = frozenset({"Epic", "Ikon", "Indy", "Mountain Collective"})

_YEAR = re.compile(r"\b(1[89]\d{2}|20\d{2})\b")


def _point(key_points: dict[str, Any], field: str) -> dict[str, Any] | None:
    raw = key_points.get(field)
    return raw if isinstance(raw, dict) else None


def _text_or_none(value: str | None) -> str | None:
    text = (value or "").strip()
    return text or None


def _year(text: str | None) -> int | None:
    if not text:
        return None
    match = _YEAR.search(text)
    return int(match.group(1)) if match else None


def _tri_from_point(raw: dict[str, Any] | None) -> bool | None:
    if not raw:
        return None
    value = str(raw.get("value") or "").strip().lower()
    if value in {"yes", "true"}:
        return True
    if value in {"no", "false"}:
        return False
    return None


def _cite(raw: dict[str, Any] | None, official_host: str) -> dict[str, str] | None:
    if not raw:
        return None
    urls = [str(u) for u in (raw.get("sources") or []) if u]
    shown = _point_text(raw)
    if not shown and not urls:
        return None
    item = classify_source(urls, official_host)
    item["value_shown"] = shown
    return item


def _host_from_website(url: str | None) -> str:
    if not url:
        return ""
    match = re.search(r"https?://([^/]+)", url.lower())
    return match.group(1).removeprefix("www.") if match else ""


def _passes_from_records(filters: dict[str, Any], key_points: dict[str, Any]) -> list[str]:
    existing = filters.get("passes")
    if isinstance(existing, list):
        return [str(name) for name in existing if str(name).strip()]
    return resort_passes(key_points)


def fact_row(
    *,
    filters: dict[str, Any] | None = None,
    key_points_record: dict[str, Any] | None = None,
    copy_record: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Merge harvested records into one typed row. Needs winterSportsId."""
    filters = filters or {}
    kp_wrap = key_points_record or {}
    key_points = kp_wrap.get("key_points") if isinstance(kp_wrap.get("key_points"), dict) else {}
    if not key_points and isinstance(filters.get("key_points"), dict):
        key_points = filters["key_points"]
    copy_record = copy_record or {}

    winter_sports_id = str(
        filters.get("winterSportsId") or kp_wrap.get("winterSportsId") or copy_record.get("winterSportsId") or ""
    ).strip()
    if not winter_sports_id:
        return None

    website = _text_or_none(filters.get("website")) or _text_or_none(_point_text(key_points.get("website")))
    official_host = _host_from_website(website)
    passes = _passes_from_records(filters, key_points)
    mega = [name for name in passes if name in _MEGA_FOR_ROUNDUP]
    local = [name for name in passes if name not in _MEGA_PASSES and name not in _MEGA_FOR_ROUNDUP]
    if len(mega) >= 2 and not local:
        passes = []

    lift = _point(key_points, "lift_ticket")
    lift_raw = _text_or_none(_point_text(lift))
    parsed = parse_native_day_price(lift_raw or "") if lift_raw else None
    usd = parsed["usd"] if parsed else None
    band = filters.get("price_band")
    if band is None and usd is not None:
        band = price_band(float(usd))

    sources: dict[str, dict[str, str]] = {}
    if isinstance(filters.get("sources"), dict):
        for field, item in filters["sources"].items():
            if isinstance(item, dict) and (item.get("url") or item.get("kind")):
                sources[field] = {
                    "kind": str(item.get("kind") or "unknown"),
                    "url": str(item.get("url") or ""),
                    "value_shown": str((filters.get("evidence") or {}).get(field) or ""),
                }

    cited = _cite(key_points.get("website") if isinstance(key_points.get("website"), dict) else None, official_host)
    if website and cited:
        sources.setdefault("website", cited)
    if passes:
        pass_cite = _cite(_point(key_points, "pass_affiliation"), official_host)
        if pass_cite:
            sources.setdefault("passes", pass_cite)
    if lift_raw:
        ticket_cite = _cite(lift, official_host)
        if ticket_cite:
            sources.setdefault("lift_ticket_raw", ticket_cite)
            sources.setdefault("price_band", ticket_cite)

    for field in _TEXT_FIELDS:
        cited_field = _cite(_point(key_points, field), official_host)
        if cited_field:
            sources[field] = cited_field
    year_point = _point(key_points, "year_started")
    if year_point:
        cited_year = _cite(year_point, official_host)
        if cited_year:
            sources["year_started"] = cited_year

    amenities: dict[str, bool | None] = {}
    for column in FILTER_COLUMNS:
        if column in filters:
            amenities[column] = filters.get(column)
        else:
            key = "tubing" if column == "snowtubing" else "ski_school" if column == "ski_instructors" else column
            amenities[column] = _tri_from_point(_point(key_points, key))
        if amenities[column] is not None and column not in sources:
            point_key = "tubing" if column == "snowtubing" else "ski_school" if column == "ski_instructors" else column
            cited_amenity = _cite(_point(key_points, point_key), official_host)
            if cited_amenity:
                sources[column] = cited_amenity

    notes = copy_record.get("notes") or []
    note_count = len(notes) if isinstance(notes, list) else None
    note_chars = None
    if isinstance(notes, list) and notes:
        note_chars = sum(len(str(item.get("text") or "")) for item in notes if isinstance(item, dict))

    tickets: dict[str, str | None] = {}
    for src, dest in _TICKET_RAW:
        tickets[dest] = _text_or_none(_point_text(key_points.get(src)))
        cited_ticket = _cite(_point(key_points, src), official_host)
        if tickets[dest] and cited_ticket:
            sources.setdefault(dest, cited_ticket)

    row: dict[str, Any] = {
        "winter_sports_id": winter_sports_id,
        "page_id": _text_or_none(filters.get("pageId") or kp_wrap.get("pageId") or copy_record.get("pageId")),
        "name": _text_or_none(filters.get("resort") or kp_wrap.get("resort") or copy_record.get("resort_label")),
        "country": _text_or_none(filters.get("country") or kp_wrap.get("country") or copy_record.get("country")),
        "state": _text_or_none(filters.get("state") or kp_wrap.get("state") or copy_record.get("state")),
        "tier": _text_or_none(filters.get("tier") or kp_wrap.get("tier") or copy_record.get("tier")),
        "website": website,
        "passes": passes,
        "owner": _text_or_none(_point_text(key_points.get("owner"))),
        "operator_group": _text_or_none(_point_text(key_points.get("operator_group"))),
        "operator_status": _text_or_none(_point_text(key_points.get("operator_status"))),
        "year_started": _year(_point_text(key_points.get("year_started"))),
        "village_type": _text_or_none(_point_text(key_points.get("village_type"))),
        "visit_type": _text_or_none(_point_text(key_points.get("visit_type"))),
        "famous_for": _text_or_none(_point_text(key_points.get("famous_for"))),
        "on_hill_lodge": _text_or_none(_point_text(key_points.get("on_hill_lodge"))),
        "lift_ticket_raw": lift_raw,
        "lift_ticket_amount": parsed["amount"] if parsed else None,
        "lift_ticket_currency": parsed["currency"] if parsed else None,
        "lift_ticket_usd": usd,
        "price_band": band,
        **tickets,
        **amenities,
        "drive": _text_or_none(_point_text(key_points.get("drive"))),
        "parking": _text_or_none(_point_text(key_points.get("parking"))),
        "season_open": _text_or_none(_point_text(key_points.get("season_open"))),
        "season_close": _text_or_none(_point_text(key_points.get("season_close"))),
        "sectors": _text_or_none(_point_text(key_points.get("sectors"))),
        "terrain": _text_or_none(_point_text(key_points.get("terrain"))),
        "copy_status": _text_or_none(copy_record.get("status")),
        "note_count": note_count if copy_record else None,
        "note_chars": note_chars,
        "search_attempts": copy_record.get("search_attempts") if copy_record else None,
        "sources": sources,
    }
    return row


def _index_by_id(path: Path) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    if not path.is_file():
        return found
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        wid = str(item.get("winterSportsId") or "").strip()
        if wid:
            found[wid] = item
    return found


def load_copy_records(paths: list[Path]) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for path in paths:
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            wid = str(item.get("winterSportsId") or "").strip()
            if wid:
                found[wid] = item
    return found


def persist_result(result: dict[str, Any], *, filters: dict[str, Any] | None = None) -> None:
    """Upsert one harvested resort into local SQLite, and DynamoDB when RESORT_FACTS_PUSH=1."""
    import os

    from .fact_store import connect, upsert_fact

    row = fact_row(
        filters=filters,
        key_points_record=result if result.get("key_points") else None,
        copy_record=result,
    )
    if not row:
        return
    conn = connect()
    try:
        upsert_fact(conn, row)
        conn.commit()
    finally:
        conn.close()
    if os.environ.get("RESORT_FACTS_PUSH") == "1":
        from .facts_ddb import push_facts

        push_facts([row])


def harvest_fact_rows(
    *,
    filters_path: Path = FILTERS_PATH,
    key_points_path: Path = KEY_POINTS_PATH,
    copy_paths: list[Path] | None = None,
) -> list[dict[str, Any]]:
    filters = _index_by_id(filters_path)
    points = _index_by_id(key_points_path)
    copies = load_copy_records(copy_paths or [])
    ids = sorted(set(filters) | set(points) | set(copies))
    rows: list[dict[str, Any]] = []
    for wid in ids:
        row = fact_row(
            filters=filters.get(wid),
            key_points_record=points.get(wid),
            copy_record=copies.get(wid),
        )
        if row:
            rows.append(row)
    return rows
