"""SQLite table of harvested resort facts. One row per winter_sports_id."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import output_path
from .filters import FILTER_COLUMNS
from .pass_chips import PASS_COLUMNS


DB_PATH = output_path("resort_facts", "resort_facts.sqlite")

AMENITY_COLUMNS = FILTER_COLUMNS

SCHEMA = """
CREATE TABLE IF NOT EXISTS resort_facts (
    winter_sports_id TEXT PRIMARY KEY,
    page_id TEXT,
    name TEXT,
    country TEXT,
    state TEXT,
    tier TEXT,
    updated_at TEXT NOT NULL,
    website TEXT,
    passes TEXT NOT NULL DEFAULT '[]',
    other_passes TEXT NOT NULL DEFAULT '[]',
    owner TEXT,
    operator_group TEXT,
    operator_status TEXT,
    year_started INTEGER,
    village_type TEXT,
    visit_type TEXT,
    famous_for TEXT,
    on_hill_lodge TEXT,
    lift_ticket_raw TEXT,
    lift_ticket_amount REAL,
    lift_ticket_currency TEXT,
    lift_ticket_usd REAL,
    price_band TEXT,
    weekend_ticket_raw TEXT,
    night_ticket_raw TEXT,
    season_pass_raw TEXT,
    snowtubing INTEGER,
    ski_rentals INTEGER,
    ski_instructors INTEGER,
    night_skiing INTEGER,
    terrain_park INTEGER,
    nordic INTEGER,
    childcare INTEGER,
    snowmaking INTEGER,
    glacier INTEGER,
    drive TEXT,
    parking TEXT,
    season_open TEXT,
    season_close TEXT,
    sectors TEXT,
    terrain TEXT,
    copy_status TEXT,
    note_count INTEGER,
    note_chars INTEGER,
    search_attempts INTEGER,
    sources TEXT NOT NULL DEFAULT '{}'
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute(SCHEMA)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(resort_facts)")}
    for column in PASS_COLUMNS:
        if column not in existing:
            conn.execute(f"ALTER TABLE resort_facts ADD COLUMN {column} INTEGER")
    if "other_passes" not in existing:
        conn.execute("ALTER TABLE resort_facts ADD COLUMN other_passes TEXT NOT NULL DEFAULT '[]'")
    return conn


def _tri(value: Any) -> int | None:
    if value is True:
        return 1
    if value is False:
        return 0
    return None


def _from_tri(value: Any) -> bool | None:
    if value in (1, True):
        return True
    if value in (0, False):
        return False
    return None


def upsert_fact(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    payload = dict(row)
    payload["updated_at"] = _now()
    payload["passes"] = json.dumps(row.get("passes") or [], ensure_ascii=False)
    payload["other_passes"] = json.dumps(row.get("other_passes") or [], ensure_ascii=False)
    payload["sources"] = json.dumps(row.get("sources") or {}, ensure_ascii=False)
    for column in (*AMENITY_COLUMNS, *PASS_COLUMNS):
        payload[column] = _tri(row.get(column))
    columns = [
        "winter_sports_id",
        "page_id",
        "name",
        "country",
        "state",
        "tier",
        "updated_at",
        "website",
        "passes",
        "other_passes",
        "owner",
        "operator_group",
        "operator_status",
        "year_started",
        "village_type",
        "visit_type",
        "famous_for",
        "on_hill_lodge",
        "lift_ticket_raw",
        "lift_ticket_amount",
        "lift_ticket_currency",
        "lift_ticket_usd",
        "price_band",
        "weekend_ticket_raw",
        "night_ticket_raw",
        "season_pass_raw",
        *AMENITY_COLUMNS,
        *PASS_COLUMNS,
        "drive",
        "parking",
        "season_open",
        "season_close",
        "sectors",
        "terrain",
        "copy_status",
        "note_count",
        "note_chars",
        "search_attempts",
        "sources",
    ]
    placeholders = ", ".join(":" + name for name in columns)
    assignments = ", ".join(f"{name} = excluded.{name}" for name in columns if name != "winter_sports_id")
    conn.execute(
        f"INSERT INTO resort_facts ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(winter_sports_id) DO UPDATE SET {assignments}",
        {name: payload.get(name) for name in columns},
    )


def replace_facts(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    conn.execute("DELETE FROM resort_facts")
    for row in rows:
        upsert_fact(conn, row)
    conn.commit()
    return len(rows)


def _row_out(raw: sqlite3.Row) -> dict[str, Any]:
    item = dict(raw)
    item["passes"] = json.loads(item.get("passes") or "[]")
    item["other_passes"] = json.loads(item.get("other_passes") or "[]")
    item["sources"] = json.loads(item.get("sources") or "{}")
    for column in (*AMENITY_COLUMNS, *PASS_COLUMNS):
        item[column] = _from_tri(item.get(column))
    return item


def query_facts(
    conn: sqlite3.Connection,
    *,
    country: str | None = None,
    state: str | None = None,
    pass_name: str | None = None,
    amenity: str | None = None,
    price_band: str | None = None,
    tier: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    values: list[Any] = []
    if country:
        clauses.append("country = ?")
        values.append(country)
    if state:
        clauses.append("state = ?")
        values.append(state)
    if pass_name:
        from .pass_chips import PASS_LABELS

        key = pass_name.replace("-", "_").lower()
        by_label = {label.lower(): col for col, label in PASS_LABELS.items()}
        column = key if key in PASS_COLUMNS else by_label.get(pass_name.lower())
        if column:
            clauses.append(
                f"({column} = 1 OR EXISTS (SELECT 1 FROM json_each(resort_facts.passes) WHERE value = ?))"
            )
            values.append(PASS_LABELS[column])
        else:
            clauses.append("EXISTS (SELECT 1 FROM json_each(resort_facts.passes) WHERE value = ?)")
            values.append(pass_name)
    if amenity:
        if amenity not in AMENITY_COLUMNS:
            raise ValueError(f"unknown amenity {amenity}")
        clauses.append(f"{amenity} = 1")
    if price_band:
        clauses.append("price_band = ?")
        values.append(price_band)
    if tier:
        clauses.append("tier = ?")
        values.append(tier)
    sql = "SELECT * FROM resort_facts"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY name"
    return [_row_out(row) for row in conn.execute(sql, values)]


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def amenity_counts(rows: list[dict[str, Any]], column: str) -> dict[str, int]:
    counts = {"true": 0, "false": 0, "unknown": 0}
    for row in rows:
        value = row.get(column)
        if value is True:
            counts["true"] += 1
        elif value is False:
            counts["false"] += 1
        else:
            counts["unknown"] += 1
    return counts


def fact_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregates that never treat a null amenity as false."""
    by_country: dict[str, list[float]] = {}
    insufficient: dict[str, int] = {}
    ikon = local = both = 0
    mega = {"Epic", "Ikon", "Indy", "Mountain Collective", "Power Pass", "Nord Pass"}
    for row in rows:
        country = row.get("country") or "unknown"
        usd = row.get("lift_ticket_usd")
        if usd is not None:
            by_country.setdefault(country, []).append(float(usd))
        if row.get("copy_status") == "insufficient_data":
            state = row.get("state") or "unknown"
            insufficient[state] = insufficient.get(state, 0) + 1
        names = set(row.get("passes") or [])
        has_ikon = "Ikon" in names
        has_local = bool(names - mega)
        if has_ikon and has_local:
            both += 1
        elif has_ikon:
            ikon += 1
        elif has_local:
            local += 1
    night = amenity_counts(rows, "night_skiing")
    known_night = night["true"] + night["false"]
    return {
        "n": len(rows),
        "night_skiing": night,
        "night_skiing_known_share_true": (night["true"] / known_night) if known_night else None,
        "median_lift_ticket_usd_by_country": {
            country: _median(values) for country, values in sorted(by_country.items())
        },
        "insufficient_data_by_state": insufficient,
        "passes": {"ikon_only": ikon, "local_only": local, "ikon_and_local": both},
    }
