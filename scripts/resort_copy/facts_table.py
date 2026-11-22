#!/usr/bin/env python3
"""Ingest harvested JSONL into SQLite and optionally atlas-ResortFacts in DynamoDB.

  python scripts/resort_copy/facts_table.py --ingest
  python scripts/resort_copy/facts_table.py --ingest --copy output/ollama_resort_copy/vermont.jsonl
  python scripts/resort_copy/facts_table.py --create-table
  python scripts/resort_copy/facts_table.py --ingest --push
  python scripts/resort_copy/facts_table.py --query --state Vermont --pass Ikon
  python scripts/resort_copy/facts_table.py --serve
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.config import KEY_POINTS_PATH  # noqa: E402
from resort_copy.fact_store import DB_PATH, connect, fact_stats, query_facts, replace_facts  # noqa: E402
from resort_copy.facts import harvest_fact_rows  # noqa: E402
from resort_copy.facts_api import serve  # noqa: E402
from resort_copy.facts_ddb import TABLE_NAME, WIKI_COLUMNS, apply_user_edit, ensure_table, push_facts  # noqa: E402
from resort_copy.filters import FILTERS_PATH, FILTER_COLUMNS  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--filters", type=Path, default=FILTERS_PATH)
    parser.add_argument("--key-points", type=Path, default=KEY_POINTS_PATH)
    parser.add_argument("--copy", type=Path, nargs="*", default=[], help="Copy JSONL for status/note counts")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--create-table", action="store_true", help="Create atlas-ResortFacts if it is missing")
    parser.add_argument("--push", action="store_true", help="Harvest-write rows into DynamoDB (skips wiki locks)")
    parser.add_argument(
        "--set",
        nargs=3,
        metavar=("WINTER_SPORTS_ID", "COLUMN", "VALUE"),
        help="Wiki/user edit: true, false, clear, or a sourced string. Locks the cell.",
    )
    parser.add_argument("--query", action="store_true")
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--country")
    parser.add_argument("--state")
    parser.add_argument("--pass-name", dest="pass_name")
    parser.add_argument("--amenity")
    parser.add_argument("--price-band")
    parser.add_argument("--tier")
    args = parser.parse_args()

    rows: list | None = None
    if args.ingest or args.push:
        rows = harvest_fact_rows(
            filters_path=args.filters,
            key_points_path=args.key_points,
            copy_paths=list(args.copy),
        )
    if args.ingest and rows is not None:
        conn = connect(args.db)
        n = replace_facts(conn, rows)
        conn.close()
        print(f"upserted {n} resorts -> {args.db}", file=sys.stderr)

    if args.create_table or args.push or args.set:
        client = __import__("boto3").client("dynamodb")
        if args.create_table:
            ensure_table(client, TABLE_NAME)
            print(f"table {TABLE_NAME}", file=sys.stderr)
        if args.set:
            resort_id, column, raw = args.set
            column = column.replace("-", "_")
            if column not in WIKI_COLUMNS:
                raise SystemExit(f"unknown fact field {column}")
            if raw == "clear":
                value = None
            elif column in FILTER_COLUMNS:
                if raw not in {"true", "false"}:
                    raise SystemExit("--set for an amenity must be true, false, or clear")
                value = raw == "true"
            elif column == "passes":
                value = [part.strip() for part in raw.split(",") if part.strip()]
            elif column in {"year_started", "lift_ticket_amount", "lift_ticket_usd"}:
                value = raw
            else:
                value = raw
            apply_user_edit(client, winter_sports_id=resort_id, column=column, value=value, table_name=TABLE_NAME)
            print(f"user {resort_id} {column}={raw}", file=sys.stderr)
        if args.push:
            written = push_facts(rows or [], table_name=TABLE_NAME, client=client)
            print(f"pushed {written} resorts to {TABLE_NAME}", file=sys.stderr)

    if args.query or args.stats:
        conn = connect(args.db)
        rows = query_facts(
            conn,
            country=args.country,
            state=args.state,
            pass_name=args.pass_name,
            amenity=args.amenity,
            price_band=args.price_band,
            tier=args.tier,
        )
        conn.close()
        if args.stats:
            print(json.dumps(fact_stats(rows), ensure_ascii=False, indent=2))
        if args.query:
            print(json.dumps(rows, ensure_ascii=False, indent=2))

    if args.serve:
        serve(host=args.host, port=args.port, db_path=args.db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
