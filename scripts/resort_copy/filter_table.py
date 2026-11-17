#!/usr/bin/env python3
"""Build and query the resort amenity filter table.

  python scripts/resort_copy/filter_table.py --from-copy output/ollama_resort_copy/test50.jsonl --csv output/resort_filters/filters.csv
  python scripts/resort_copy/filter_table.py --snowtubing --ski-instructors
  python scripts/resort_copy/filter_table.py --snowtubing --ski-rentals --any
  python scripts/resort_copy/filter_table.py --create-table
  python scripts/resort_copy/filter_table.py --push
  python scripts/resort_copy/filter_table.py --set WINTER_ID snowtubing true
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.amenity_store import (  # noqa: E402
    TABLE_NAME,
    apply_user_edit,
    ensure_table,
    push_rows,
)
from resort_copy.filters import (  # noqa: E402
    FILTERS_PATH,
    FILTER_COLUMNS,
    load_filter_rows,
    rows_from_copy_jsonl,
    select_rows,
    upsert_filter_row,
    write_filter_csv,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jsonl", type=Path, default=FILTERS_PATH, help="Filter table JSONL")
    parser.add_argument("--from-copy", type=Path, default=None, help="Rebuild the table from a resort-copy JSONL")
    parser.add_argument("--csv", type=Path, default=None, help="Also write a CSV with true, false, or blank cells")
    parser.add_argument("--any", action="store_true", help="Match any selected amenity instead of all of them")
    parser.add_argument("--push", action="store_true", help="Write the local rows into the DynamoDB amenity table")
    parser.add_argument("--create-table", action="store_true", help="Create atlas-ResortAmenities if it is missing")
    parser.add_argument(
        "--set",
        nargs=3,
        metavar=("WINTER_SPORTS_ID", "COLUMN", "VALUE"),
        help="User edit: true, false, or clear. Locks the cell against the AI.",
    )
    for column in FILTER_COLUMNS:
        parser.add_argument(f"--{column.replace('_', '-')}", action="store_true")
    args = parser.parse_args()

    if args.create_table or args.push or args.set:
        client = __import__("boto3").client("dynamodb")
        if args.create_table:
            ensure_table(client, TABLE_NAME)
            print(f"table {TABLE_NAME}", file=sys.stderr)
        if args.set:
            resort_id, column, raw = args.set
            if column.replace("-", "_") in FILTER_COLUMNS:
                if raw not in {"true", "false", "clear"}:
                    raise SystemExit("--set for an amenity must be true, false, or clear")
                value = None if raw == "clear" else raw == "true"
            else:
                value = None if raw == "clear" else raw
            apply_user_edit(
                client,
                winter_sports_id=resort_id,
                column=column.replace("-", "_"),
                value=value,
                table_name=TABLE_NAME,
            )
            print(f"user {resort_id} {column}={raw}", file=sys.stderr)

    if args.from_copy:
        rows = rows_from_copy_jsonl(args.from_copy)
        for row in rows:
            upsert_filter_row(row, args.jsonl)
    else:
        rows = load_filter_rows(args.jsonl)

    wanted = [column for column in FILTER_COLUMNS if getattr(args, column)]
    chosen = select_rows(rows, wanted, match_all=not args.any)
    if args.csv:
        write_filter_csv(chosen if wanted else rows, args.csv)
        print(f"wrote {args.csv}", file=sys.stderr)
    if args.push:
        client = __import__("boto3").client("dynamodb")
        outcomes = push_rows(chosen if wanted else rows, table_name=TABLE_NAME, client=client)
        print(f"pushed {len(outcomes)} resorts to {TABLE_NAME}", file=sys.stderr)
    header = ["resort", *FILTER_COLUMNS]
    print("\t".join(header))
    for row in chosen:
        cells = [str(row.get("resort") or "")]
        for column in FILTER_COLUMNS:
            value = row.get(column)
            cells.append("" if value is None else ("true" if value else "false"))
        print("\t".join(cells))
    print(f"{len(chosen)} resorts", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
