#!/usr/bin/env python3
"""
Query Iceberg tables (Glue catalog) and print a summary + sample rows.
Use this to prove the catalog works or to feed data for display (e.g. about page).

Requires: pip install -r requirements-iceberg.txt
AWS: credentials with Glue + S3 read access.

Usage:
  python scripts/query_iceberg.py --s3-bucket globalskiatlas-backend-k8s-output
  python scripts/query_iceberg.py --s3-bucket BUCKET --json
  python scripts/query_iceberg.py --s3-bucket BUCKET --limit 20
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

DEFAULT_BUCKET = "globalskiatlas-backend-k8s-output"


def _snapshot_to_dict(snap):
    """Serialize one Iceberg snapshot for JSON/display."""
    op = snap.summary.operation if snap.summary else None
    op_str = op.value if hasattr(op, "value") else str(op)
    d = {
        "snapshot_id": snap.snapshot_id,
        "timestamp_ms": snap.timestamp_ms,
        "operation": op_str,
    }
    if snap.summary:
        for key in ("added-records", "total-records", "added-data-files", "total-data-files"):
            val = getattr(snap.summary, "additional_properties", {}).get(key) or (snap.summary.get(key) if hasattr(snap.summary, "get") else None)
            if val is not None:
                d[key] = val
    return d


def _table_versioning(table):
    """Return versioning info for one table (current snapshot, history)."""
    meta = table.metadata
    current = meta.current_snapshot_id
    snapshots = getattr(meta, "snapshots", []) or []
    # Last N snapshots (newest first in list, so take last 10)
    history = [_snapshot_to_dict(s) for s in (snapshots[-10:] if len(snapshots) > 10 else snapshots)]
    return {
        "current_snapshot_id": current,
        "snapshot_count": len(snapshots),
        "snapshots": history,
    }


def _load_catalog(s3_bucket: str, region: str):
    from pyiceberg.catalog import load_catalog
    return load_catalog(
        name="glue",
        type="glue",
        warehouse=f"s3://{s3_bucket}/iceberg",
        **{"glue.region": region},
    )
DEFAULT_DATABASE = "ski_atlas"
TABLES = ["ski_areas_analyzed", "ski_areas", "lifts", "pistes"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Query Iceberg tables (Glue) and print summary + sample.",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=DEFAULT_BUCKET,
        help=f"S3 bucket / warehouse (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=DEFAULT_DATABASE,
        help=f"Glue database (default: {DEFAULT_DATABASE})",
    )
    parser.add_argument(
        "--region",
        type=str,
        default="us-east-1",
        help="AWS region for Glue",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max sample rows to show from ski_areas_analyzed (default: 10)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON only (for embedding or API)",
    )
    args = parser.parse_args()

    try:
        catalog = _load_catalog(args.s3_bucket, args.region)
    except Exception as e:
        print(f"Failed to load catalog: {e}", file=sys.stderr)
        return 1

    # Table counts and versioning (use limit for large tables so it stays fast)
    count_limits = {"ski_areas_analyzed": None, "ski_areas": None, "lifts": 15_000, "pistes": 15_000}
    counts = {}
    versioning = {}
    for table_name in TABLES:
        try:
            table = catalog.load_table(f"{args.database}.{table_name}")
            versioning[table_name] = _table_versioning(table)
            limit = count_limits.get(table_name)
            scan = table.scan(limit=limit) if limit else table.scan()
            df = scan.to_pandas()
            n = len(df)
            if limit and n >= limit:
                counts[table_name] = f"{n}+"
            else:
                counts[table_name] = n
        except Exception as e:
            counts[table_name] = f"error: {e}"
            versioning[table_name] = {"error": str(e)}

    # Sample from ski_areas_analyzed (resort names, country, etc.)
    sample_rows = []
    try:
        table = catalog.load_table(f"{args.database}.ski_areas_analyzed")
        df = table.scan().to_pandas()
        n = min(args.limit, len(df))
        # Pick columns that exist and are display-friendly
        cols = [c for c in ["name", "name_en", "country_code", "region", "lifts", "pistes_km"] if c in df.columns]
        if not cols:
            cols = list(df.columns)[:6]
        sub = df[cols].head(n)
        for _, row in sub.iterrows():
            sample_rows.append(row.to_dict())
    except Exception as e:
        sample_rows = [{"error": str(e)}]

    out = {
        "source": "Apache Iceberg via AWS Glue",
        "database": args.database,
        "bucket": args.s3_bucket,
        "table_counts": counts,
        "versioning": versioning,
        "sample_resorts": sample_rows,
    }

    if args.json:
        print(json.dumps(out, default=str, indent=2))
        return 0

    # Human-readable
    print("=== Iceberg tables (Glue catalog) ===\n")
    print(f"Database: {args.database}  |  Bucket: {args.s3_bucket}\n")
    print("Row counts:")
    for name, count in counts.items():
        print(f"  {name}: {count}")
    print("\nVersioning (snapshots):")
    for name, ver in versioning.items():
        if "error" in ver:
            print(f"  {name}: {ver['error']}")
        else:
            cur = ver.get("current_snapshot_id")
            n = ver.get("snapshot_count", 0)
            snaps = ver.get("snapshots", [])
            last = snaps[-1] if snaps else {}
            ts_ms = last.get("timestamp_ms")
            ts_str = ""
            if ts_ms:
                ts_str = datetime.utcfromtimestamp(ts_ms / 1000.0).strftime("%Y-%m-%d %H:%M UTC")
            op = last.get("operation", "?")
            print(f"  {name}: current_snapshot_id={cur}  snapshots={n}  latest={ts_str}  operation={op}")
    print(f"\nSample resorts (first {len(sample_rows)}):")
    for i, row in enumerate(sample_rows, 1):
        if "error" in row:
            print(f"  {row['error']}")
        else:
            parts = [f"{k}={v}" for k, v in row.items() if v is not None and str(v)[:50]]
            print(f"  {i}. " + " | ".join(parts))
    print("\n(Query ran successfully - Iceberg + Glue are working.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
