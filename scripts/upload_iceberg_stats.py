#!/usr/bin/env python3
"""
Run query_iceberg.py --json and upload the result to S3 for the Download Data page.
The Global Ski Atlas frontend fetches this JSON from /api/iceberg-stats (Lambda reads S3).

Requires: pip install -r requirements-iceberg.txt
AWS: credentials with Glue read + S3 write to the bucket.

Usage:
  python scripts/upload_iceberg_stats.py --s3-bucket globalskiatlas-backend-k8s-output
  python scripts/upload_iceberg_stats.py --s3-bucket BUCKET --limit 20
  python scripts/upload_iceberg_stats.py --s3-bucket BUCKET --dry-run
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

DEFAULT_BUCKET = "globalskiatlas-backend-k8s-output"
S3_KEY = "iceberg-stats/latest.json"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Query Iceberg (Glue), output JSON, upload to S3 for Download Data page.",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=DEFAULT_BUCKET,
        help=f"S3 bucket to upload to (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max sample resorts in JSON (default: 20)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default="us-east-1",
        help="AWS region for Glue",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run query and print JSON only; do not upload",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    query_script = script_dir / "query_iceberg.py"

    cmd = [
        sys.executable,
        str(query_script),
        "--json",
        "--s3-bucket",
        args.s3_bucket,
        "--region",
        args.region,
        "--limit",
        str(args.limit),
    ]
    try:
        result = subprocess.run(
            cmd,
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        print("query_iceberg.py timed out", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Failed to run query_iceberg.py: {e}", file=sys.stderr)
        return 1

    if result.returncode != 0:
        print(result.stderr or "query_iceberg.py failed", file=sys.stderr)
        return result.returncode

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON from query_iceberg.py: {e}", file=sys.stderr)
        return 1

    if args.dry_run:
        print(json.dumps(data, default=str, indent=2))
        return 0

    try:
        import boto3
    except ImportError:
        print("boto3 required for upload. pip install -r requirements-iceberg.txt", file=sys.stderr)
        return 1

    body = json.dumps(data, default=str, indent=2)
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=args.s3_bucket,
            Key=S3_KEY,
            Body=body,
            ContentType="application/json",
        )
    except Exception as e:
        print(f"Upload failed: {e}", file=sys.stderr)
        return 1

    print(f"Uploaded s3://{args.s3_bucket}/{S3_KEY}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
