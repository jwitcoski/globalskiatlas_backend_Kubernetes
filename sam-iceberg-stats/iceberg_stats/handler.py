"""
Lambda handler: read s3://IcebergStatsBucket/iceberg-stats/latest.json and return as JSON.
Served at GET /api/iceberg-stats for the Download Data page.
"""
import json
import os

import boto3
from botocore.exceptions import ClientError

S3_KEY = "iceberg-stats/latest.json"


def handler(event, context):
    bucket = os.environ.get("ICEBERG_STATS_BUCKET")
    if not bucket:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "ICEBERG_STATS_BUCKET not set"}),
        }
    s3 = boto3.client("s3")
    try:
        resp = s3.get_object(Bucket=bucket, Key=S3_KEY)
        body = resp["Body"].read().decode("utf-8")
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=300",
            },
            "body": body,
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "error": "iceberg-stats not found",
                    "hint": "Run upload_iceberg_stats.py and set IcebergStatsBucket in SAM",
                }),
            }
        raise
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "server error", "detail": str(e)}),
        }
