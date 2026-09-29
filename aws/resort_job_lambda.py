"""Queue one resort change. A daily 16 GB Fargate task drains the queue.

Status is s3://BUCKET/resort-jobs/<jobId>.json.
Requests wait in s3://BUCKET/resort-jobs/inbox/<jobId>.json.

Env: RESORT_JOB_SECRET, S3_BUCKET, ECS_CLUSTER, ECS_TASK_DEFINITION
(default globalskiatlas-backend-k8s-pipeline-medium), ECS_SUBNETS (comma),
ECS_SECURITY_GROUP, ECS_CONTAINER_NAME (default pipeline).
"""
from __future__ import annotations

import json
import os
import uuid

import boto3

BUCKET = os.environ.get("S3_BUCKET", "globalskiatlas-backend-k8s-output")
SECRET = os.environ.get("RESORT_JOB_SECRET", "")


def _resp(code: int, body: dict) -> dict:
    return {
        "statusCode": code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def _auth(event: dict) -> bool:
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    return bool(SECRET) and headers.get("authorization") == f"Bearer {SECRET}"


def _job_id_from(event: dict) -> str:
    path = event.get("rawPath") or event.get("path") or ""
    parts = [p for p in path.split("/") if p]
    if parts and parts[-1] != "resort-jobs":
        return parts[-1]
    params = event.get("pathParameters") or {}
    return str(params.get("jobId") or "")


def _get(job_id: str) -> dict:
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"resort-jobs/{job_id}.json")
    except Exception:
        return _resp(404, {"jobId": job_id, "status": "missing"})
    return _resp(200, json.loads(obj["Body"].read().decode()))


def _lock_held(s3, wid: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"resort-jobs/lock-{wid}.json")
    except Exception:
        return None
    lock = json.loads(obj["Body"].read().decode())
    job = json.loads(s3.get_object(Bucket=BUCKET, Key=f"resort-jobs/{lock['jobId']}.json")["Body"].read().decode())
    if job.get("status") in ("queued", "running"):
        return job
    return None


def _post(body: dict) -> dict:
    action = body.get("action")
    if action not in ("update", "add", "delete"):
        return _resp(400, {"message": "action must be update, add, or delete"})
    wid = str(body.get("winter_sports_id") or "")
    region = str(body.get("region") or "")
    if action == "add" and not wid:
        return _resp(400, {"message": "winter_sports_id is required"})
    if action != "add" and (not wid or not region):
        return _resp(400, {"message": "winter_sports_id and region are required"})
    s3 = boto3.client("s3")
    if wid:
        held = _lock_held(s3, wid)
        if held:
            return _resp(202, {"jobId": held.get("jobId"), "status": held.get("status")})
    job_id = uuid.uuid4().hex
    queued = {"jobId": job_id, "status": "queued", "message": action, "winter_sports_id": wid}
    s3.put_object(
        Bucket=BUCKET,
        Key=f"resort-jobs/{job_id}.json",
        Body=json.dumps(queued).encode(),
        ContentType="application/json",
    )
    if wid:
        s3.put_object(
            Bucket=BUCKET,
            Key=f"resort-jobs/lock-{wid}.json",
            Body=json.dumps({"jobId": job_id}).encode(),
            ContentType="application/json",
        )
    request = {
        "action": action,
        "winter_sports_id": wid,
        "region": region,
        "name": str(body.get("name") or ""),
        "country": str(body.get("country") or ""),
        "state": str(body.get("state") or ""),
        "lat": str(body.get("lat") or ""),
        "lon": str(body.get("lon") or ""),
        "jobId": job_id,
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=f"resort-jobs/inbox/{job_id}.json",
        Body=json.dumps(request).encode(),
        ContentType="application/json",
    )
    return _resp(202, {"jobId": job_id, "status": "queued"})


def handler(event, _context):
    if not _auth(event):
        return _resp(401, {"message": "unauthorized"})
    method = (
        (event.get("requestContext") or {}).get("http", {}).get("method")
        or event.get("httpMethod")
        or "POST"
    )
    if method == "GET":
        job_id = _job_id_from(event)
        if not job_id:
            return _resp(400, {"message": "jobId required"})
        return _get(job_id)
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        return _resp(400, {"message": "plain JSON body required"})
    return _post(json.loads(body))
