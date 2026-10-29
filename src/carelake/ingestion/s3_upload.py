"""Upload ingested (clean) files, quarantine files and the manifest to S3.

STATUS: planning/key-mapping logic is unit-tested with a fake client. Real uploads have NEVER been
run (no bucket was created). Default is a dry run; pass ``execute=True`` to write to S3.

Layout (one bucket, prefix configurable):
    s3://<bucket>/<prefix>/ingested/<entity>/load_date=YYYY-MM-DD/<file>.csv   (Snowflake stage)
    s3://<bucket>/<prefix>/quarantine/<entity>/load_date=.../<file>.csv        (audit, not loaded)
    s3://<bucket>/<prefix>/manifest/manifest.jsonl
Idempotency: each object stores its SHA-256 as user metadata; an object with the same key and
hash is skipped, so reruns upload nothing.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("carelake.s3")


@dataclass(frozen=True)
class Upload:
    local: Path
    key: str
    sha256: str


def plan_uploads(work_dir: Path, prefix: str) -> list[Upload]:
    plan: list[Upload] = []
    for kind in ("ingested", "quarantine"):
        for f in sorted((work_dir / kind).glob("*/load_date=*/*.csv")):
            rel = f.relative_to(work_dir / kind).as_posix()
            plan.append(
                Upload(f, f"{prefix}/{kind}/{rel}", hashlib.sha256(f.read_bytes()).hexdigest())
            )
    manifest = work_dir / "manifest" / "manifest.jsonl"
    if manifest.exists():
        plan.append(
            Upload(
                manifest,
                f"{prefix}/manifest/manifest.jsonl",
                hashlib.sha256(manifest.read_bytes()).hexdigest(),
            )
        )
    return plan


def _remote_hash(client: Any, bucket: str, key: str) -> str | None:
    try:
        return client.head_object(Bucket=bucket, Key=key)["Metadata"].get("sha256")
    except Exception as exc:  # botocore ClientError 404 -> object absent
        if "404" in str(exc) or "Not Found" in str(exc):
            return None
        raise


def upload(
    work_dir: Path, bucket: str, prefix: str, client: Any = None, execute: bool = False
) -> dict[str, int]:
    plan = plan_uploads(work_dir, prefix.strip("/"))
    stats = {"planned": len(plan), "uploaded": 0, "skipped_unchanged": 0}
    if not execute:
        for u in plan:
            log.info("DRY RUN would upload %s -> s3://%s/%s", u.local, bucket, u.key)
        return stats
    if client is None:
        import boto3  # optional dependency: pip install -e ".[aws]"

        client = boto3.client("s3")
    for u in plan:
        if _remote_hash(client, bucket, u.key) == u.sha256:
            stats["skipped_unchanged"] += 1
            continue
        client.put_object(
            Bucket=bucket,
            Key=u.key,
            Body=u.local.read_bytes(),
            Metadata={"sha256": u.sha256},
            ServerSideEncryption="AES256",
        )
        stats["uploaded"] += 1
        log.info("uploaded s3://%s/%s", bucket, u.key)
    return stats
