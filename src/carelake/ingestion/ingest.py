"""Local ingestion: validate raw CSVs, split into clean / quarantine, record a manifest.

Idempotency: a file is identified by its SHA-256. Files already present in the
manifest are skipped, so reruns never reprocess or duplicate data.
Nothing is silently dropped: every rejected or duplicate row is written to
quarantine with a reason code.
Cross-file checks (e.g. referential integrity) are deliberately left to the
warehouse / dbt layer, where all entities are visible together.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from carelake.data_generation.generator import SCHEMAS

log = logging.getLogger("carelake.ingestion")

ICD10_RE = re.compile(r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$")
TS_FMT = "%Y-%m-%dT%H:%M:%SZ"
PRIMARY_KEYS = {
    "facilities": "facility_id",
    "practitioners": "practitioner_id",
    "patients": "patient_id",
    "encounters": "encounter_id",
    "conditions": "condition_id",
    "observations": "observation_id",
}
REQUIRED = {
    "facilities": ["facility_id", "department"],
    "practitioners": ["practitioner_id"],
    "patients": ["patient_id", "birth_date"],
    "encounters": ["encounter_id", "patient_id", "facility_id", "start_ts", "encounter_class"],
    "conditions": ["condition_id", "encounter_id", "icd10_code"],
    "observations": ["observation_id", "encounter_id", "loinc_code"],
}
TS_COLUMNS = ["start_ts", "end_ts", "recorded_at", "observed_at", "updated_at"]


class FileRejected(Exception):
    """The whole file is unusable (e.g. missing required columns)."""


@dataclass
class FileResult:
    run_id: str
    entity: str
    source_file: str
    sha256: str
    status: str  # ingested | skipped_already_loaded | rejected
    rows_read: int = 0
    rows_clean: int = 0
    rows_quarantined: int = 0
    new_columns: list[str] = field(default_factory=list)
    duration_s: float = 0.0
    error: str | None = None
    finished_at: str = ""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def row_error(entity: str, row: dict[str, str]) -> str | None:
    """Return a reason code if the row violates a single-row rule, else None."""
    for col in REQUIRED[entity]:
        if not (row.get(col) or "").strip():
            return f"null_required:{col}"
    for col in TS_COLUMNS:
        val = row.get(col)
        if val:
            try:
                datetime.strptime(val, TS_FMT)
            except ValueError:
                return f"invalid_timestamp:{col}"
    if entity == "conditions" and not ICD10_RE.match(row["icd10_code"]):
        return "invalid_code:icd10_code"
    if entity == "encounters" and row.get("end_ts") and row["end_ts"] < row["start_ts"]:
        return "end_before_start"
    return None


def process_file(
    path: Path, entity: str, run_id: str, clean_dir: Path, quarantine_dir: Path, rel: Path
) -> FileResult:
    t0 = time.monotonic()
    result = FileResult(run_id, entity, str(rel), _sha256(path), "ingested")
    expected = SCHEMAS[entity]
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames or []
        missing = [c for c in expected if c not in header]
        if missing:
            raise FileRejected(f"missing columns {missing}")
        result.new_columns = [c for c in header if c not in expected]
        if result.new_columns:
            log.warning("schema evolution in %s: new columns %s", rel, result.new_columns)
        clean, quarantined, seen = [], [], set()
        for row in reader:
            result.rows_read += 1
            reason = row_error(entity, row)
            if reason is None:
                fingerprint = tuple(row.get(c, "") for c in header)
                if fingerprint in seen:
                    reason = "exact_duplicate"
                else:
                    seen.add(fingerprint)
            if reason:
                quarantined.append({**row, "_reject_reason": reason, "_source_file": str(rel)})
            else:
                clean.append(row)
    out_rel = rel
    (clean_dir / out_rel).parent.mkdir(parents=True, exist_ok=True)
    with (clean_dir / out_rel).open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header, lineterminator="\n")
        w.writeheader()
        w.writerows(clean)
    if quarantined:
        qpath = quarantine_dir / out_rel
        qpath.parent.mkdir(parents=True, exist_ok=True)
        qcols = header + ["_reject_reason", "_source_file"]
        with qpath.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=qcols, lineterminator="\n")
            w.writeheader()
            w.writerows(quarantined)
    result.rows_clean, result.rows_quarantined = len(clean), len(quarantined)
    result.duration_s = round(time.monotonic() - t0, 4)
    return result


def load_manifest(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def ingest(raw_dir: Path, work_dir: Path, run_id: str | None = None) -> list[FileResult]:
    """Ingest every unseen file under ``raw_dir``. Returns one result per file seen."""
    run_id = run_id or uuid.uuid4().hex[:12]
    manifest_path = work_dir / "manifest" / "manifest.jsonl"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    done = {m["sha256"] for m in load_manifest(manifest_path) if m["status"] == "ingested"}
    results: list[FileResult] = []
    for entity in SCHEMAS:
        for path in sorted((raw_dir / entity).glob("load_date=*/*.csv")):
            rel = path.relative_to(raw_dir)
            sha = _sha256(path)
            if sha in done:
                res = FileResult(run_id, entity, str(rel), sha, "skipped_already_loaded")
            else:
                try:
                    res = process_file(
                        path, entity, run_id, work_dir / "ingested", work_dir / "quarantine", rel
                    )
                except FileRejected as exc:
                    log.error("file rejected %s: %s", rel, exc)
                    res = FileResult(run_id, entity, str(rel), sha, "rejected", error=str(exc))
            res.finished_at = datetime.now(UTC).isoformat()
            log.info(
                "%s %s rows=%d clean=%d quarantined=%d",
                res.status,
                rel,
                res.rows_read,
                res.rows_clean,
                res.rows_quarantined,
            )
            results.append(res)
    with manifest_path.open("a") as fh:
        for r in results:
            fh.write(json.dumps(asdict(r), sort_keys=True) + "\n")
    return results
