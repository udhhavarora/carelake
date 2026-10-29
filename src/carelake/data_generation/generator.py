"""Deterministic synthetic healthcare data generator.

Two batches are produced from one seed:
  batch 1 (load_date = start): baseline load.
  batch 2 (load_date = start + 1 day): new records plus every anomaly type
  (duplicates, corrections, late arrivals, invalid codes, orphans, nulls,
  and a new column in encounters = schema evolution).

All anomalies are recorded in ``anomalies.json`` so tests and reconciliation
can compare what was injected with what the pipeline detected.
Data is entirely synthetic and only FHIR-*inspired* (not FHIR conformant).
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

ICD10 = ["I10", "E11.9", "J45.909", "N18.3", "M54.5", "J18.9", "K21.9", "F32.9", "I50.9", "S72.0"]
INVALID_ICD10 = ["ZZZ99", "123", "I1O"]  # fails ^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$
OBS_CODES = {"8867-4": "bpm", "8480-6": "mmHg", "2339-0": "mg/dL", "29463-7": "kg"}
DEPARTMENTS = ["Cardiology", "Emergency", "Orthopaedics", "Respiratory", "General Medicine"]
ENCOUNTER_CLASSES = ["inpatient", "emergency", "outpatient"]
GENDERS = ["female", "male", "other", "unknown"]

SCHEMAS: dict[str, list[str]] = {
    "facilities": ["facility_id", "facility_name", "department", "updated_at"],
    "practitioners": ["practitioner_id", "full_name", "specialty", "facility_id", "updated_at"],
    "patients": ["patient_id", "birth_date", "gender", "postcode_area", "updated_at"],
    "encounters": [
        "encounter_id",
        "patient_id",
        "practitioner_id",
        "facility_id",
        "encounter_class",
        "start_ts",
        "end_ts",
        "status",
        "updated_at",
    ],
    "conditions": [
        "condition_id",
        "encounter_id",
        "patient_id",
        "icd10_code",
        "recorded_at",
        "updated_at",
    ],
    "observations": [
        "observation_id",
        "encounter_id",
        "patient_id",
        "loinc_code",
        "value",
        "unit",
        "observed_at",
        "updated_at",
    ],
}
# Schema evolution: batch 2 encounters gain this column; earlier files do not have it.
EVOLVED_ENCOUNTER_COLUMN = "discharge_disposition"
# Columns added over time; raw tables always carry them (NULL until a file supplies them).
OPTIONAL_COLUMNS: dict[str, list[str]] = {"encounters": [EVOLVED_ENCOUNTER_COLUMN]}
DISPOSITIONS = ["home", "transfer", "deceased", "left_against_advice"]


@dataclass(frozen=True)
class GenConfig:
    seed: int = 42
    n_patients: int = 200
    n_practitioners: int = 20
    n_facilities: int = 3
    start_date: date = date(2025, 1, 1)
    encounters_batch1: int = 400
    encounters_batch2: int = 100


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate(cfg: GenConfig) -> dict[int, dict[str, list[dict]]]:
    """Return {batch: {entity: rows}} and the anomaly log under key 0."""
    rng = random.Random(cfg.seed)
    t1 = datetime.combine(cfg.start_date, datetime.min.time()) + timedelta(hours=23)
    t2 = t1 + timedelta(days=1)
    anomalies: list[dict] = []

    facilities, practitioners, patients = [], [], []
    for i in range(cfg.n_facilities):
        for dep in DEPARTMENTS:
            facilities.append(
                {
                    "facility_id": f"FAC{i + 1:02d}-{dep[:3].upper()}",
                    "facility_name": f"CareLake Hospital {i + 1}",
                    "department": dep,
                    "updated_at": _ts(t1),
                }
            )
    fac_ids = [f["facility_id"] for f in facilities]
    for i in range(cfg.n_practitioners):
        practitioners.append(
            {
                "practitioner_id": f"PR{i + 1:04d}",
                "full_name": f"Practitioner {i + 1}",
                "specialty": rng.choice(DEPARTMENTS),
                "facility_id": rng.choice(fac_ids),
                "updated_at": _ts(t1),
            }
        )
    for i in range(cfg.n_patients):
        birth = date(1930, 1, 1) + timedelta(days=rng.randint(0, 365 * 90))
        patients.append(
            {
                "patient_id": f"PT{i + 1:06d}",
                "birth_date": birth.isoformat(),
                "gender": rng.choice(GENDERS),
                "postcode_area": f"AB{rng.randint(1, 20)}",
                "updated_at": _ts(t1),
            }
        )
    pr_ids = [p["practitioner_id"] for p in practitioners]
    pt_ids = [p["patient_id"] for p in patients]

    def make_encounter(
        n: int, window_start: datetime, load_ts: datetime
    ) -> tuple[list, list, list]:
        enc, cond, obs = [], [], []
        for _ in range(n):
            n_enc = len(enc)
            eid = f"{load_ts:%m%d}-E{n_enc + 1:05d}"
            cls = rng.choices(ENCOUNTER_CLASSES, weights=[4, 3, 3])[0]
            start = window_start + timedelta(minutes=rng.randint(0, 24 * 60 - 1))
            los_h = rng.randint(24, 24 * 10) if cls == "inpatient" else rng.randint(1, 8)
            end = start + timedelta(hours=los_h)
            pid = rng.choice(pt_ids)
            enc.append(
                {
                    "encounter_id": eid,
                    "patient_id": pid,
                    "practitioner_id": rng.choice(pr_ids),
                    "facility_id": rng.choice(fac_ids),
                    "encounter_class": cls,
                    "start_ts": _ts(start),
                    "end_ts": _ts(end),
                    "status": "finished",
                    "updated_at": _ts(load_ts),
                }
            )
            for k in range(rng.randint(1, 3)):
                cond.append(
                    {
                        "condition_id": f"{eid}-C{k + 1}",
                        "encounter_id": eid,
                        "patient_id": pid,
                        "icd10_code": rng.choice(ICD10),
                        "recorded_at": _ts(end),
                        "updated_at": _ts(load_ts),
                    }
                )
            for k in range(rng.randint(1, 4)):
                code = rng.choice(list(OBS_CODES))
                obs.append(
                    {
                        "observation_id": f"{eid}-O{k + 1}",
                        "encounter_id": eid,
                        "patient_id": pid,
                        "loinc_code": code,
                        "value": round(rng.uniform(40, 180), 1),
                        "unit": OBS_CODES[code],
                        "observed_at": _ts(start),
                        "updated_at": _ts(load_ts),
                    }
                )
        return enc, cond, obs

    b1_start = t1 - timedelta(days=60)
    e1, c1, o1 = make_encounter(cfg.encounters_batch1, b1_start, t1)
    # Windows of 60 days: spread batch-1 encounters over the whole window.
    for e in e1:
        shift = timedelta(days=rng.randint(0, 59))
        s = datetime.strptime(e["start_ts"], "%Y-%m-%dT%H:%M:%SZ") + shift
        e["end_ts"] = _ts(
            s
            + (
                datetime.strptime(e["end_ts"], "%Y-%m-%dT%H:%M:%SZ")
                - datetime.strptime(e["start_ts"], "%Y-%m-%dT%H:%M:%SZ")
            )
        )
        e["start_ts"] = _ts(s)
    ends = {e["encounter_id"]: e["end_ts"] for e in e1}
    starts = {e["encounter_id"]: e["start_ts"] for e in e1}
    for c in c1:
        c["recorded_at"] = ends[c["encounter_id"]]
    for o in o1:
        o["observed_at"] = starts[o["encounter_id"]]

    batch1 = {
        "facilities": facilities,
        "practitioners": practitioners,
        "patients": patients,
        "encounters": e1,
        "conditions": c1,
        "observations": o1,
    }

    # ---- batch 2: new data + anomalies ----
    e2, c2, o2 = make_encounter(cfg.encounters_batch2, t1 + timedelta(hours=1), t2)
    for e in e2:
        e[EVOLVED_ENCOUNTER_COLUMN] = rng.choice(DISPOSITIONS)

    # 1. Late-arriving: encounter that started 10 days before batch 1 but first seen in batch 2.
    late_start = t1 - timedelta(days=10)
    late = dict(
        e2[0],
        encounter_id="LATE-E00001",
        start_ts=_ts(late_start),
        end_ts=_ts(late_start + timedelta(hours=30)),
        encounter_class="inpatient",
    )
    e2.append(late)
    anomalies.append({"type": "late_arriving", "entity": "encounters", "key": "LATE-E00001"})

    # 2. Corrected record: batch-1 encounter re-sent with new end_ts and later updated_at.
    victim = e1[5]
    fixed = dict(
        victim,
        end_ts=_ts(datetime.strptime(victim["end_ts"], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=12)),
        updated_at=_ts(t2),
    )
    fixed[EVOLVED_ENCOUNTER_COLUMN] = "home"
    e2.append(fixed)
    anomalies.append({"type": "correction", "entity": "encounters", "key": victim["encounter_id"]})

    # 3. Exact duplicate rows within the batch.
    dup = dict(e2[1])
    e2.append(dup)
    anomalies.append({"type": "duplicate", "entity": "encounters", "key": dup["encounter_id"]})

    # 4. Invalid ICD-10 code.
    bad = dict(c2[0], condition_id=c2[0]["condition_id"] + "-BAD", icd10_code=INVALID_ICD10[0])
    c2.append(bad)
    anomalies.append({"type": "invalid_code", "entity": "conditions", "key": bad["condition_id"]})

    # 5. Orphan encounter referencing a non-existent patient.
    orphan = dict(e2[2], encounter_id="ORPH-E00001", patient_id="PT999999")
    e2.append(orphan)
    anomalies.append({"type": "orphan_fk", "entity": "encounters", "key": "ORPH-E00001"})

    # 6. Null in a required field and end before start.
    nul = dict(e2[3], encounter_id="NULL-E00001", patient_id="")
    e2.append(nul)
    anomalies.append({"type": "null_required", "entity": "encounters", "key": "NULL-E00001"})
    neg = dict(
        e2[4],
        encounter_id="TIME-E00001",
        end_ts=_ts(datetime.strptime(e2[4]["start_ts"], "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=2)),
    )
    e2.append(neg)
    anomalies.append({"type": "end_before_start", "entity": "encounters", "key": "TIME-E00001"})

    # 7. Historical change: patient demographic correction.
    p = dict(patients[0], postcode_area="AB99", updated_at=_ts(t2))
    anomalies.append({"type": "correction", "entity": "patients", "key": p["patient_id"]})

    batch2 = {"encounters": e2, "conditions": c2, "observations": o2, "patients": [p]}
    anomalies.append(
        {"type": "schema_evolution", "entity": "encounters", "column": EVOLVED_ENCOUNTER_COLUMN}
    )
    return {1: batch1, 2: batch2, 0: {"anomalies": anomalies}}  # type: ignore[dict-item]


def write_batches(cfg: GenConfig, out_dir: Path) -> list[Path]:
    """Write CSVs to ``out_dir/<entity>/load_date=YYYY-MM-DD/<entity>_<n>.csv``."""
    data = generate(cfg)
    written: list[Path] = []
    for batch in (1, 2):
        load_date = cfg.start_date + timedelta(days=batch - 1)
        for entity, rows in data[batch].items():
            cols = list(SCHEMAS[entity])
            if entity == "encounters" and batch == 2:
                cols.append(EVOLVED_ENCOUNTER_COLUMN)
            path = out_dir / entity / f"load_date={load_date}" / f"{entity}_{batch:03d}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=cols, restval="", lineterminator="\n")
                w.writeheader()
                w.writerows(rows)
            written.append(path)
    (out_dir / "anomalies.json").write_text(
        json.dumps(data[0]["anomalies"], indent=2, sort_keys=True) + "\n"
    )  # type: ignore[index]
    return written
