import filecmp
import json
from pathlib import Path

from carelake.data_generation.generator import GenConfig, write_batches
from carelake.ingestion.ingest import ICD10_RE, ingest, load_manifest, row_error

CFG = GenConfig(n_patients=30, n_practitioners=5, encounters_batch1=40, encounters_batch2=10)


def test_generator_is_deterministic(tmp_path: Path):
    a, b = tmp_path / "a", tmp_path / "b"
    files_a, files_b = write_batches(CFG, a), write_batches(CFG, b)
    for fa, fb in zip(files_a, files_b, strict=True):
        assert filecmp.cmp(fa, fb, shallow=False)


def test_different_seed_changes_output(tmp_path: Path):
    write_batches(CFG, tmp_path / "a")
    write_batches(GenConfig(**{**CFG.__dict__, "seed": 7}), tmp_path / "b")
    p = "patients/load_date=2025-01-01/patients_001.csv"
    assert (tmp_path / "a" / p).read_text() != (tmp_path / "b" / p).read_text()


def test_row_rules():
    base = {"condition_id": "c", "encounter_id": "e", "icd10_code": "I10"}
    assert row_error("conditions", base) is None
    assert row_error("conditions", {**base, "icd10_code": "ZZZ99"}) == "invalid_code:icd10_code"
    assert ICD10_RE.match("E11.9")
    enc = {
        "encounter_id": "e",
        "patient_id": "p",
        "facility_id": "f",
        "encounter_class": "inpatient",
        "start_ts": "2025-01-02T00:00:00Z",
        "end_ts": "2025-01-01T00:00:00Z",
    }
    assert row_error("encounters", enc) == "end_before_start"
    assert row_error("encounters", {**enc, "patient_id": ""}) == "null_required:patient_id"


def test_ingest_quarantines_injected_anomalies_and_is_idempotent(tmp_path: Path):
    raw = tmp_path / "raw"
    write_batches(CFG, raw)
    anomalies = json.loads((raw / "anomalies.json").read_text())

    first = ingest(raw, tmp_path, run_id="r1")
    assert all(r.status == "ingested" for r in first)
    quarantined = sum(r.rows_quarantined for r in first)
    # duplicate, invalid code, null patient, end-before-start = 4 injected row-level defects
    assert quarantined == 4
    enc2 = next(r for r in first if r.source_file.startswith("encounters/load_date=2025-01-02"))
    assert enc2.new_columns == ["discharge_disposition"]
    assert any(a["type"] == "orphan_fk" for a in anomalies)  # left for warehouse-level check

    qfile = next((tmp_path / "quarantine" / "encounters").glob("*/*.csv")).read_text()
    assert "exact_duplicate" in qfile and "null_required:patient_id" in qfile

    second = ingest(raw, tmp_path, run_id="r2")
    assert all(r.status == "skipped_already_loaded" for r in second)
    ingested = [
        m
        for m in load_manifest(tmp_path / "manifest" / "manifest.jsonl")
        if m["status"] == "ingested"
    ]
    assert len(ingested) == len(first)


def test_loader_is_idempotent_and_handles_schema_evolution(tmp_path: Path):
    import duckdb

    from carelake.warehouse.local_loader import load_raw

    raw = tmp_path / "raw"
    write_batches(CFG, raw)
    ingest(raw, tmp_path, run_id="r1")
    db = tmp_path / "w.duckdb"
    first = load_raw(tmp_path / "ingested", db, "l1")
    assert first["encounters"] > 0
    assert load_raw(tmp_path / "ingested", db, "l2") == dict.fromkeys(first, 0)
    con = duckdb.connect(str(db), read_only=True)
    cols = [r[0] for r in con.execute("describe raw.encounters").fetchall()]
    nulls = con.execute(
        "select count(*) from raw.encounters where discharge_disposition is null"
    ).fetchone()[0]
    assert "discharge_disposition" in cols and nulls == 40  # batch-1 rows get NULL
