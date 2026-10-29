"""The CareLake check suite: which checks run against which layer, and with what severity.

Expected vs. unexpected differences between layers:
  * ingestion -> raw: must match exactly (every clean row is loaded once) -> critical.
  * raw -> staging: raw may hold several versions per key; distinct keys must match -> critical.
  * staging -> fact: the difference must equal the quarantined orphans -> critical.
  * orphan rate: small rates are tolerated (reported as warning), large rates block.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from carelake.data_generation.generator import SCHEMAS
from carelake.ingestion.ingest import load_manifest
from carelake.quality.checks import (
    CRITICAL,
    WARNING,
    CheckResult,
    duplicate_keys,
    expected_columns,
    gate,
    historical_stability,
    null_count,
    reconcile_counts,
    referential_integrity,
    row_count_min,
    run_suite,
    timestamp_order,
)

ORPHAN_BLOCK_RATE = 0.01  # >1% of encounters without a patient blocks publication


def build_results(con: duckdb.DuckDBPyConnection, data_dir: Path) -> list[CheckResult]:
    a = "analytics"
    r: list[CheckResult] = []
    for entity, cols in SCHEMAS.items():
        r.append(row_count_min(con, f"raw.{entity}"))
        r.append(expected_columns(con, f"raw.{entity}", cols))
    for table, key in [
        (f"{a}.dim_patient", "patient_id"),
        (f"{a}.dim_facility", "facility_id"),
        (f"{a}.dim_practitioner", "practitioner_id"),
        (f"{a}.fct_encounter", "encounter_id"),
    ]:
        r += [duplicate_keys(con, table, key), null_count(con, table, key)]
    r.append(null_count(con, f"{a}.fct_encounter", "patient_id"))
    r.append(timestamp_order(con, f"{a}.fct_encounter", "start_ts", "end_ts"))

    # Orphan encounters: tolerated up to a threshold, otherwise block.
    ri = referential_integrity(
        con,
        f"{a}.stg_encounters",
        "patient_id",
        f"{a}.dim_patient",
        "patient_id",
        max_violation_rate=ORPHAN_BLOCK_RATE,
    )
    r.append(ri)
    if ri.passed and ri.details["violations"]:
        r.append(
            CheckResult(
                "orphan_encounters_present",
                f"{a}.stg_encounters",
                False,
                WARNING,
                {
                    "violations": ri.details["violations"],
                    "action": "routed to analytics.quarantine_orphan_encounters",
                },
            )
        )
    r.append(
        referential_integrity(
            con, f"{a}.fct_encounter", "facility_id", f"{a}.dim_facility", "facility_id"
        )
    )
    r.append(
        referential_integrity(
            con,
            f"{a}.stg_conditions",
            "encounter_id",
            f"{a}.stg_encounters",
            "encounter_id",
            max_violation_rate=0.01,
            severity=WARNING,
        )
    )

    # Source-to-target reconciliation.
    manifest = [
        m
        for m in load_manifest(data_dir / "manifest" / "manifest.jsonl")
        if m["status"] == "ingested"
    ]
    for entity in SCHEMAS:
        clean = sum(m["rows_clean"] for m in manifest if m["entity"] == entity)
        r.append(
            reconcile_counts(
                con,
                f"ingested_clean_rows_vs_raw:{entity}",
                f"select {clean}",
                f"select count(*) from raw.{entity}",
                f"raw.{entity}",
                CRITICAL,
                "manifest rows_clean must equal rows loaded into raw",
            )
        )
    quarantined = sum(m["rows_quarantined"] for m in manifest)
    read = sum(m["rows_read"] for m in manifest)
    clean_all = sum(m["rows_clean"] for m in manifest)
    r.append(
        CheckResult(
            "reconcile:ingestion_rows_accounted",
            "manifest",
            read == clean_all + quarantined,
            CRITICAL,
            {"rows_read": read, "clean": clean_all, "quarantined": quarantined},
        )
    )
    r.append(
        reconcile_counts(
            con,
            "raw_distinct_keys_vs_staging:encounters",
            "select count(distinct encounter_id) from raw.encounters",
            f"select count(*) from {a}.stg_encounters",
            f"{a}.stg_encounters",
            CRITICAL,
            "raw may hold several versions per key; distinct keys must equal staged rows",
        )
    )
    r.append(
        reconcile_counts(
            con,
            "staging_vs_fact_plus_quarantine:encounters",
            f"select count(*) from {a}.stg_encounters",
            f"select (select count(*) from {a}.fct_encounter) + "
            f"(select count(*) from {a}.quarantine_orphan_encounters)",
            f"{a}.fct_encounter",
            CRITICAL,
            "difference must be exactly the quarantined orphans",
        )
    )
    r.append(
        reconcile_counts(
            con,
            "fact_diagnosis_count_vs_conditions",
            f"select sum(diagnosis_count) from {a}.fct_encounter",
            f"select count(*) from {a}.stg_conditions c join {a}.fct_encounter e "
            f"on e.encounter_id=c.encounter_id",
            f"{a}.fct_encounter",
            CRITICAL,
            "no double counting: summed diagnosis_count equals joined condition rows",
        )
    )
    r.append(
        reconcile_counts(
            con,
            "mart_los_encounters_vs_fact",
            f"select sum(encounters) from {a}.mart_avg_los_by_department",
            f"select count(*) from {a}.fct_encounter where encounter_class='inpatient' "
            f"and end_ts is not null",
            f"{a}.mart_avg_los_by_department",
            CRITICAL,
        )
    )

    # Unexpected changes in history: closed months should not change between runs.
    r.append(
        historical_stability(
            con,
            data_dir / "dq" / "snapshot_encounters_by_month.json",
            f"select strftime(date_trunc('month', start_ts), '%Y-%m'), count(*) "
            f"from {a}.fct_encounter group by 1",
            f"{a}.fct_encounter",
        )
    )
    return r


def run(db_path: Path, data_dir: Path, run_id: str) -> tuple[dict, bool]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        results = build_results(con, data_dir)
    finally:
        con.close()
    report = run_suite(run_id, results, data_dir / "dq" / f"report_{run_id}.json")
    (data_dir / "dq" / "latest.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    return report, gate(report)
