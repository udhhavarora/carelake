# Demo walkthrough

All numbers below come from real runs of the default seed (42) on a laptop. They are single runs,
not benchmarks. The cloud stages are not part of this walkthrough because they have not been built.

1. **Successful run.** `scripts/run_local_pipeline.sh` -> `dbt build` PASS=43 WARN=1 ERROR=0; DQ 37/38 pass, gate PUBLISH.
   Report: [`data/sample/dq_report_passing.json`](../data/sample/dq_report_passing.json).
2. **Repeat run, no duplicates.** Running `carelake ingest` / `carelake load` again processes 0 new files and loads 0 rows
   (`skipped_already_loaded`). In Airflow, a repeated date (2025-01-02) loaded nothing new.
3. **Late and corrected records.** Raw holds 503 encounter rows for 502 distinct ids: encounter `0101-E00006` has two
   versions and staging keeps the later one (new end time, `home` disposition). The late-arriving `LATE-E00001`
   (started 2024-12-22) is in `fct_encounter`. Patient `PT000001` shows the corrected postcode `AB99`.
4. **Failed quality check blocks publication.** After deleting 3 raw condition rows and duplicating a patient key in a copy
   of the data, the gate exits 2 with 3 critical failures. In Airflow, `dq_gate` is `blocked_by_dq`, `publish` does not run.
   Report: [`data/sample/dq_report_blocked.json`](../data/sample/dq_report_blocked.json).
5. **Recoverable task failure.** With `--conf '{"fail_once_task":"load"}'`, `load` attempt 1 fails (simulated), goes
   `up_for_retry`, attempt 2 succeeds and the run continues. After repairing a damaged table, a rerun passes the gate.
6. **Source-to-target reconciliation.** Chain checked by the DQ suite: manifest clean rows = raw rows per entity;
   raw distinct encounter keys = staged; staged = fact + quarantined orphans (502 = 501 + 1); summed
   `diagnosis_count` = joined condition rows (no double counting); mart inpatient encounters = fact inpatient encounters.

Example analytical queries (DuckDB, after a pipeline run):
```sql
select * from analytics.mart_avg_los_by_department order by discharge_month, department;
select * from analytics.mart_readmission_rate order by discharge_month;
```
Caution: output reflects synthetic data (see limitations in the data dictionary); the latest month's readmission rate is censored.
