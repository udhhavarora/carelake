# CareLake

Synthetic healthcare data platform: Python → S3 → Snowflake → dbt → Airflow.
Work in progress; status below is what has actually been run.

## Status

| Component | State |
|---|---|
| Deterministic generator (6 entities, 2 batches, injected anomalies) | Implemented, unit-tested locally |
| Local ingestion (validation, quarantine, manifest, idempotent rerun) | Implemented, unit-tested locally |
| Local raw loader (DuckDB, append-only, file-level idempotent) | Implemented, unit-tested locally |
| dbt project (6 staging views, 3 dims, 1 fact, orphan quarantine, 2 marts) on DuckDB | `dbt build`: PASS=43 WARN=1 ERROR=0 |
| dbt on Snowflake (`snowflake` target in `dbt/profiles.yml`) | Written, **never executed** |
| DQ framework (38 checks, JSON report, publish gate) | Implemented; unit-tested (4 tests) and run on local data |
| S3 upload, Snowflake load, Airflow, CI | Not started |

No cloud integration has been executed.

## Run locally (no cloud access needed)

```bash
uv venv --python 3.13 && uv pip install -e ".[dev]"
.venv/bin/python -m carelake.cli generate   # writes data/raw/<entity>/load_date=.../*.csv
.venv/bin/python -m carelake.cli ingest     # data/ingested, data/quarantine, data/manifest
.venv/bin/pytest && .venv/bin/ruff check .
```

## dbt on DuckDB (local)

DuckDB is a local stand-in so the models and tests run without Snowflake. Models use
`qualify`, which Snowflake also supports; `date_diff`, `interval 30 day` and `regexp_matches`
are DuckDB spellings and will need small changes for Snowflake (`datediff`, `dateadd`,
`regexp_like`).

```bash
uv pip install -e ".[dev,local]"
export CARELAKE_DATA_DIR=data
.venv/bin/python -m carelake.cli generate && .venv/bin/python -m carelake.cli ingest
.venv/bin/python -m carelake.cli load
cd dbt && ../.venv/bin/dbt build --profiles-dir .
```

Measured on the default seed (not a benchmark): ingest quarantined 4 rows; raw has 503 encounter
rows for 502 distinct ids (one corrected); fact has 501 encounters plus 1 orphan routed to
`quarantine_orphan_encounters`, which fires the one expected warning.

Known synthetic-data limitations visible in the marts: the generator assigns departments
independently of encounter class (so "Emergency" has inpatient stays), and 200 patients with random
re-visits produce high readmission rates (about 12-32%). These numbers demonstrate the logic, not
real clinical rates. Readmission definition is documented in `mart_readmission_rate.sql`.

## Data quality gate

```bash
.venv/bin/python -m carelake.cli dq   # exit 0 = publish, exit 2 = blocked by a critical failure
```

Checks cover row counts, nulls, duplicate keys, referential integrity (orphans tolerated up to 1%
and reported as a warning, never dropped), schema consistency (new columns reported, missing ones
fail), timestamp order, ingestion-to-raw-to-staging-to-fact-to-mart reconciliation, and changes to
previously reported months. Reports: `data/sample/dq_report_passing.json` (37/38 pass, 1 expected
warning) and `data/sample/dq_report_blocked.json` (I deliberately duplicated a patient key and deleted
3 raw condition rows in a copy of the data: 3 critical failures, exit code 2). Task success and
data-quality success are separate: the command running is not the same as the data being publishable.

Not yet covered: the historical-stability check only compares against the previous run's snapshot
file, and the suite currently runs on DuckDB only.

## Injected anomalies (batch 2)

Late-arriving encounter, corrected encounter and patient, exact duplicate, invalid ICD-10 code,
null required field, end-before-start, orphan patient reference, new `discharge_disposition`
column. Ground truth is written to `anomalies.json`. Row-level defects are quarantined at
ingestion; the orphan reference is a cross-table check left to the warehouse/dbt layer.

All data is synthetic and FHIR-inspired only, not FHIR conformant.
