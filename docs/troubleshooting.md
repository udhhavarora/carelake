# Troubleshooting

Every entry below was hit during development.

| Symptom | Cause | Fix |
|---|---|---|
| `airflow dags list` prints "No data found" | Airflow 3 parses DAGs in a separate processor; the CLI only reads the metadata DB | `airflow dags reserialize`, then `dags list` |
| `dbt_build` fails in a day-1 backfill: column `discharge_disposition` "cannot be referenced" | Raw table lacked a column added by a later file | Fixed: loader always adds `OPTIONAL_COLUMNS`. Regression test exists |
| `dq` exits 2 | A critical check failed; this is the gate working | Read the printed FAIL lines or `data/dq/latest.json`; do not retry blindly |
| `dq_gate` blocked after you edited or deleted raw rows | Raw no longer matches the ingestion manifest | Drop the damaged raw table (`drop table raw.<entity>`) and rerun; the loader reloads its files |
| `dbt` cannot find its profile | Missing `--profiles-dir .` | Run from `dbt/` with `dbt build --profiles-dir .` |
| dbt creates the DuckDB file in the wrong place | `CARELAKE_DATA_DIR` is relative and dbt resolves it from `dbt/` | Use an absolute path |
| `docker info` says it cannot connect to the daemon | Docker Desktop not running | Not needed for any documented path |
| `ruff format` rewraps a fixed long line back to one line | Formatter fits lines to 100 columns | Split the expression into variables |
