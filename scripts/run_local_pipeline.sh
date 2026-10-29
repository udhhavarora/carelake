#!/usr/bin/env bash
# Full local pipeline on DuckDB, no cloud access: generate -> ingest -> load -> dbt -> DQ gate.
# Used by developers and by CI so both exercise exactly the same steps.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export CARELAKE_DATA_DIR="${CARELAKE_DATA_DIR:-$ROOT/data}"
PY="${PYTHON:-python}"
DBT="${DBT:-dbt}"

rm -rf "$CARELAKE_DATA_DIR"/{raw,ingested,quarantine,manifest,dq,carelake.duckdb}

$PY -m carelake.cli generate
$PY -m carelake.cli ingest
$PY -m carelake.cli load
(cd "$ROOT/dbt" && $DBT build --profiles-dir .)
$PY -m carelake.cli dq   # exits 2 (failing this script) if a critical check fails
