"""Render Snowflake SQL for the RAW layer from the same schema definitions the generator uses.

STATUS: rendered text is unit-tested for structure only. It has NEVER been executed against
Snowflake (no account was available) and was written without checking the current Snowflake docs.
Treat every statement as unverified until run.

Snowflake-specific (not generic SQL): COPY INTO, stages, file formats, METADATA$FILENAME,
QUALIFY, TIMESTAMP_NTZ/LTZ, MERGE's nondeterministic-match error.
"""

from __future__ import annotations

from carelake.data_generation.generator import OPTIONAL_COLUMNS, SCHEMAS
from carelake.ingestion.ingest import PRIMARY_KEYS

DB, RAW = "CARELAKE", "CARELAKE.RAW"
FILE_FORMAT = f"{RAW}.CSV_FMT"
STAGE = f"{RAW}.S3_INGESTED_STAGE"
META_COLS = ["_SOURCE_FILE", "_RUN_ID", "_LOADED_AT"]


def columns(entity: str) -> list[str]:
    """Source columns in file order; optional (later-added) columns always come last."""
    return SCHEMAS[entity] + OPTIONAL_COLUMNS.get(entity, [])


def render_file_format() -> str:
    # Raw is all-text by design: typing and validity belong to dbt staging, so a bad value can
    # never fail the load. NULL_IF maps empty strings to SQL NULL (null semantics).
    return (
        f"CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT}\n"
        "  TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"'\n"
        "  NULL_IF = ('') EMPTY_FIELD_AS_NULL = TRUE TRIM_SPACE = FALSE;"
    )


def render_stage(bucket: str, prefix: str) -> str:
    return (
        f"CREATE STAGE IF NOT EXISTS {STAGE}\n"
        f"  URL = 's3://{bucket}/{prefix}/ingested/'\n"
        "  STORAGE_INTEGRATION = CARELAKE_S3_INT\n"
        f"  FILE_FORMAT = {FILE_FORMAT};"
    )


def render_raw_table(entity: str) -> str:
    cols = ",\n  ".join(f"{c.upper()} VARCHAR" for c in columns(entity))
    return (
        f"CREATE TABLE IF NOT EXISTS {RAW}.{entity.upper()} (\n  {cols},\n"
        "  _SOURCE_FILE VARCHAR,\n  _RUN_ID VARCHAR,\n"
        "  _LOADED_AT TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())"
        "::TIMESTAMP_NTZ\n);"
    )


def render_copy(entity: str, run_id: str) -> str:
    """COPY one entity's files from the stage into RAW.

    Positional $n selection is used so file metadata can be added. Files written before an
    optional column existed are one column short: Snowflake yields NULL for a positional
    reference beyond the file's width (ASSUMPTION - verify on first run).
    Idempotency: Snowflake's load history skips files already loaded into this table (kept
    64 days), so re-running does not duplicate. After 64 days you must not re-run old files.
    """
    cols = columns(entity)
    select = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
    targets = ", ".join([c.upper() for c in cols] + META_COLS[:2])
    return (
        f"COPY INTO {RAW}.{entity.upper()} ({targets})\n"
        f"FROM (SELECT {select}, METADATA$FILENAME, '{run_id}' FROM @{STAGE}/{entity}/)\n"
        "PATTERN = '.*[.]csv'\n"
        "ON_ERROR = ABORT_STATEMENT;  -- ingestion already quarantined bad rows; stop on surprises"
    )


def render_current_table(entity: str) -> str:
    cols = ",\n  ".join(f"{c.upper()} VARCHAR" for c in columns(entity))
    return f"CREATE TABLE IF NOT EXISTS {DB}.ANALYTICS.{entity.upper()}_CURRENT (\n  {cols}\n);"


def render_merge_current(entity: str, watermark: str) -> str:
    """OPTIONAL current-state table kept by MERGE (not used by the dbt models, which use QUALIFY).

    Why MERGE: it upserts the newest version of each key in one atomic statement, so a downstream
    incremental model can read a small current-state table instead of the full append-only history.
    The source is de-duplicated first because Snowflake raises an error (or picks an arbitrary
    row, depending on ERROR_ON_NONDETERMINISTIC_MERGE) if several source rows match one target row.
    Deletes: the synthetic sources never send delete markers. If a feed did, add a soft-delete flag
    and a `WHEN MATCHED AND s.is_deleted THEN UPDATE SET ...` branch rather than hard-deleting.
    """
    cols = columns(entity)
    pk = PRIMARY_KEYS[entity].upper()
    cur = f"{DB}.ANALYTICS.{entity.upper()}_CURRENT"
    non_key = [c.upper() for c in cols if c.upper() != pk]
    ts = "TRY_TO_TIMESTAMP_NTZ({})"
    newer = f"{ts.format('s.UPDATED_AT')} > {ts.format('t.UPDATED_AT')}"
    return (
        f"MERGE INTO {cur} t\n"
        "USING (\n"
        f"  SELECT {', '.join(c.upper() for c in cols)}, _LOADED_AT FROM {RAW}.{entity.upper()}\n"
        f"  WHERE _LOADED_AT > '{watermark}'::TIMESTAMP_NTZ\n"
        f"  QUALIFY ROW_NUMBER() OVER (PARTITION BY {pk} "
        "ORDER BY UPDATED_AT DESC, _LOADED_AT DESC) = 1\n"
        f") s ON t.{pk} = s.{pk}\n"
        f"WHEN MATCHED AND {newer} THEN UPDATE SET\n  "
        + ",\n  ".join(f"t.{c} = s.{c}" for c in non_key)
        + f"\nWHEN NOT MATCHED THEN INSERT ({', '.join(c.upper() for c in cols)})\n"
        f"  VALUES ({', '.join('s.' + c.upper() for c in cols)});"
    )


def render_all(bucket: str, prefix: str, run_id: str, watermark: str = "1970-01-01") -> list[str]:
    stmts = [
        "USE ROLE CARELAKE_LOADER;",
        "USE WAREHOUSE CARELAKE_WH;",
        "ALTER SESSION SET TIMEZONE = 'UTC';",
        render_file_format(),
        render_stage(bucket, prefix),
    ]
    stmts += [render_raw_table(e) for e in SCHEMAS]
    stmts += [render_copy(e, run_id) for e in SCHEMAS]
    return stmts
