"""Load ingested CSVs into the local DuckDB ``raw`` schema (stand-in for Snowflake COPY INTO).

Mirrors the Snowflake pattern: raw tables are append-only, every row carries its
source file and load timestamp, and a file is never loaded twice (like COPY INTO's
load history). Deduplication and "latest version wins" happen downstream in dbt staging.
Schema evolution: columns are matched by name; files lacking a newer column get NULL.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from carelake.data_generation.generator import OPTIONAL_COLUMNS, SCHEMAS

log = logging.getLogger("carelake.loader")


def _table_exists(con: duckdb.DuckDBPyConnection, entity: str) -> bool:
    return entity in _created(con)


def _append_file(
    con: duckdb.DuckDBPyConnection, entity: str, f: Path, rel: str, run_id: str
) -> int:
    """Append one CSV, adding any columns the table does not have yet (schema evolution)."""
    cols = [
        c[0]
        for c in con.execute(
            "describe select * from read_csv(?, all_varchar=true)", [str(f)]
        ).fetchall()
    ]
    meta = "? as _source_file, ? as _run_id, current_timestamp as _loaded_at"
    src = f"select *, {meta} from read_csv(?, all_varchar=true, header=true)"
    params = [rel, run_id, str(f)]
    if not _table_exists(con, entity):
        con.execute(f"create table raw.{entity} as {src}", params)
    else:
        have = {r[0] for r in con.execute("describe raw." + entity).fetchall()}
        for c in cols:
            if c not in have:
                con.execute(f'alter table raw.{entity} add column "{c}" varchar')
        col_list = ", ".join(f'"{c}"' for c in cols) + ", _source_file, _run_id, _loaded_at"
        con.execute(f"insert into raw.{entity} ({col_list}) {src}", params)
    return con.execute("select count(*) from read_csv(?, all_varchar=true)", [str(f)]).fetchone()[0]


def load_raw(
    ingested_dir: Path, db_path: Path, run_id: str, as_of: str | None = None
) -> dict[str, int]:
    """Append unseen files to raw.<entity>. Returns rows loaded per entity."""
    loaded: dict[str, int] = {}
    con = duckdb.connect(str(db_path))
    try:
        con.execute("create schema if not exists raw")
        for entity in SCHEMAS:
            files = sorted(
                f
                for f in (ingested_dir / entity).glob("load_date=*/*.csv")
                if not as_of or f.parent.name.removeprefix("load_date=") <= as_of
            )
            seen: set[str] = set()
            if _table_exists(con, entity):
                seen = {
                    r[0]
                    for r in con.execute(
                        f"select distinct _source_file from raw.{entity}"
                    ).fetchall()
                }
            n = 0
            for f in files:
                rel = f.relative_to(ingested_dir).as_posix()
                if rel in seen:
                    continue
                rows = _append_file(con, entity, f, rel, run_id)
                n += rows
                log.info("loaded %s rows=%d", rel, rows)
            if _table_exists(con, entity):
                have = {r[0] for r in con.execute("describe raw." + entity).fetchall()}
                for c in OPTIONAL_COLUMNS.get(entity, []):
                    if c not in have:
                        con.execute(f'alter table raw.{entity} add column "{c}" varchar')
            loaded[entity] = n
    finally:
        con.close()
    return loaded


def _created(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        r[0]
        for r in con.execute(
            "select table_name from information_schema.tables where table_schema='raw'"
        ).fetchall()
    }
