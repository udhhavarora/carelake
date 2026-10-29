"""Reusable data-quality checks over a DuckDB (or Snowflake-compatible) connection.

Each check returns a ``CheckResult``. A failed check with severity ``critical`` blocks
publication (see ``run_suite`` / ``gate``); ``warning`` fails are reported but do not block.
Table names are interpolated, so they must come from code/config, never from user input.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

CRITICAL, WARNING = "critical", "warning"


@dataclass
class CheckResult:
    check_name: str
    dataset: str
    passed: bool
    severity: str
    details: dict[str, Any] = field(default_factory=dict)


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return con.execute(sql).fetchone()[0]


def row_count_min(con, table: str, minimum: int = 1, severity: str = CRITICAL) -> CheckResult:
    n = _scalar(con, f"select count(*) from {table}")
    return CheckResult("row_count_min", table, n >= minimum, severity, {"rows": n, "min": minimum})


def null_count(con, table: str, column: str, severity: str = CRITICAL) -> CheckResult:
    n = _scalar(con, f"select count(*) from {table} where {column} is null")
    return CheckResult(f"null_count:{column}", table, n == 0, severity, {"null_rows": n})


def duplicate_keys(con, table: str, key: str, severity: str = CRITICAL) -> CheckResult:
    rows = con.execute(
        f"select {key}, count(*) c from {table} group by 1 having count(*) > 1 limit 5"
    ).fetchall()
    n = _scalar(
        con, f"select count(*) from (select {key} from {table} group by 1 having count(*)>1)"
    )
    return CheckResult(
        f"duplicate_keys:{key}",
        table,
        n == 0,
        severity,
        {"duplicate_key_count": n, "sample": [r[0] for r in rows]},
    )


def referential_integrity(
    con,
    child: str,
    child_col: str,
    parent: str,
    parent_col: str,
    max_violation_rate: float = 0.0,
    severity: str = CRITICAL,
) -> CheckResult:
    """Fail if more than ``max_violation_rate`` of child rows have no parent.

    A small tolerated rate lets the pipeline publish while the offenders stay visible
    (they are quarantined by the models, never dropped silently).
    """
    total = _scalar(con, f"select count(*) from {child}")
    bad = con.execute(
        f"select c.{child_col} from {child} c left join {parent} p on p.{parent_col}=c.{child_col} "
        f"where p.{parent_col} is null limit 5"
    ).fetchall()
    n_bad = _scalar(
        con,
        f"select count(*) from {child} c left join {parent} p on p.{parent_col}=c.{child_col} "
        f"where p.{parent_col} is null",
    )
    rate = n_bad / total if total else 0.0
    return CheckResult(
        f"referential_integrity:{child_col}->{parent}.{parent_col}",
        child,
        rate <= max_violation_rate,
        severity,
        {
            "violations": n_bad,
            "rate": round(rate, 6),
            "max_rate": max_violation_rate,
            "sample": [r[0] for r in bad],
        },
    )


def expected_columns(con, table: str, columns: list[str], severity: str = CRITICAL) -> CheckResult:
    schema, name = table.split(".")
    have = {
        r[0]
        for r in con.execute(
            "select column_name from information_schema.columns "
            "where table_schema=? and table_name=?",
            [schema, name],
        ).fetchall()
    }
    missing = sorted(set(columns) - have)
    extra = sorted(have - set(columns) - {"_source_file", "_run_id", "_loaded_at"})
    # extra columns are schema evolution: reported, not failed.
    return CheckResult(
        "schema_consistency",
        table,
        not missing,
        severity,
        {"missing": missing, "unexpected_new_columns": extra},
    )


def timestamp_order(
    con, table: str, start_col: str, end_col: str, severity: str = CRITICAL
) -> CheckResult:
    n = _scalar(con, f"select count(*) from {table} where {end_col} < {start_col}")
    return CheckResult(
        f"timestamp_order:{start_col}<={end_col}", table, n == 0, severity, {"violations": n}
    )


def reconcile_counts(
    con,
    name: str,
    left_sql: str,
    right_sql: str,
    dataset: str,
    severity: str = CRITICAL,
    note: str = "",
) -> CheckResult:
    """Two scalar SQL expressions must be equal (source-to-target reconciliation)."""
    left, right = _scalar(con, left_sql), _scalar(con, right_sql)
    return CheckResult(
        f"reconcile:{name}",
        dataset,
        left == right,
        severity,
        {"left": left, "right": right, "difference": left - right, "note": note},
    )


def historical_stability(
    con,
    snapshot_path: Path,
    sql: str,
    dataset: str,
    tolerance: float = 0.0,
    severity: str = WARNING,
) -> CheckResult:
    """Compare per-period results with the previous run's snapshot.

    ``sql`` must return (period, value). Changes to periods that already existed are
    "unexpected changes in historical results". New periods are expected and ignored.
    The snapshot is overwritten after comparison so each run compares with the last one.
    """
    current = {str(p): v for p, v in con.execute(sql).fetchall()}
    previous = json.loads(snapshot_path.read_text()) if snapshot_path.exists() else None
    changed = {}
    if previous is not None:
        for period, old in previous.items():
            new = current.get(period)
            if new is None or abs(new - old) > tolerance * max(abs(old), 1):
                changed[period] = {"previous": old, "current": new}
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(current, sort_keys=True))
    return CheckResult(
        "historical_stability",
        dataset,
        not changed,
        severity,
        {"baseline_exists": previous is not None, "changed_periods": changed},
    )


def run_suite(run_id: str, results: list[CheckResult], report_path: Path) -> dict:
    """Write the structured report and return it. ``gate`` decides whether to block."""
    report = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "summary": {
            "total": len(results),
            "passed": sum(r.passed for r in results),
            "failed_critical": sum(not r.passed and r.severity == CRITICAL for r in results),
            "failed_warning": sum(not r.passed and r.severity == WARNING for r in results),
        },
        "results": [asdict(r) | {"result": "pass" if r.passed else "fail"} for r in results],
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str) + "\n")
    return report


def gate(report: dict) -> bool:
    """True if the data may be published (no critical failures)."""
    return report["summary"]["failed_critical"] == 0
