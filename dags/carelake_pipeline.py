"""CareLake daily pipeline: generate -> ingest -> load -> dbt build -> DQ gate -> publish.

Key behaviours
  * Retries: transient failures (crashes, timeouts) are retried; a DQ block is NOT retried,
    because re-running unchanged data cannot make it valid.
  * Task success != data-quality success: ``dq_gate`` fails the run when a critical check
    fails, so ``publish`` (a downstream task) is skipped and nothing is promoted.
  * Idempotent: ingest/load track files by hash, dbt rebuilds tables, so a rerun of the same
    date changes nothing.
  * Backfill: ``process_date`` limits ingestion to partitions <= that date. Replay days in
    order, one logical date at a time (verified with `airflow dags test <dag> <date>`). The
    `airflow backfill create` command is the intended production path but is untested here.
  * max_active_runs=1 because state lives in one local DuckDB file.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from carelake.orchestration.steps import (
    DQ_BLOCKED_EXIT,
    StepFailed,
    maybe_inject_failure,
    run_step,
)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("CARELAKE_DATA_DIR", ROOT / "data")).resolve()
PY = os.environ.get("PIPELINE_PYTHON", str(ROOT / ".venv/bin/python"))
DBT = os.environ.get("PIPELINE_DBT", str(ROOT / ".venv/bin/dbt"))
ENV = {**os.environ, "CARELAKE_DATA_DIR": str(DATA_DIR)}


def _go(task_name: str, cmd: list[str], cwd: Path = ROOT) -> str:
    ctx = get_current_context()
    run_id = ctx["run_id"]
    inject = ctx["params"].get("fail_once_task", "")
    safe_run = "".join(c if c.isalnum() else "_" for c in run_id)
    try:
        maybe_inject_failure(DATA_DIR, safe_run, task_name, inject)
        return run_step(
            task_name,
            cmd,
            cwd=cwd,
            env=ENV,
            data_dir=DATA_DIR,
            run_id=run_id,
            attempt=ctx["ti"].try_number,
            logical_date=ctx["ds"],
        )
    except StepFailed as exc:
        if task_name == "dq_gate" and exc.returncode == DQ_BLOCKED_EXIT:
            raise AirflowFailException(f"DATA QUALITY BLOCKED PUBLICATION: {exc}") from exc
        raise


def _as_of() -> str:
    ctx = get_current_context()
    return ctx["params"].get("process_date") or ctx["ds"]


with DAG(
    dag_id="carelake_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=5)},
    params={
        "process_date": Param("", type="string", description="YYYY-MM-DD; default = logical date"),
        "fail_once_task": Param("", type="string", description="demo: fail this task once"),
    },
    tags=["carelake"],
) as dag:

    @task
    def generate() -> None:
        _go("generate", [PY, "-m", "carelake.cli", "generate"])

    @task
    def ingest() -> None:
        _go("ingest", [PY, "-m", "carelake.cli", "ingest", "--as-of", _as_of()])

    @task
    def load() -> None:
        _go("load", [PY, "-m", "carelake.cli", "load", "--as-of", _as_of()])

    @task
    def dbt_build() -> None:
        _go("dbt_build", [DBT, "build", "--profiles-dir", "."], cwd=ROOT / "dbt")

    @task
    def dq_gate() -> None:
        _go("dq_gate", [PY, "-m", "carelake.cli", "dq"])

    @task
    def publish() -> None:
        ctx = get_current_context()
        out = DATA_DIR / "published" / "latest.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"run_id": ctx["run_id"], "as_of": _as_of()}) + "\n")

    generate() >> ingest() >> load() >> dbt_build() >> dq_gate() >> publish()
