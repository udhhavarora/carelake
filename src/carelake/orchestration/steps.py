"""Pipeline steps as subprocess calls, with run metadata. Standard library only.

Airflow runs in its own virtualenv (its dependency tree is large and would clash with dbt's),
so each task shells out to the project's Python. Every attempt appends one JSON line to
``<data_dir>/runs/run_log.jsonl`` (run id, task, attempt, status, duration, return code).
"""

from __future__ import annotations

import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

DQ_BLOCKED_EXIT = 2  # exit code of `carelake dq` when a critical check fails


class StepFailed(Exception):
    """A step exited non-zero. ``returncode`` lets callers tell DQ blocks from crashes."""

    def __init__(self, task: str, returncode: int, stderr_tail: str):
        super().__init__(f"{task} exited {returncode}: {stderr_tail}")
        self.returncode = returncode


def record(data_dir: Path, entry: dict) -> None:
    path = data_dir / "runs" / "run_log.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(entry, sort_keys=True) + "\n")


def maybe_inject_failure(data_dir: Path, run_id: str, task: str, fail_once_task: str) -> None:
    """Simulate a transient fault: fail the first attempt of ``task``, succeed afterwards.

    A marker file makes this deterministic across retries. Demonstration only.
    """
    if fail_once_task != task:
        return
    marker = data_dir / "runs" / ".fail_markers" / f"{run_id}__{task}"
    if marker.exists():
        return
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    raise RuntimeError(f"simulated transient failure in {task} (first attempt)")


def run_step(
    task: str,
    cmd: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    data_dir: Path,
    run_id: str,
    attempt: int,
    logical_date: str,
) -> str:
    """Run one command; always record metadata; raise StepFailed on non-zero exit."""
    started = datetime.now(UTC)
    t0 = time.monotonic()
    proc = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    status = (
        "success"
        if proc.returncode == 0
        else (
            "blocked_by_dq"
            if task == "dq_gate" and proc.returncode == DQ_BLOCKED_EXIT
            else "failed"
        )
    )
    record(
        data_dir,
        {
            "run_id": run_id,
            "task": task,
            "attempt": attempt,
            "logical_date": logical_date,
            "status": status,
            "returncode": proc.returncode,
            "started_at": started.isoformat(),
            "duration_s": round(time.monotonic() - t0, 3),
            "stdout_tail": proc.stdout[-500:],
            "stderr_tail": proc.stderr[-500:],
        },
    )
    if proc.returncode != 0:
        raise StepFailed(task, proc.returncode, (proc.stderr or proc.stdout)[-300:])
    return proc.stdout
