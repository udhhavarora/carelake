import json
import sys
from pathlib import Path

import pytest

from carelake import cli
from carelake.orchestration import steps


def test_record_and_run_step_success_and_failure(tmp_path: Path):
    kw = dict(cwd=tmp_path, env={}, data_dir=tmp_path, run_id="r", attempt=1, logical_date="d")
    out = steps.run_step("t", [sys.executable, "-c", "print('hi')"], **kw)
    assert out.strip() == "hi"
    with pytest.raises(steps.StepFailed) as exc:
        steps.run_step("t", [sys.executable, "-c", "import sys; sys.exit(3)"], **kw)
    assert exc.value.returncode == 3
    with pytest.raises(steps.StepFailed):
        steps.run_step("dq_gate", [sys.executable, "-c", "import sys; sys.exit(2)"], **kw)
    log = [json.loads(x) for x in (tmp_path / "runs" / "run_log.jsonl").read_text().splitlines()]
    assert [r["status"] for r in log] == ["success", "failed", "blocked_by_dq"]


def test_fail_once_injection_fails_first_attempt_only(tmp_path: Path):
    steps.maybe_inject_failure(tmp_path, "r", "load", "")  # not targeted: no-op
    with pytest.raises(RuntimeError, match="simulated transient failure in load"):
        steps.maybe_inject_failure(tmp_path, "r", "load", "load")
    steps.maybe_inject_failure(tmp_path, "r", "load", "load")  # retry passes


def test_cli_generate_ingest_load(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("CARELAKE_DATA_DIR", str(tmp_path))
    assert cli.main(["generate"]) == 0
    assert cli.main(["ingest", "--as-of", "2025-01-01"]) == 0
    assert cli.main(["load", "--as-of", "2025-01-01"]) == 0
    assert (tmp_path / "carelake.duckdb").exists()
    assert cli.main(["s3-upload"]) == 0  # dry run: must not need boto3 or credentials
    monkeypatch.delenv("CARELAKE_S3_BUCKET", raising=False)
    with pytest.raises(SystemExit):
        cli.main(["s3-upload", "--execute"])
