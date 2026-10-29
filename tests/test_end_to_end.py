"""Local integration test: real dbt build on DuckDB, then the DQ gate (pass and block).

Needs dbt (installed via the `local` extra) but NO cloud access. Takes a few seconds.
"""

import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from carelake import cli
from carelake.quality.suite import run as run_dq

ROOT = Path(__file__).resolve().parents[1]
DBT = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    data = tmp_path_factory.mktemp("e2e")
    import os

    mp = pytest.MonkeyPatch()
    mp.setenv("CARELAKE_DATA_DIR", str(data))
    for cmd in (["generate"], ["ingest"], ["load"]):
        assert cli.main(cmd) == 0
    proc = subprocess.run(
        [DBT, "build", "--profiles-dir", "."],
        cwd=ROOT / "dbt",
        capture_output=True,
        text=True,
        env={**os.environ, "CARELAKE_DATA_DIR": str(data)},
    )
    mp.undo()
    assert proc.returncode == 0, proc.stdout[-2000:]
    return data


def test_gate_passes_on_clean_build(built: Path):
    report, ok = run_dq(built / "carelake.duckdb", built, "e2e1")
    assert ok and report["summary"]["failed_critical"] == 0
    warned = [r["check_name"] for r in report["results"] if r["result"] == "fail"]
    assert warned == ["orphan_encounters_present"]  # the one injected orphan, tolerated


def test_gate_blocks_when_data_is_tampered(built: Path, tmp_path: Path):
    shutil.copytree(built, tmp_path / "copy")
    con = duckdb.connect(str(tmp_path / "copy" / "carelake.duckdb"))
    con.execute(
        "delete from raw.conditions where condition_id in "
        "(select condition_id from raw.conditions limit 3)"
    )
    con.close()
    report, ok = run_dq(tmp_path / "copy" / "carelake.duckdb", tmp_path / "copy", "e2e2")
    assert not ok
    failed = {
        r["check_name"]
        for r in report["results"]
        if r["result"] == "fail" and r["severity"] == "critical"
    }
    assert "reconcile:ingested_clean_rows_vs_raw:conditions" in failed
