import duckdb

from carelake.quality import checks as q


def _con():
    con = duckdb.connect()
    con.execute("create table dim(id varchar)")
    con.execute("insert into dim values ('a'),('b')")
    con.execute("create table fact(id varchar, dim_id varchar, s timestamp, e timestamp)")
    con.execute(
        "insert into fact values ('1','a','2025-01-01','2025-01-02'),"
        "('2','b','2025-01-01','2025-01-02'),('3','zzz','2025-01-03','2025-01-02'),"
        "('3','a',null,null)"
    )
    return con


def test_duplicates_nulls_and_order():
    con = _con()
    d = q.duplicate_keys(con, "fact", "id")
    assert not d.passed and d.details["sample"] == ["3"]
    assert not q.null_count(con, "fact", "s").passed
    assert not q.timestamp_order(con, "fact", "s", "e").passed
    assert q.row_count_min(con, "fact", 4).passed


def test_referential_integrity_threshold():
    con = _con()
    strict = q.referential_integrity(con, "fact", "dim_id", "dim", "id")
    assert not strict.passed and strict.details["violations"] == 1
    tolerant = q.referential_integrity(con, "fact", "dim_id", "dim", "id", max_violation_rate=0.5)
    assert tolerant.passed


def test_reconcile_and_historical(tmp_path):
    con = _con()
    ok = q.reconcile_counts(con, "n", "select 4", "select count(*) from fact", "fact")
    bad = q.reconcile_counts(con, "n", "select 5", "select count(*) from fact", "fact")
    assert ok.passed and not bad.passed and bad.details["difference"] == 1
    snap = tmp_path / "s.json"
    sql = (
        "select strftime(date_trunc('month', s), '%Y-%m'), count(*) "
        "from fact where s is not null group by 1"
    )
    first = q.historical_stability(con, snap, sql, "fact")
    assert first.passed and not first.details["baseline_exists"]
    con.execute("insert into fact values ('9','a','2025-01-05','2025-01-06')")
    second = q.historical_stability(con, snap, sql, "fact")
    assert not second.passed and "2025-01" in second.details["changed_periods"]


def test_gate_blocks_only_on_critical(tmp_path):
    warn = q.CheckResult("w", "t", False, q.WARNING)
    crit = q.CheckResult("c", "t", False, q.CRITICAL)
    good = q.CheckResult("g", "t", True, q.CRITICAL)
    assert q.gate(q.run_suite("r", [good, warn], tmp_path / "a.json"))
    blocked = q.run_suite("r", [good, warn, crit], tmp_path / "b.json")
    assert not q.gate(blocked) and blocked["summary"]["failed_critical"] == 1
