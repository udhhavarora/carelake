# Design decisions

| Decision | Why | Trade-off |
|---|---|---|
| Develop on DuckDB first | No Snowflake account was available; models and tests can run for free in CI | SQL dialect differences must be ported; Snowflake behaviour unverified |
| Raw is append-only; staging picks the latest version | Mirrors how `COPY INTO` loads files; corrections never overwrite history | Raw grows; staging does the dedupe work each build |
| Quarantine instead of drop | Nothing is silently discarded; reasons are auditable | Quarantine files need monitoring |
| Cross-table checks in the warehouse layer | Referential integrity needs all entities visible | Orphans are only caught after load |
| Orphan tolerance of 1% (warning below, block above) | Small leaks should not stall reporting but must stay visible | Threshold is a judgement call, set in `suite.py` |
| No incremental dbt models | Data is tiny; a full rebuild takes about 6 s. Incremental would add complexity without benefit | Revisit when volumes justify it |
| File-hash manifest for idempotency | Same file twice is a no-op, similar to Snowflake load history | A re-sent file with different content is treated as new, which is intended |
| DQ gate is a separate step with its own exit code | Task success is not data-quality success; blocks must not be retried | One more task in the DAG |
| Airflow in its own venv, tasks call project venv | Airflow's dependency tree clashes with dbt's | Subprocess boundary; slightly more plumbing |
| `dags test` instead of a full scheduler | 8 GB laptop, Docker daemon unavailable | Scheduler, UI and backfill command untested |
| CI uses no secrets | Pipeline runs fully on DuckDB; cloud tests belong in a separate gated workflow | Cloud code paths get no automatic coverage |
