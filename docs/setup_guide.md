# Setup guide

## Prerequisites
| Need | For | Status |
|---|---|---|
| [uv](https://docs.astral.sh/uv/) and Python 3.11+ (3.13 used) | everything | required |
| Git | clone, CI | required |
| Docker | optional, not used by any documented path | unverified |
| AWS account + credentials | S3 upload | **not built yet** |
| Snowflake account | warehouse load | **not built yet; no account assumed** |

## A. Local, no cloud (everything verified)
```bash
git clone <repo> && cd carelake
uv venv --python 3.13 && uv pip install -e ".[dev,local]"
source .venv/bin/activate
scripts/run_local_pipeline.sh      # generate, ingest, load, dbt build, DQ gate
pytest && ruff check .
```
Expected: `dbt build` PASS=43 WARN=1 ERROR=0, DQ `{"total": 38, "passed": 37, "failed_critical": 0, "failed_warning": 1} -> PUBLISH`.
The one warning is the deliberately injected orphan encounter.

Outputs land in `data/` (git-ignored): `raw/`, `ingested/`, `quarantine/`, `manifest/`, `dq/`, `carelake.duckdb`.
Set `CARELAKE_DATA_DIR` to relocate them (use an absolute path).

## B. Airflow, local (verified with `dags test` only)
```bash
uv venv --python 3.13 .venv-airflow
uv pip install --python .venv-airflow/bin/python apache-airflow -e .
export AIRFLOW_HOME=$PWD/.airflow AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
.venv-airflow/bin/airflow db migrate
.venv-airflow/bin/airflow dags reserialize
.venv-airflow/bin/airflow dags test carelake_pipeline 2025-01-01
.venv-airflow/bin/airflow dags test carelake_pipeline 2025-01-02
```
Each attempt is recorded in `data/runs/run_log.jsonl`. The scheduler and UI were not exercised.

## C. AWS and Snowflake (code written, NEVER EXECUTED)
Dry runs work without any account and print what would happen:
```bash
python -m carelake.cli s3-upload        # plan only
python -m carelake.cli snowflake-sql    # prints the RAW-layer SQL
```
Before using `--execute` you must decide and do the following yourself:
1. Create a bucket and IAM policies per [aws_iam.md](aws_iam.md); set `CARELAKE_S3_BUCKET` (and region).
2. Obtain a Snowflake account (a trial is not assumed). Review and run `snowflake/00_account_setup.sql`
   (set a small `<CREDIT_QUOTA>`), then `snowflake/01_s3_integration.sql`.
3. Create a key pair, register the public key on the service user, set `SNOWFLAKE_ACCOUNT`,
   `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`. `uv pip install -e ".[aws,snowflake]"`.
4. Run `s3-upload --execute`, then `snowflake-sql --execute`, then `dbt build --target snowflake`
   (port the DuckDB-specific functions first, see architecture.md).
Expect the first real run to surface syntax or permission errors; none of this has been verified.
