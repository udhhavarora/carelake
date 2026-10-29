"""Offline tests of cloud-facing logic. They prove SQL/key generation, NOT that Snowflake or S3
accept it; real integration is unverified."""

from pathlib import Path

from carelake.data_generation.generator import SCHEMAS, write_batches
from carelake.ingestion.ingest import ingest
from carelake.ingestion.s3_upload import upload
from carelake.snowflake import sql

CFG_ARGS = dict(n_patients=20, n_practitioners=3, encounters_batch1=10, encounters_batch2=5)


def test_copy_columns_match_schema_and_metadata():
    for entity in SCHEMAS:
        stmt = sql.render_copy(entity, "RUN1")
        n = len(sql.columns(entity))
        assert f"${n}" in stmt and f"${n + 1}" not in stmt
        assert "METADATA$FILENAME" in stmt and "'RUN1'" in stmt
        assert "ON_ERROR = ABORT_STATEMENT" in stmt
    assert "DISCHARGE_DISPOSITION" in sql.render_copy("encounters", "r")


def test_raw_ddl_is_text_with_metadata_and_utc():
    ddl = sql.render_raw_table("encounters")
    assert "START_TS VARCHAR" in ddl and "_LOADED_AT TIMESTAMP_NTZ" in ddl
    assert "ALTER SESSION SET TIMEZONE = 'UTC';" in sql.render_all("b", "p", "r")


def test_merge_dedupes_source_and_guards_on_updated_at():
    m = sql.render_merge_current("encounters", "2025-01-01")
    assert "QUALIFY ROW_NUMBER()" in m and "WHEN MATCHED AND" in m
    assert "WHEN NOT MATCHED THEN INSERT" in m
    assert ") s ON t.ENCOUNTER_ID = s.ENCOUNTER_ID" in m
    assert "t.ENCOUNTER_ID = s.ENCOUNTER_ID," not in m  # key never in SET list


def test_no_credentials_in_rendered_sql():
    text = "\n".join(sql.render_all("bucket", "pre", "r")).lower()
    assert "aws_key" not in text and "password" not in text and "secret" not in text


class FakeS3:
    def __init__(self):
        self.objects: dict[str, str] = {}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise RuntimeError("404 Not Found")
        return {"Metadata": {"sha256": self.objects[Key]}}

    def put_object(self, Bucket, Key, Body, Metadata, ServerSideEncryption):
        assert ServerSideEncryption == "AES256"
        self.objects[Key] = Metadata["sha256"]


def test_s3_upload_dry_run_then_idempotent(tmp_path: Path):
    from carelake.data_generation.generator import GenConfig

    write_batches(GenConfig(**CFG_ARGS), tmp_path / "raw")
    ingest(tmp_path / "raw", tmp_path, run_id="r")
    dry = upload(tmp_path, "b", "pre", execute=False)
    assert dry["uploaded"] == 0 and dry["planned"] > 0
    fake = FakeS3()
    first = upload(tmp_path, "b", "pre/", client=fake, execute=True)
    assert first["uploaded"] == first["planned"]
    assert "pre/manifest/manifest.jsonl" in fake.objects
    assert any(k.startswith("pre/ingested/encounters/load_date=2025-01-01/") for k in fake.objects)
    again = upload(tmp_path, "b", "pre", client=fake, execute=True)
    assert again["uploaded"] == 0 and again["skipped_unchanged"] == first["planned"]
