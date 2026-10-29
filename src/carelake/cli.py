"""Command line entry points: ``python -m carelake.cli generate|ingest``."""

from __future__ import annotations

import argparse
import json
import logging
import os
import uuid
from datetime import date
from pathlib import Path

from carelake.data_generation.generator import GenConfig, write_batches
from carelake.ingestion.ingest import ingest
from carelake.quality.suite import run as run_dq
from carelake.warehouse.local_loader import load_raw


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    data_dir = Path(os.environ.get("CARELAKE_DATA_DIR", "data"))
    p = argparse.ArgumentParser(prog="carelake")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="write deterministic synthetic CSV batches")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--start-date", type=date.fromisoformat, default=date(2025, 1, 1))
    i = sub.add_parser("ingest", help="validate raw files, quarantine bad rows, update manifest")
    i.add_argument("--as-of", help="only partitions with load_date <= YYYY-MM-DD")
    ld = sub.add_parser("load", help="append ingested files to local DuckDB raw schema")
    ld.add_argument("--as-of", help="only partitions with load_date <= YYYY-MM-DD")
    sub.add_parser("dq", help="run data-quality suite; exit 2 if a critical check fails")
    sf = sub.add_parser(
        "snowflake-sql", help="print (or with --execute run) RAW-layer Snowflake SQL"
    )
    sf.add_argument("--bucket", default=os.environ.get("CARELAKE_S3_BUCKET", "<BUCKET>"))
    sf.add_argument("--prefix", default=os.environ.get("CARELAKE_S3_PREFIX", "carelake"))
    sf.add_argument("--execute", action="store_true", help="really run on Snowflake (untested)")
    up = sub.add_parser("s3-upload", help="plan (or with --execute perform) S3 upload")
    up.add_argument("--execute", action="store_true", help="really write to S3 (untested)")
    args = p.parse_args(argv)

    if args.cmd == "generate":
        files = write_batches(
            GenConfig(seed=args.seed, start_date=args.start_date), data_dir / "raw"
        )
        print(f"wrote {len(files)} files under {data_dir / 'raw'}")
    elif args.cmd == "dq":
        report, ok = run_dq(data_dir / "carelake.duckdb", data_dir, uuid.uuid4().hex[:12])
        print(json.dumps(report["summary"]), "->", "PUBLISH" if ok else "BLOCKED")
        for res in report["results"]:
            if res["result"] == "fail":
                name = f"{res['check_name']} on {res['dataset']}"
                print(f"  FAIL [{res['severity']}] {name}: {res['details']}")
        return 0 if ok else 2
    elif args.cmd == "snowflake-sql":
        from carelake.snowflake.runner import run_statements
        from carelake.snowflake.sql import render_all

        stmts = render_all(args.bucket, args.prefix, uuid.uuid4().hex[:12])
        run_statements(stmts, execute=args.execute)
    elif args.cmd == "s3-upload":
        from carelake.ingestion.s3_upload import upload

        bucket = os.environ.get("CARELAKE_S3_BUCKET", "")
        if args.execute and not bucket:
            raise SystemExit("set CARELAKE_S3_BUCKET before using --execute")
        print(
            upload(
                data_dir,
                bucket or "<BUCKET>",
                os.environ.get("CARELAKE_S3_PREFIX", "carelake"),
                execute=args.execute,
            )
        )
    elif args.cmd == "load":
        counts = load_raw(
            data_dir / "ingested",
            data_dir / "carelake.duckdb",
            uuid.uuid4().hex[:12],
            as_of=args.as_of,
        )
        print(f"loaded rows: {counts}")
    else:
        results = ingest(data_dir / "raw", data_dir, as_of=args.as_of)
        bad = [r for r in results if r.status == "rejected"]
        print(f"{len(results)} files processed, {len(bad)} rejected")
        return 1 if bad else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
