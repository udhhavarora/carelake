"""Command line entry points: ``python -m carelake.cli generate|ingest``."""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import date
from pathlib import Path

from carelake.data_generation.generator import GenConfig, write_batches
from carelake.ingestion.ingest import ingest
from carelake.warehouse.local_loader import load_raw


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    data_dir = Path(os.environ.get("CARELAKE_DATA_DIR", "data"))
    p = argparse.ArgumentParser(prog="carelake")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="write deterministic synthetic CSV batches")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--start-date", type=date.fromisoformat, default=date(2025, 1, 1))
    sub.add_parser("ingest", help="validate raw files, quarantine bad rows, update manifest")
    sub.add_parser("load", help="append ingested files to local DuckDB raw schema")
    args = p.parse_args(argv)

    if args.cmd == "generate":
        files = write_batches(
            GenConfig(seed=args.seed, start_date=args.start_date), data_dir / "raw"
        )
        print(f"wrote {len(files)} files under {data_dir / 'raw'}")
    elif args.cmd == "load":
        counts = load_raw(
            data_dir / "ingested", data_dir / "carelake.duckdb", uuid.uuid4().hex[:12]
        )
        print(f"loaded rows: {counts}")
    else:
        results = ingest(data_dir / "raw", data_dir)
        bad = [r for r in results if r.status == "rejected"]
        print(f"{len(results)} files processed, {len(bad)} rejected")
        return 1 if bad else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
