"""Execute rendered SQL on Snowflake. STATUS: NEVER RUN (no account). Dry run by default.

Auth is key-pair only; the private key path comes from the environment and is never logged.
The connector is an optional dependency: pip install -e ".[snowflake]".
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger("carelake.snowflake")

REQUIRED_ENV = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH"]


def run_statements(statements: list[str], execute: bool = False) -> int:
    if not execute:
        for s in statements:
            print(s, "\n")
        log.info("DRY RUN: %d statements printed, nothing executed", len(statements))
        return len(statements)
    missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"missing environment variables: {missing}")
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as fh:
        key = serialization.load_pem_private_key(fh.read(), password=None)
    der = key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=der,
        role=os.environ.get("SNOWFLAKE_ROLE", "CARELAKE_LOADER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CARELAKE_WH"),
        session_parameters={"TIMEZONE": "UTC", "QUERY_TAG": "carelake"},
    )
    try:
        cur = con.cursor()
        for s in statements:
            log.info("executing: %s", s.splitlines()[0][:100])
            cur.execute(s)
    finally:
        con.close()
    return len(statements)
