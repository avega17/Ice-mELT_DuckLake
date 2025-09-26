"""
Centralized DuckLake connection helper for DuckDB.

- Attaches a DuckLake catalog and loads essential extensions.
- Uses environment variables for connection string/Data Path and S3/R2 credentials.
- Minimal implementation; assumes env is configured correctly.
"""
from __future__ import annotations

import os
import duckdb


def _create_ducklake_connection(
    *,
    target: str | None = None,
    attach_alias: str = "eo_pv_lakehouse",
) -> duckdb.DuckDBPyConnection:
    """
    Create and return a DuckDB connection attached to a DuckLake catalog.

    Env vars expected:
        DUCKLAKE_ATTACH_DEV  (e.g., ducklake:postgres:dbname=... host=... user=... password=... sslmode=require)
        DUCKLAKE_ATTACH_PROD (same format as above)
        DUCKLAKE_DATA_PATH (e.g., r2://bucket/ducklake_data)
        R2_ACCESS_KEY_ID, R2_SECRET_KEY, optional S3_REGION
    """
    target_name = target or os.getenv("DBT_TARGET", "dev")
    
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("INSTALL h3 FROM community; LOAD h3;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL cache_httpfs; LOAD cache_httpfs;")

    # Configure S3/R2 if credentials exist
    if (ak := os.getenv("R2_ACCESS_KEY_ID")) and (sk := os.getenv("R2_SECRET_KEY")):
        conn.execute("SET s3_access_key_id=?", [ak])
        conn.execute("SET s3_secret_access_key=?", [sk])
        # change endpoint to cloudflare
        r2_endpoint = os.getenv("R2_S3_ENDPOINT", "e833ac2d32c62bcff5e4b72c74e5351d.r2.cloudflarestorage.com")
        conn.execute(f"SET s3_endpoint='{r2_endpoint}';")
        # if (region := os.getenv("S3_REGION")):
        #     conn.execute("SET s3_region=?", [region])
        # if (url_style := os.getenv("R2_URL_STYLE")):
        conn.execute("SET s3_url_style='path';")
        conn.execute("SET s3_use_ssl=true;")

    attach_base = (
        os.getenv("DUCKLAKE_ATTACH_PROD") if target_name == "prod" else os.getenv("DUCKLAKE_ATTACH_DEV")
    )
    if not attach_base:
        raise RuntimeError("DuckLake attach base env not set for target (DUCKLAKE_ATTACH_*)")

    data_path = os.getenv("DUCKLAKE_DATA_PATH")
    attach_sql = (
        f"ATTACH '{attach_base}' AS {attach_alias} (DATA_PATH '{data_path}');"
        if data_path
        else f"ATTACH '{attach_base}' AS {attach_alias};"
    )

    print(f"Attaching DuckLake catalog using base connection:\n{attach_base}")
    # Execute attach
    conn.execute(attach_sql)
    conn.execute(f"USE {attach_alias};")

    return conn

# Backwards-compatible alias without underscore
create_ducklake_connection = _create_ducklake_connection

