#!/usr/bin/env python3
"""
Simple script to run Hamilton staging DAG if staging tables don't exist.
Can be triggered by dbt pre-hooks or run manually.

Usage:
    python scripts/build_with_staging.py [dev|prod]
"""

import os
import sys
import subprocess
import argparse
import duckdb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_staging_tables_exist(db_conn: str) -> int:
    """
    Check if staging tables exist using direct DuckDB query.

    Args:
        db_conn: Database connection string

    Returns:
        int: Number of staging tables found
    """
    try:
        if db_conn.startswith('ducklake:postgres:'):
            # For PostgreSQL catalog, use DuckDB with ducklake extension
            con = duckdb.connect()
            con.execute("INSTALL ducklake FROM community")
            con.execute("LOAD ducklake")
            con.execute(f"ATTACH '{db_conn}' AS ducklake_catalog")

            result = con.execute("""
                SELECT COUNT(*)
                FROM ducklake_catalog.information_schema.tables
                WHERE table_name LIKE 'stg_%'
                  AND table_name NOT LIKE 'stg_pv_consolidated%'
            """).fetchone()

        else:
            # For SQLite catalog, use DuckDB with ducklake extension
            con = duckdb.connect()
            con.execute("INSTALL ducklake FROM community")
            con.execute("LOAD ducklake")
            con.execute(f"ATTACH '{db_conn}' AS ducklake_catalog")

            result = con.execute("""
                SELECT COUNT(*)
                FROM ducklake_catalog.information_schema.tables
                WHERE table_name LIKE 'stg_%'
                  AND table_name NOT LIKE 'stg_pv_consolidated%'
            """).fetchone()

        return result[0] if result else 0

    except Exception as e:
        print(f"⚠️  Could not check staging tables: {e}")
        return 0


def run_staging_dag(target: str) -> bool:
    """
    Run Hamilton staging DAG with appropriate configuration.

    Args:
        target: dbt target (dev or prod)

    Returns:
        bool: True if successful, False otherwise
    """
    # Determine database connection based on target
    if target == "prod":
        db_conn = os.getenv("DUCKLAKE_PROD_CONN")
        print("🌩️  Running staging DAG with PostgreSQL catalog")
    else:
        db_conn = os.getenv("DUCKLAKE_DEV_CONN")
        print("💾 Running staging DAG with SQLite catalog")

    if not db_conn:
        print(f"❌ Database connection not configured for target: {target}")
        return False

    # Run staging DAG
    cmd = [
        "python", "data_loaders/stg/consolidation/val_stg_consolidation.py",
        "--sequential",
        "--database", db_conn,
        "--target-table", "stg_pv_consolidated",
        "--verbose"
    ]

    try:
        subprocess.run(cmd, check=True)
        print("✅ Hamilton staging DAG completed successfully")
        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ Hamilton staging DAG failed: {e}")
        return False


def main():
    """Check staging tables and run Hamilton DAG if needed."""

    parser = argparse.ArgumentParser(description="Prepare staging tables for dbt")
    parser.add_argument(
        "target",
        nargs="?",
        default="dev",
        choices=["dev", "prod"],
        help="dbt target environment"
    )

    args = parser.parse_args()
    target = args.target

    # Get database connection for target
    if target == "prod":
        db_conn = os.getenv("DUCKLAKE_PROD_CONN")
    else:
        db_conn = os.getenv("DUCKLAKE_DEV_CONN")

    if not db_conn:
        print(f"❌ Database connection not configured for target: {target}")
        sys.exit(1)

    # Check if staging tables exist
    staging_count = check_staging_tables_exist(db_conn)

    if staging_count == 0:
        print("🚨 No staging tables found, running Hamilton staging DAG...")

        # Run staging DAG
        if not run_staging_dag(target):
            print("❌ Failed to create staging tables")
            sys.exit(1)

    else:
        print(f"✅ Found {staging_count} existing staging tables, skipping DAG execution")

    print("✅ Staging tables ready for dbt build")


if __name__ == "__main__":
    main()
