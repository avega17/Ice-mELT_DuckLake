#!/usr/bin/env python3
"""
Check what's in the DuckDB database.
"""

import duckdb

def check_database():
    """Check the contents of the DuckDB database."""
    try:
        # Check database file
        import os
        db_path = os.path.abspath('eo_pv_data.duckdb')
        print(f"Database path: {db_path}")
        print(f"Database file exists: {os.path.exists(db_path)}")
        if os.path.exists(db_path):
            print(f"Database file size: {os.path.getsize(db_path)} bytes")

        # Connect to the database
        conn = duckdb.connect(db_path)

        print("=== DuckDB Database Contents ===")

        # Use proper DuckDB syntax to list all tables
        try:
            tables = conn.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            """).fetchall()
            print(f"Found {len(tables)} table(s) total")

            if len(tables) > 0:
                for schema, table in tables:
                    try:
                        count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
                        print(f"  📊 {schema}.{table}: {count} rows")

                        # Show sample data for pv_features table
                        if table == "pv_features" and count > 0:
                            sample = conn.execute(f"SELECT dataset_name, source_file FROM {schema}.{table} LIMIT 3").fetchall()
                            print(f"    Sample data: {sample}")

                            # Show dataset breakdown
                            dataset_counts = conn.execute(f"SELECT dataset_name, COUNT(*) FROM {schema}.{table} GROUP BY dataset_name ORDER BY COUNT(*) DESC").fetchall()
                            print(f"    Dataset breakdown:")
                            for dataset, count in dataset_counts:
                                print(f"      {dataset}: {count} features")

                    except Exception as count_error:
                        print(f"  📊 {schema}.{table}: (could not count rows: {count_error})")
            else:
                print("No tables found in database.")

                # Check if GeoParquet files exist
                from pathlib import Path
                geoparquet_dir = Path("datasets/raw/geoparquet")
                if geoparquet_dir.exists():
                    parquet_files = list(geoparquet_dir.glob("*.parquet"))
                    print(f"\nGeoParquet files available: {len(parquet_files)}")
                    for pf in parquet_files:
                        print(f"  - {pf.name} ({pf.stat().st_size / 1024:.1f} KB)")
                else:
                    print("No GeoParquet files found either.")

        except Exception as e:
            print(f"Error querying database: {e}")
            print("Database might be empty or corrupted.")

        for table in tables:
            table_name = table[0]
            print(f"\nTable: {table_name}")

            # Get row count
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
            print(f"  Rows: {count}")

            # Show first few rows if any data exists
            if count > 0:
                print("  Sample data:")
                sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3;").fetchall()
                for row in sample:
                    print(f"    {row}")

                # Show column info
                columns = conn.execute(f"DESCRIBE {table_name};").fetchall()
                print(f"  Columns: {[col[0] for col in columns]}")

                # If it's the pv_features table, show some stats
                if table_name == "pv_features":
                    print("  Dataset breakdown:")
                    dataset_counts = conn.execute(f"SELECT dataset_name, COUNT(*) FROM {table_name} GROUP BY dataset_name ORDER BY COUNT(*) DESC;").fetchall()
                    for dataset, count in dataset_counts:
                        print(f"    {dataset}: {count} features")

        conn.close()

    except Exception as e:
        print(f"Error checking database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_database()
