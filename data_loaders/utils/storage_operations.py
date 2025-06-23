"""
Storage operations utility module.

This module contains utility functions for storing data in DuckDB,
managing database schemas, and handling Arrow table storage operations.

These are utility functions (not Hamilton nodes) that implement the actual
storage logic called by Hamilton nodes.
"""

import duckdb
import pyarrow as pa
import geopandas as gpd
from typing import Dict, Any, List


def store_datasets_in_duckdb(
    arrow_tables: List[pa.Table],
    processing_reports: List[Dict[str, Any]],
    database_path: str
) -> Dict[str, str]:
    """
    Store multiple datasets in DuckDB with validation checks.
    
    Args:
        arrow_tables: List of Arrow tables to store
        processing_reports: List of processing reports for validation
        database_path: Path to DuckDB database
        
    Returns:
        Dict mapping dataset names to storage status
    """
    if not arrow_tables:
        raise ValueError("No datasets were processed successfully")

    stored_tables = {}

    with duckdb.connect(database_path) as conn:
        # Install required extensions
        conn.execute("INSTALL spatial; LOAD spatial;")
        
        # Create raw schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw_data")

        # Create reports mapping for validation
        reports_by_dataset = {
            report.get("dataset_name", "unknown"): report 
            for report in processing_reports
        }

        for table in arrow_tables:
            # Extract dataset name from table metadata
            dataset_name = "unknown_dataset"
            if hasattr(table, 'schema') and table.schema.metadata:
                metadata = table.schema.metadata
                if b'dataset_name' in metadata:
                    dataset_name = metadata[b'dataset_name'].decode('utf-8')

            # Check if dataset passed validation
            report = reports_by_dataset.get(dataset_name, {})
            ready_for_storage = report.get("ready_for_storage", False)
            
            if not ready_for_storage:
                print(f"⚠️  Skipping {dataset_name} - failed validation checks")
                stored_tables[dataset_name] = "skipped - validation failed"
                continue

            # Create table name for raw storage
            table_name = f"raw_data.doi_{dataset_name}"

            try:
                # Use safe table creation function
                row_count = create_duckdb_table_from_arrow_safe(
                    arrow_table=table,
                    processing_report=report,
                    dataset_name=dataset_name,
                    database_path=database_path,
                    table_name=table_name,
                    conn=conn
                )
                stored_tables[dataset_name] = f"{table_name} ({row_count} rows) - success"
                print(f"✅ Stored {dataset_name} as {table_name}: {row_count} rows")

            except Exception as store_error:
                print(f"❌ Failed to store {dataset_name}: {store_error}")
                stored_tables[dataset_name] = f"failed - {str(store_error)}"
                continue

    print(f"📊 Stored {len([s for s in stored_tables.values() if 'success' in s])} datasets successfully")
    return stored_tables


def create_duckdb_table_from_arrow_safe(
    arrow_table: pa.Table,
    processing_report: Dict[str, Any],
    dataset_name: str,
    database_path: str,
    table_name: str = None,
    conn: duckdb.DuckDBPyConnection = None
) -> Dict[str, Any]:
    """
    Safely create DuckDB table from Arrow table with validation.
    
    This is the refactored version of create_duckdb_table_from_arrow that follows
    Hamilton best practices with proper error handling and validation.
    
    Args:
        arrow_table: Arrow table to store
        processing_report: Processing report with validation results
        dataset_name: Name of the dataset
        database_path: Path to DuckDB database
        table_name: Optional table name (auto-generated if not provided)
        conn: Optional existing connection (creates new if not provided)
        
    Returns:
        Dict with creation results and metadata
    """
    # Check validation status
    if not processing_report.get("ready_for_storage", False):
        raise ValueError(f"Dataset {dataset_name} not ready for storage - validation failed")
    
    # Use provided connection or create new one
    should_close_conn = conn is None
    if conn is None:
        conn = duckdb.connect(database_path)
        conn.execute("INSTALL spatial; LOAD spatial;")
    
    try:
        # Generate table name if not provided
        if table_name is None:
            table_name = f"raw_data.doi_{dataset_name}"
        
        # Drop existing table
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        print(f"   🔄 Creating DuckDB table: {table_name}")
        
        # Use simplified approach with graceful fallback
        row_count = _create_table_with_fallback(conn, arrow_table, table_name)
        
        result = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "row_count": row_count,
            "status": "success",
            "validation_passed": True
        }
        
        print(f"   ✅ Successfully created table {table_name} with {row_count} rows")
        return result
        
    except Exception as creation_error:
        error_result = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "row_count": 0,
            "status": "failed",
            "error": str(creation_error),
            "validation_passed": False
        }
        print(f"   ❌ Failed to create table {table_name}: {creation_error}")
        raise creation_error
        
    finally:
        if should_close_conn and conn:
            conn.close()


def _create_table_with_fallback(
    conn: duckdb.DuckDBPyConnection,
    arrow_table: pa.Table,
    table_name: str
) -> int:
    """
    Create DuckDB table with simplified approach.

    Uses a clear strategy without complex fallback chains:
    1. Try direct Arrow to DuckDB if no geometry
    2. Try pandas conversion with geometry handling
    3. Gracefully handle any conversion issues
    """
    # Check if table has geometry columns
    geometry_columns = [col for col in arrow_table.column_names if 'geometry' in col.lower()]

    if not geometry_columns:
        # No geometry - use direct Arrow to DuckDB
        return _create_table_direct_arrow(conn, arrow_table, table_name)
    else:
        # Has geometry - use pandas with geometry handling
        return _create_table_with_geometry(conn, arrow_table, table_name)


def _create_table_direct_arrow(
    conn: duckdb.DuckDBPyConnection,
    arrow_table: pa.Table,
    table_name: str
) -> int:
    """Create table directly from Arrow table (no geometry)."""
    print(f"   🔄 Creating table directly from Arrow (no geometry)")

    try:
        # Use DuckDB's native Arrow support
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_table")
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"   ✅ Direct Arrow conversion successful: {row_count} rows")
        return row_count

    except Exception as direct_error:
        print(f"   ⚠️  Direct Arrow conversion failed: {direct_error}")
        # Fallback to pandas conversion
        df = arrow_table.to_pandas()
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"   ✅ Pandas fallback successful: {row_count} rows")
        return row_count


def _create_table_with_geometry(
    conn: duckdb.DuckDBPyConnection,
    arrow_table: pa.Table,
    table_name: str
) -> int:
    """Create table with geometry handling using simplified approach."""
    print(f"   🔄 Converting Arrow table with geometry handling")

    # Convert to pandas
    df = arrow_table.to_pandas()

    # Simplified geometry handling strategy
    if 'geometry_wkt' in df.columns:
        # WKT geometry - most reliable approach
        print(f"   🗺️  Using WKT geometry column")
        conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT
                * EXCLUDE (geometry_wkt),
                CASE
                    WHEN geometry_wkt IS NOT NULL AND geometry_wkt != ''
                    THEN ST_GeomFromText(geometry_wkt)
                    ELSE NULL
                END as geometry
            FROM df
        """)
    elif 'geometry' in df.columns:
        # Try to handle geometry column
        print(f"   🗺️  Attempting geometry column conversion")
        sample_val = df['geometry'].iloc[0] if len(df) > 0 else None

        if isinstance(sample_val, bytes):
            # WKB geometry
            conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT
                    * EXCLUDE (geometry),
                    CASE
                        WHEN geometry IS NOT NULL
                        THEN ST_GeomFromWKB(geometry)
                        ELSE NULL
                    END as geometry
                FROM df
            """)
        else:
            # Cannot reliably convert - store without geometry conversion
            print(f"   ⚠️  Cannot convert geometry column - storing as regular column")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    else:
        # No geometry columns found
        print(f"   📄 No geometry columns found - creating regular table")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"   ✅ Table creation successful: {row_count} rows")
    return row_count
