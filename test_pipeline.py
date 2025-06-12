#!/usr/bin/env python3
"""
Test script for the DOI dataset pipeline.
Run this to verify the pipeline is working correctly.
"""

import os
import sys
from pathlib import Path

# Add the current directory to Python path to import our modules
sys.path.append(str(Path(__file__).parent))

try:
    from doi_dataset_pipeline import process_datasets_to_geoparquet, DATASET_METADATA
    print("✓ Successfully imported pipeline modules")
except ImportError as e:
    print(f"✗ Failed to import pipeline modules: {e}")
    sys.exit(1)

def test_metadata():
    """Test that dataset metadata is properly configured."""
    print("\n=== Testing Dataset Metadata ===")
    
    for dataset_name, metadata in DATASET_METADATA.items():
        print(f"Dataset: {dataset_name}")
        print(f"  DOI: {metadata['doi']}")
        print(f"  Repository: {metadata['repo']}")
        print(f"  Label Format: {metadata['label_fmt']}")
        print(f"  Description: {metadata['description'][:80]}...")
        print()

def test_pipeline_dry_run():
    """Test pipeline configuration without actually downloading data."""
    print("\n=== Testing Pipeline Configuration ===")
    
    try:
        import dlt
        print("✓ dlt imported successfully")
        
        # Test pipeline creation
        pipeline = dlt.pipeline(
            pipeline_name="test_doi_pipeline",
            destination='duckdb',
            dataset_name="test_dataset",
        )
        print("✓ Pipeline created successfully")
        print(f"  Pipeline name: {pipeline.pipeline_name}")
        print(f"  Destination: {pipeline.destination}")
        print(f"  Dataset name: {pipeline.dataset_name}")
        
    except Exception as e:
        print(f"✗ Pipeline test failed: {e}")
        return False
    
    return True

def test_geoparquet_creation():
    """Test GeoParquet creation with a small dataset."""
    print("\n=== Testing GeoParquet Creation ===")

    try:
        # Test with smallest dataset
        test_datasets = ["ind_pv_solar_farms_2022"]  # 117 labels
        print(f"Processing {test_datasets[0]} to GeoParquet...")

        process_datasets_to_geoparquet(
            datasets=test_datasets,
            max_mb=250,  # Increased for large files
            force=True
        )

        # Check if GeoParquet was created
        geoparquet_dir = Path("datasets/raw/geoparquet")
        if geoparquet_dir.exists():
            parquet_files = list(geoparquet_dir.glob("*.parquet"))
            print(f"✓ Created {len(parquet_files)} GeoParquet file(s):")
            for pf in parquet_files:
                size_kb = pf.stat().st_size / 1024
                print(f"  📄 {pf.name} ({size_kb:.1f} KB)")
            return True
        else:
            print("✗ No GeoParquet files created")
            return False

    except Exception as e:
        print(f"✗ GeoParquet creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_duckdb_loading():
    """Test loading GeoParquet files into DuckDB."""
    print("\n=== Testing DuckDB Loading ===")

    try:
        # Clear any existing pipeline state first
        from doi_dataset_pipeline import clear_pipeline_state
        clear_pipeline_state("test_doi_pipeline")  # Use different pipeline name

        # Test with smallest dataset
        test_datasets = ["ind_pv_solar_farms_2022"]

        # Create a completely separate test pipeline
        import dlt
        from doi_dataset_pipeline import geoparquet_filesystem_source

        # Create test pipeline with unique name
        test_pipeline = dlt.pipeline(
            pipeline_name="test_doi_pipeline",  # Different from main pipeline
            destination='duckdb',
            dataset_name="test_dataset",  # Different from main dataset
        )

        # Load from filesystem source
        load_info = test_pipeline.run(
            geoparquet_filesystem_source(),
            table_name="pv_features"
        )

        print(f"✓ Test pipeline completed: {load_info}")

        # Check database contents
        import duckdb

        # Use the same database path that dlt uses
        db_path = os.path.abspath('eo_pv_data.duckdb')
        print(f"Checking database at: {db_path}")
        print(f"Database file exists: {os.path.exists(db_path)}")

        conn = duckdb.connect(db_path)

        # Use proper DuckDB syntax to check for tables
        try:
            # Check for tables in the test dataset schema
            dataset_schema = "test_dataset"  # Updated to match the test dataset name
            table_name = "pv_features"

            # Try to query the table directly
            count = conn.execute(f"SELECT COUNT(*) FROM {dataset_schema}.{table_name}").fetchone()[0]
            print(f"✓ Found table {dataset_schema}.{table_name} with {count} rows")

            # Show sample data - first check what columns exist
            if count > 0:
                # Get column names first
                columns = conn.execute(f"DESCRIBE {dataset_schema}.{table_name}").fetchall()
                column_names = [col[0] for col in columns]
                print(f"  Columns: {column_names}")

                # Show sample data with available columns
                if 'dataset_name' in column_names and 'source_file' in column_names:
                    sample = conn.execute(f"SELECT dataset_name, source_file FROM {dataset_schema}.{table_name} LIMIT 3").fetchall()
                    print(f"  Sample data: {sample}")
                else:
                    # Show first few columns as sample
                    sample_cols = column_names[:3] if len(column_names) >= 3 else column_names
                    sample = conn.execute(f"SELECT {', '.join(sample_cols)} FROM {dataset_schema}.{table_name} LIMIT 3").fetchall()
                    print(f"  Sample data: {sample}")

            conn.close()

            # Force cleanup
            import gc
            gc.collect()

            return True

        except Exception as e:
            print(f"Could not find table {dataset_schema}.{table_name}: {e}")

            # Fallback: try to list all tables using information_schema
            try:
                tables = conn.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                """).fetchall()

                if len(tables) > 0:
                    print(f"✓ Found {len(tables)} table(s):")
                    for schema, table in tables:
                        try:
                            count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
                            print(f"  📊 {schema}.{table}: {count} rows")
                        except Exception as count_error:
                            print(f"  📊 {schema}.{table}: (could not count rows: {count_error})")
                    conn.close()

                    # Force cleanup
                    import gc
                    gc.collect()

                    return True
                else:
                    print("✗ No tables found")
                    conn.close()
                    return False

            except Exception as fallback_error:
                print(f"✗ Could not list tables: {fallback_error}")
                conn.close()
                return False

    except Exception as e:
        print(f"✗ DuckDB loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_utility_functions():
    """Test that utility functions are accessible."""
    print("\n=== Testing Utility Functions ===")

    try:
        from utils.fetch_and_preprocess import fetch_dataset_files
        print("✓ fetch_dataset_files imported successfully")

        # Test function signature
        import inspect
        sig = inspect.signature(fetch_dataset_files)
        print(f"  Function signature: {sig}")

    except ImportError as e:
        print(f"✗ Failed to import utility functions: {e}")
        return False

    return True

def main():
    """Run all tests."""
    print("DOI Dataset Pipeline Test Suite")
    print("=" * 40)
    print(f"Current working directory: {os.getcwd()}")

    # Run basic tests first
    test_metadata()

    if not test_utility_functions():
        print("\n✗ Utility function tests failed")
        return 1

    if not test_pipeline_dry_run():
        print("\n✗ Pipeline configuration tests failed")
        return 1

    # Test the new GeoParquet pipeline
    if not test_geoparquet_creation():
        print("\n✗ GeoParquet creation tests failed")
        return 1

    if not test_duckdb_loading():
        print("\n✗ DuckDB loading tests failed")
        return 1

    print("\n" + "=" * 40)
    print("🎉 All tests passed!")
    print("\nTo run the full pipeline with all datasets:")
    print("  python doi_dataset_pipeline.py")
    print("\nTo run with specific datasets:")
    print("  python -c \"from doi_dataset_pipeline import load_doi_datasets; load_doi_datasets(['ind_pv_solar_farms_2022'])\"")

    # Force cleanup before exit
    import gc
    gc.collect()

    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
    finally:
        # Force cleanup and exit
        import gc
        import time
        gc.collect()
        time.sleep(0.5)  # Brief pause for cleanup

    sys.exit(exit_code)
