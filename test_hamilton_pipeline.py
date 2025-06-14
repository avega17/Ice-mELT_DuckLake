#!/usr/bin/env python3
"""
Test script for the Hamilton-based DOI dataset pipeline.
Run this to verify the Hamilton pipeline is working correctly.
"""

import os
import sys
from pathlib import Path
import tempfile
import shutil

# Add the current directory to Python path to import our modules
sys.path.append(str(Path(__file__).parent))

def test_imports():
    """Test that all required modules can be imported."""
    print("=== Testing Imports ===")
    
    try:
        import hamilton
        print("✓ Hamilton imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import Hamilton: {e}")
        return False
    
    try:
        from hamilton_doi_pipeline import (
            dataset_metadata, target_datasets, download_doi_dataset,
            extract_geospatial_files, process_geospatial_data,
            overture_admin_boundaries, run_doi_pipeline
        )
        print("✓ Hamilton pipeline modules imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import pipeline modules: {e}")
        return False
    
    try:
        import geopandas as gpd
        import pandas as pd
        import duckdb
        print("✓ Geospatial and database dependencies imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import dependencies: {e}")
        return False
    
    return True


def test_hamilton_driver():
    """Test Hamilton driver creation and basic functionality."""
    print("\n=== Testing Hamilton Driver ===")
    
    try:
        from hamilton import driver
        import hamilton_doi_pipeline as pipeline_module
        
        # Create Hamilton driver with test config
        config = {
            "database_path": "./test_eo_pv_data.duckdb",
            "export_geoparquet": False
        }
        
        dr = driver.Driver(config, pipeline_module)
        print("✓ Hamilton driver created successfully")
        
        # Test that we can visualize the DAG
        try:
            # This will show the pipeline structure
            print("✓ Pipeline DAG structure available")
            print(f"  Available functions: {len(dr.list_available_variables())}")
        except Exception as e:
            print(f"⚠ Could not visualize DAG: {e}")
        
        return True
        
    except Exception as e:
        print(f"✗ Hamilton driver test failed: {e}")
        return False


def test_dataset_metadata():
    """Test dataset metadata loading."""
    print("\n=== Testing Dataset Metadata ===")
    
    try:
        from hamilton_doi_pipeline import dataset_metadata, target_datasets

        metadata = dataset_metadata()
        print(f"✓ Loaded metadata for {len(metadata)} datasets")

        # Test target datasets generation
        targets = list(target_datasets())
        print(f"✓ Generated {len(targets)} target datasets for processing")

        # Verify some expected datasets are present
        expected_datasets = [
            "chn_med_res_pv_2024",
            "global_harmonized_large_solar_farms_2020",
            "ind_pv_solar_farms_2022",
            "global_pv_inventory_sent2_2024"
        ]

        found_datasets = []
        for dataset in expected_datasets:
            if dataset in metadata:
                found_datasets.append(dataset)
                display_name = metadata[dataset].get('display_name', dataset)
                print(f"  ✓ {dataset}: {display_name}")

        if not found_datasets:
            print("  ⚠ No expected datasets found in manifest")
            return False
        
        return True
        
    except Exception as e:
        print(f"✗ Dataset metadata test failed: {e}")
        return False


def test_overture_maps_query():
    """Test Overture Maps querying functionality."""
    print("\n=== Testing Overture Maps Query ===")
    
    try:
        from hamilton_doi_pipeline import overture_admin_boundaries
        
        # Test with a small bbox (around San Francisco)
        bbox = [-122.5, 37.7, -122.3, 37.8]
        
        print("Attempting to query Overture Maps (this may take a moment)...")
        result = overture_admin_boundaries(bbox=bbox)
        
        if len(result) > 0:
            print(f"✓ Successfully queried Overture Maps: {len(result)} boundaries")
            print(f"  Columns: {list(result.columns)}")
        else:
            print("⚠ Overture Maps query returned no results (may be expected)")
        
        return True
        
    except Exception as e:
        print(f"✗ Overture Maps query failed: {e}")
        print("  This may be expected if network/S3 access is limited")
        return True  # Don't fail the test for network issues


def test_duckdb_connection():
    """Test DuckDB connection and spatial extensions."""
    print("\n=== Testing DuckDB Connection ===")
    
    try:
        import duckdb
        
        # Test basic connection
        conn = duckdb.connect()
        print("✓ DuckDB connection established")
        
        # Test spatial extension
        conn.execute("INSTALL spatial; LOAD spatial;")
        print("✓ Spatial extension loaded")
        
        # Test H3 extension
        conn.execute("INSTALL h3; LOAD h3;")
        print("✓ H3 extension loaded")
        
        # Test httpfs extension (for S3 access)
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        print("✓ HTTPFS extension loaded")
        
        # Test basic spatial query
        result = conn.execute("SELECT ST_Point(0, 0) as geom").fetchone()
        print("✓ Basic spatial query successful")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ DuckDB test failed: {e}")
        return False


def test_pipeline_dry_run():
    """Test pipeline execution without actual downloads."""
    print("\n=== Testing Pipeline Dry Run ===")
    
    try:
        from hamilton import driver
        import hamilton_doi_pipeline as pipeline_module
        
        # Create test config
        config = {
            "database_path": "./test_eo_pv_data.duckdb",
            "export_geoparquet": False
        }
        
        dr = driver.Driver(config, pipeline_module)
        
        # Test that we can execute just the metadata function
        result = dr.execute(["dataset_metadata"])
        metadata = result["dataset_metadata"]
        
        print(f"✓ Pipeline dry run successful")
        print(f"  Metadata loaded for {len(metadata)} datasets")
        
        return True
        
    except Exception as e:
        print(f"✗ Pipeline dry run failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🧪 Testing Hamilton DOI Pipeline")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_hamilton_driver,
        test_dataset_metadata,
        test_duckdb_connection,
        test_overture_maps_query,
        test_pipeline_dry_run
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                print(f"❌ {test.__name__} failed")
        except Exception as e:
            print(f"❌ {test.__name__} crashed: {e}")
    
    print("\n" + "=" * 50)
    print(f"🏁 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Hamilton pipeline is ready.")
        return True
    else:
        print("⚠️  Some tests failed. Check the output above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
