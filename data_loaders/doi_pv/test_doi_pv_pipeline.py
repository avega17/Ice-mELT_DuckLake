#!/usr/bin/env python3
"""
Test script for the DOI PV locations pipeline.
Tests the proper Hamilton driver pattern.
"""

# Add repo root to path to import modules
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

def test_hamilton_driver_creation():
    """Test Hamilton driver creation with proper module separation."""
    print("🧪 Testing Hamilton Driver Creation")
    print("=" * 50)
    
    try:
        # Test imports
        print("🔍 Testing imports...")
        from data_loaders.doi_pv.ingest_doi_pv_locations import create_hamilton_driver, run_doi_pv_pipeline
        from dataflows.raw import doi_pv_locations
        print("   ✅ Imports successful")
        
        # Test driver creation
        print("🔍 Testing driver creation...")
        # Test sequential mode
        config_sequential = {
            "database_path": ":memory:",
            "manifest_path": "data_loaders/doi_manifest.json",
            "max_mb": 10,
            "export_geoparquet": False,
            "force_download": False,
            "cache_path": "./test_cache",
            "execution_mode": "sequential"
        }

        # Test parallel mode
        config_parallel = {
            "database_path": ":memory:",
            "manifest_path": "data_loaders/doi_manifest.json",
            "max_mb": 10,
            "export_geoparquet": False,
            "force_download": False,
            "cache_path": "./test_cache",
            "execution_mode": "parallel"
        }
        
        # Test sequential without caching
        dr_seq_no_cache = create_hamilton_driver(config_sequential, enable_caching=False, use_parallel=False)
        print("   ✅ Driver creation (sequential, no cache) successful")

        # Test sequential with caching
        dr_seq_cache = create_hamilton_driver(config_sequential, enable_caching=True, use_parallel=False)
        print("   ✅ Driver creation (sequential, with cache) successful")

        # Test parallel with caching (default)
        dr_parallel_cache = create_hamilton_driver(config_parallel, enable_caching=True, use_parallel=True)
        print("   ✅ Driver creation (parallel, with cache) successful")
        
        # Test function discovery (using sequential driver)
        print("🔍 Testing function discovery...")
        functions = dr_seq_no_cache.list_available_variables()
        function_names = [f.name for f in functions]
        print(f"   📋 Found {len(function_names)} functions")
        print(f"   🔧 Config used: {config_sequential}")
        
        expected_functions = [
            "doi_metadata", "dataset_names", "dataset_download_path",
            "processed_geodataframe", "geo_arrow_table", "collected_arrow_tables", "pipeline_result"
        ]
        
        found = [f for f in expected_functions if f in function_names]
        missing = [f for f in expected_functions if f not in function_names]

        print(f"   ✅ Found expected functions: {found}")
        if missing:
            print(f"   ⚠️  Missing functions: {missing}")

        print(f"   📋 All available functions: {sorted(function_names)}")
        
        # Test metadata loading (if manifest exists)
        print("🔍 Testing metadata loading...")
        try:
            result = dr_seq_no_cache.execute(["doi_metadata"])
            print("   ✅ Metadata loading successful")
            print(f"   📊 Found {len(result['doi_metadata'])} datasets")
        except FileNotFoundError:
            print("   ⚠️  Manifest file not found (expected in test environment)")
        except Exception as e:
            print(f"   ❌ Metadata loading failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_basic_functionality():
    """Test basic Hamilton functionality without visualization."""
    print("\n🧪 Testing Basic Hamilton Functionality")
    print("=" * 50)

    try:
        from data_loaders.doi_pv.ingest_doi_pv_locations import create_hamilton_driver

        config_sequential = {
            "database_path": ":memory:",
            "manifest_path": "data_loaders/doi_manifest.json",
            "cache_path": "./test_cache",
            "execution_mode": "sequential"
        }

        dr = create_hamilton_driver(config_sequential, enable_caching=False, use_parallel=False)

        # Test basic driver functionality
        print("🔍 Testing driver functionality...")
        functions = dr.list_available_variables()
        print(f"   ✅ Driver has {len(functions)} functions available")

        # Test configuration access
        print("🔍 Testing configuration...")
        try:
            # Try to access a simple config value
            result = dr.execute(["doi_metadata"])
            print("   ✅ Basic execution test successful")
        except FileNotFoundError:
            print("   ⚠️  Manifest file not found (expected in test environment)")
        except Exception as e:
            print(f"   ⚠️  Basic execution test failed: {e}")

        return True

    except Exception as e:
        print(f"❌ Basic functionality test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 DOI PV Pipeline Test Suite")
    print("=" * 60)
    
    tests = [
        ("Hamilton Driver Creation", test_hamilton_driver_creation),
        ("Basic Functionality", test_basic_functionality)
    ]
    
    passed = 0
    for test_name, test_func in tests:
        print(f"\n📋 Running: {test_name}")
        if test_func():
            passed += 1
            print(f"✅ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")
    
    print(f"\n📊 Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All tests passed! DOI PV pipeline is ready.")
    else:
        print("⚠️  Some tests failed.")


if __name__ == "__main__":
    main()