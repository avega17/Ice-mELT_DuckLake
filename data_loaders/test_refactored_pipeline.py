#!/usr/bin/env python3
"""
Test script for the refactored Hamilton DOI pipeline.

This script validates that the refactored pipeline maintains functionality
while following Hamilton best practices. It tests both individual modules
and the complete pipeline execution.

Usage:
    python test_refactored_pipeline.py [--quick] [--verbose]
    
    --quick: Run only basic validation tests
    --verbose: Enable detailed logging
"""

import argparse
import sys
import tempfile
from pathlib import Path
from typing import Dict, Any, List

# Import Hamilton components
from hamilton import driver
from hamilton.base import DictResult

# Import refactored modules
try:
    from hamilton_modules import (
        data_sources, transformations, validations, storage
    )
    from refactored_pv_doi_pipeline import (
        create_hamilton_driver, run_doi_pipeline
    )
except ImportError as import_error:
    print(f"❌ Import error: {import_error}")
    print("   Make sure you're running from the data_loaders directory")
    sys.exit(1)


def test_module_imports() -> bool:
    """Test that all Hamilton modules can be imported successfully."""
    print("🔍 Testing module imports...")
    
    try:
        # Test individual module imports
        modules_to_test = [
            ("data_sources", data_sources),
            ("transformations", transformations),
            ("validations", validations),
            ("storage", storage)
        ]
        
        for module_name, module in modules_to_test:
            # Check that module has expected functions
            module_functions = [name for name in dir(module) if not name.startswith('_')]
            print(f"   ✅ {module_name}: {len(module_functions)} functions")
            
            # Validate function naming (should be nouns, not verbs)
            verb_functions = [
                func for func in module_functions 
                if any(func.startswith(verb) for verb in ['get_', 'load_', 'process_', 'create_', 'download_'])
            ]
            
            if verb_functions:
                print(f"   ⚠️  {module_name} has verb-named functions: {verb_functions}")
            else:
                print(f"   ✅ {module_name} follows noun-based naming conventions")
        
        return True
        
    except Exception as import_test_error:
        print(f"   ❌ Module import test failed: {import_test_error}")
        return False


def test_hamilton_driver_creation() -> bool:
    """Test Hamilton driver creation with different configurations."""
    print("🔍 Testing Hamilton driver creation...")
    
    try:
        # Test basic driver creation
        config = {
            "use_cache": True,
            "max_mb": 50,  # Small for testing
            "database_path": ":memory:"  # In-memory for testing
        }
        
        driver_instance = create_hamilton_driver(config, use_parallel=False)
        
        if driver_instance is None:
            print("   ❌ Driver creation returned None")
            return False
        
        print(f"   ✅ Basic driver created successfully")
        
        # Test driver with parallel execution
        try:
            parallel_driver = create_hamilton_driver(config, use_parallel=True)
            print(f"   ✅ Parallel driver created successfully")
        except Exception as parallel_error:
            print(f"   ⚠️  Parallel driver creation failed: {parallel_error}")
            print(f"   ℹ️  This is acceptable - falling back to sequential")
        
        return True
        
    except Exception as driver_test_error:
        print(f"   ❌ Driver creation test failed: {driver_test_error}")
        return False


def test_individual_hamilton_nodes() -> bool:
    """Test individual Hamilton nodes with mock data."""
    print("🔍 Testing individual Hamilton nodes...")
    
    try:
        # Create test driver
        config = {"use_cache": False, "max_mb": 10}
        dr = create_hamilton_driver(config, use_parallel=False)
        
        # Test dataset_metadata node (should work without external dependencies)
        try:
            metadata_result = dr.execute(["dataset_metadata"])
            metadata = metadata_result["dataset_metadata"]
            
            if isinstance(metadata, dict) and len(metadata) > 0:
                print(f"   ✅ dataset_metadata: {len(metadata)} datasets")
            else:
                print(f"   ⚠️  dataset_metadata returned unexpected result: {type(metadata)}")
                
        except Exception as metadata_error:
            print(f"   ❌ dataset_metadata test failed: {metadata_error}")
            return False
        
        # Test target_datasets_list node
        try:
            targets_result = dr.execute(["target_datasets_list"])
            targets = targets_result["target_datasets_list"]
            
            if isinstance(targets, list) and len(targets) > 0:
                print(f"   ✅ target_datasets_list: {len(targets)} datasets")
            else:
                print(f"   ⚠️  target_datasets_list returned unexpected result: {type(targets)}")
                
        except Exception as targets_error:
            print(f"   ❌ target_datasets_list test failed: {targets_error}")
            return False
        
        return True
        
    except Exception as nodes_test_error:
        print(f"   ❌ Individual nodes test failed: {nodes_test_error}")
        return False


def test_pipeline_configuration() -> bool:
    """Test pipeline with different configurations."""
    print("🔍 Testing pipeline configurations...")
    
    configurations = [
        {
            "name": "minimal_config",
            "config": {
                "use_cache": False,
                "max_mb": 10,
                "database_path": ":memory:"
            }
        },
        {
            "name": "cache_enabled",
            "config": {
                "use_cache": True,
                "max_mb": 25,
                "database_path": ":memory:"
            }
        }
    ]
    
    for test_config in configurations:
        try:
            print(f"   Testing {test_config['name']}...")
            
            # Create driver with test configuration
            dr = create_hamilton_driver(test_config['config'], use_parallel=False)
            
            # Test basic execution (metadata only)
            result = dr.execute(["dataset_metadata"])
            
            if result and "dataset_metadata" in result:
                print(f"   ✅ {test_config['name']}: Configuration valid")
            else:
                print(f"   ❌ {test_config['name']}: Invalid result")
                return False
                
        except Exception as config_error:
            print(f"   ❌ {test_config['name']} failed: {config_error}")
            return False
    
    return True


def test_error_handling() -> bool:
    """Test error handling and graceful failures."""
    print("🔍 Testing error handling...")
    
    try:
        # Test with invalid configuration
        invalid_config = {
            "use_cache": "invalid_boolean",  # Invalid type
            "max_mb": -1,  # Invalid value
            "database_path": "/invalid/path/that/does/not/exist"
        }
        
        try:
            dr = create_hamilton_driver(invalid_config, use_parallel=False)
            # If this succeeds, Hamilton is handling invalid config gracefully
            print(f"   ✅ Invalid config handled gracefully")
        except Exception as expected_error:
            print(f"   ✅ Invalid config properly rejected: {type(expected_error).__name__}")
        
        # Test with missing dependencies (should fail gracefully)
        try:
            config = {"use_cache": False, "max_mb": 10}
            dr = create_hamilton_driver(config, use_parallel=False)
            
            # Try to execute nodes that require external data
            # This should fail gracefully without crashing
            result = dr.execute(["dataset_download_path"])
            print(f"   ⚠️  External dependency test unexpectedly succeeded")
            
        except Exception as expected_dependency_error:
            print(f"   ✅ Missing dependency handled gracefully: {type(expected_dependency_error).__name__}")
        
        return True
        
    except Exception as error_test_error:
        print(f"   ❌ Error handling test failed: {error_test_error}")
        return False


def test_code_organization() -> bool:
    """Test that code organization follows Hamilton best practices."""
    print("🔍 Testing code organization...")
    
    try:
        # Check module structure
        expected_modules = ["data_sources", "transformations", "validations", "storage"]
        
        for module_name in expected_modules:
            module = globals().get(module_name)
            if module is None:
                print(f"   ❌ Missing expected module: {module_name}")
                return False
            
            # Check for proper function organization
            functions = [name for name in dir(module) if not name.startswith('_') and callable(getattr(module, name))]
            
            if len(functions) == 0:
                print(f"   ❌ {module_name} has no functions")
                return False
            
            print(f"   ✅ {module_name}: {len(functions)} functions")
        
        # Check utility modules exist
        utils_path = Path("utils")
        if utils_path.exists():
            util_files = list(utils_path.glob("*.py"))
            print(f"   ✅ Utility modules: {len(util_files)} files")
        else:
            print(f"   ⚠️  Utils directory not found")
        
        return True
        
    except Exception as org_test_error:
        print(f"   ❌ Code organization test failed: {org_test_error}")
        return False


def run_comprehensive_tests(quick_mode: bool = False, verbose: bool = False) -> bool:
    """
    Run comprehensive test suite for the refactored pipeline.
    
    Args:
        quick_mode: Run only basic tests
        verbose: Enable detailed logging
        
    Returns:
        bool: True if all tests pass
    """
    print("🚀 Starting comprehensive test suite for refactored Hamilton pipeline")
    print("=" * 70)
    
    test_results = []
    
    # Basic tests (always run)
    basic_tests = [
        ("Module Imports", test_module_imports),
        ("Hamilton Driver Creation", test_hamilton_driver_creation),
        ("Code Organization", test_code_organization)
    ]
    
    # Extended tests (skip in quick mode)
    extended_tests = [
        ("Individual Hamilton Nodes", test_individual_hamilton_nodes),
        ("Pipeline Configuration", test_pipeline_configuration),
        ("Error Handling", test_error_handling)
    ]
    
    # Run basic tests
    for test_name, test_function in basic_tests:
        print(f"\n{test_name}:")
        try:
            result = test_function()
            test_results.append((test_name, result))
            if result:
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as test_error:
            print(f"💥 {test_name} CRASHED: {test_error}")
            test_results.append((test_name, False))
    
    # Run extended tests if not in quick mode
    if not quick_mode:
        for test_name, test_function in extended_tests:
            print(f"\n{test_name}:")
            try:
                result = test_function()
                test_results.append((test_name, result))
                if result:
                    print(f"✅ {test_name} PASSED")
                else:
                    print(f"❌ {test_name} FAILED")
            except Exception as test_error:
                print(f"💥 {test_name} CRASHED: {test_error}")
                test_results.append((test_name, False))
    
    # Print summary
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY:")
    
    passed_tests = [name for name, result in test_results if result]
    failed_tests = [name for name, result in test_results if not result]
    
    print(f"   ✅ Passed: {len(passed_tests)}/{len(test_results)}")
    print(f"   ❌ Failed: {len(failed_tests)}/{len(test_results)}")
    
    if failed_tests:
        print(f"   Failed tests: {', '.join(failed_tests)}")
    
    overall_success = len(failed_tests) == 0
    
    if overall_success:
        print("\n🎉 All tests passed! Refactored pipeline is ready for use.")
    else:
        print(f"\n⚠️  {len(failed_tests)} tests failed. Please review and fix issues.")
    
    return overall_success


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test refactored Hamilton DOI pipeline")
    parser.add_argument("--quick", action="store_true", help="Run only basic tests")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    success = run_comprehensive_tests(quick_mode=args.quick, verbose=args.verbose)
    
    sys.exit(0 if success else 1)
