#!/usr/bin/env python3
"""
Refactored Hamilton-based DOI dataset pipeline for EO PV data ingestion.

This is the new main orchestration file that uses the modular Hamilton components
following best practices for dependency injection, naming conventions, and code organization.

Key improvements over the original raw_pv_doi_ingest.py:
- Modular architecture with separate concerns
- Proper Hamilton dependency injection
- Noun-based function naming
- Simplified error handling
- Clear separation of Hamilton nodes and utility functions
- Preparation for dbt integration

Usage:
    python refactored_pv_doi_pipeline.py [options]
    
    # Or use Hamilton CLI
    hamilton run data_loaders.refactored_pv_doi_pipeline --config database_path=../db/eo_pv_data.duckdb
"""

import argparse
import os
from pathlib import Path
from typing import Dict, Any

from hamilton import driver
from hamilton.execution import executors
from hamilton.base import DictResult

# Import modular Hamilton components
from hamilton_modules import (
    data_sources, transformations, validations, storage
)

# Import utility functions for cache management
try:
    from utils.ingestion_utils import (
        load_download_cache, clear_all_cache, print_raw_data_summary
    )
except ImportError:
    # Fallback import
    from ingestion_utils import (
        load_download_cache, clear_all_cache, print_raw_data_summary
    )


def create_hamilton_driver(
    config: Dict[str, Any],
    use_parallel: bool = True,
    enable_caching: bool = True
) -> driver.Driver:
    """
    Create Hamilton driver with modular components and built-in caching.

    Args:
        config: Configuration dictionary for the pipeline
        use_parallel: Whether to use parallel execution capabilities
        enable_caching: Whether to enable Hamilton's built-in caching

    Returns:
        driver.Driver: Configured Hamilton driver with caching
    """
    cache_status = "with caching" if enable_caching else "without caching"
    execution_mode = "parallel" if use_parallel else "sequential"
    print(f"🔧 Creating Hamilton driver with {execution_mode} execution {cache_status}...")

    # Define modules to include in the DAG
    hamilton_modules = [
        data_sources,
        transformations,
        validations,
        storage
    ]

    try:
        # Start with base builder
        builder = driver.Builder().with_modules(*hamilton_modules).with_config(config)

        # Add caching if enabled
        if enable_caching:
            # Use Hamilton's built-in caching with intelligent defaults
            cache_config = {
                "path": config.get("cache_path", "./.hamilton_cache")  # Cache directory
            }

            # Handle force_download by setting recompute behavior
            if config.get("force_download", False):
                # Force recompute for all nodes when force_download=True
                cache_config["recompute"] = True
                print("   🔄 Force download enabled - will recompute all cached nodes")
            # Note: Don't set recompute=False as it causes "bool is not iterable" error

            builder = builder.with_cache(**cache_config)
            print(f"   📦 Enabled Hamilton caching at: {cache_config['path']}")

        if use_parallel:
            # Add parallel execution capabilities
            builder = (
                builder
                .enable_dynamic_execution(allow_experimental_mode=True)
                .with_local_executor(executors.SynchronousLocalTaskExecutor())
            )

        dr = builder.build()

        execution_type = "parallel" if use_parallel else "sequential"
        cache_type = "with Hamilton caching" if enable_caching else "without caching"
        print(f"✅ Created Hamilton driver: {execution_type} execution {cache_type}")

        return dr
        
    except Exception as builder_error:
        print(f"❌ Failed to create Hamilton driver: {builder_error}")
        print("🔄 Falling back to basic driver...")
        
        # Final fallback to basic driver
        dr = driver.Driver(config, *hamilton_modules, adapter=DictResult())
        print("✅ Created basic Hamilton driver (sequential processing)")
        return dr


def run_parallel_pipeline(
    dr: driver.Driver,
    config: Dict[str, Any]
) -> str:
    """
    Run the pipeline using parallel execution with proper Hamilton patterns.
    
    Args:
        dr: Hamilton driver instance
        config: Configuration dictionary
        
    Returns:
        str: Status message indicating pipeline completion
    """
    print("🚀 Starting parallel Hamilton DOI pipeline execution...")
    
    # Define target variables for parallel execution
    # These correspond to the final outputs we want from the DAG
    final_vars = [
        "pipeline_execution_summary"  # This collects all results and provides summary
    ]
    
    print(f"   Target variables: {final_vars}")
    print(f"   Configuration: {config}")
    
    try:
        # Execute the complete pipeline
        results = dr.execute(final_vars)
        
        # Extract and validate results
        execution_summary = results["pipeline_execution_summary"]
        
        if not execution_summary:
            raise ValueError("No execution summary generated")
        
        # Print detailed results
        print("📊 Pipeline Execution Summary:")
        print(f"   Total datasets processed: {execution_summary.get('total_datasets_processed', 0)}")
        print(f"   Successful storage count: {execution_summary.get('successful_storage_count', 0)}")
        print(f"   Storage success rate: {execution_summary.get('storage_success_rate', 0.0):.2%}")
        print(f"   Average quality score: {execution_summary.get('average_quality_score', 0.0):.2f}")
        print(f"   Pipeline status: {execution_summary.get('pipeline_status', 'unknown')}")
        
        # Print stored tables
        stored_tables = execution_summary.get('stored_tables', {})
        if stored_tables:
            print("📋 Stored datasets:")
            for dataset_name, table_info in stored_tables.items():
                print(f"   • {dataset_name}: {table_info}")
        
        # Print recommendations
        recommendations = execution_summary.get('recommendations', [])
        if recommendations:
            print("💡 Recommendations:")
            for rec in recommendations:
                print(f"   • {rec}")
        
        status_message = f"Successfully processed {execution_summary.get('total_datasets_processed', 0)} datasets"
        print(f"✅ {status_message}")
        
        return status_message
        
    except Exception as pipeline_error:
        print(f"❌ Hamilton pipeline execution failed: {pipeline_error}")
        print(f"   Error type: {type(pipeline_error).__name__}")
        raise


def run_sequential_pipeline(
    dr: driver.Driver,
    config: Dict[str, Any]
) -> str:
    """
    Run the pipeline using sequential execution for debugging and development.
    
    Args:
        dr: Hamilton driver instance
        config: Configuration dictionary
        
    Returns:
        str: Status message indicating pipeline completion
    """
    print("🚀 Starting sequential Hamilton DOI pipeline execution...")
    
    # For sequential execution, we can target intermediate results for debugging
    target_vars = [
        "dataset_metadata",
        "target_datasets_list", 
        "duckdb_storage_result"
    ]
    
    try:
        # Execute the pipeline
        results = dr.execute(target_vars)
        
        # Extract results
        metadata = results["dataset_metadata"]
        targets = results["target_datasets_list"]
        storage_result = results["duckdb_storage_result"]
        
        print(f"📊 Sequential Pipeline Results:")
        print(f"   Datasets available: {len(metadata)}")
        print(f"   Datasets processed: {len(targets)}")
        print(f"   Storage results: {len(storage_result)}")
        
        # Print storage results
        if storage_result:
            print("📋 Storage results:")
            for dataset_name, status in storage_result.items():
                print(f"   • {dataset_name}: {status}")
        
        successful_count = len([s for s in storage_result.values() if "success" in s.lower()])
        status_message = f"Successfully processed {successful_count}/{len(targets)} datasets"
        print(f"✅ {status_message}")
        
        return status_message
        
    except Exception as pipeline_error:
        print(f"❌ Sequential pipeline execution failed: {pipeline_error}")
        print(f"   Error type: {type(pipeline_error).__name__}")
        raise


def run_doi_pipeline(
    database_path: str = "../db/eo_pv_data.duckdb",
    use_parallel: bool = True,
    enable_caching: bool = True,
    force_download: bool = False,
    max_mb: int = 250,
    manifest_path: str = None,
    cache_path: str = "./.hamilton_cache"
) -> str:
    """
    Run the complete refactored DOI dataset pipeline using Hamilton best practices.

    This function demonstrates the new modular architecture with Hamilton's built-in caching:
    - Modular component organization
    - Proper dependency injection
    - Hamilton's built-in caching system
    - Simplified error handling
    - Clear separation of concerns

    Args:
        database_path: Path to DuckDB database
        use_parallel: Whether to use parallel processing
        enable_caching: Whether to enable Hamilton's built-in caching
        force_download: Force re-download (triggers cache recomputation)
        max_mb: Maximum file size in MB for downloads
        manifest_path: Optional explicit path to doi_manifest.json file
        cache_path: Path to Hamilton cache directory

    Returns:
        str: Status message indicating pipeline completion
    """
    # Create Hamilton driver configuration
    config = {
        "database_path": database_path,
        "force_download": force_download,
        "max_mb": max_mb,
        "cache_path": cache_path
    }

    # Add manifest path if provided
    if manifest_path is not None:
        config["manifest_path"] = manifest_path

    # Create Hamilton driver with caching
    dr = create_hamilton_driver(config, use_parallel, enable_caching)
    
    # Execute pipeline based on execution mode
    if use_parallel:
        result = run_parallel_pipeline(dr, config)
    else:
        result = run_sequential_pipeline(dr, config)
    
    # Print summary of raw_data schema
    try:
        print_raw_data_summary(database_path)
    except Exception as summary_error:
        print(f"⚠️  Could not print database summary: {summary_error}")
    
    return result


if __name__ == "__main__":
    """
    Command-line interface for the refactored Hamilton DOI pipeline.
    """
    parser = argparse.ArgumentParser(
        description="Run refactored Hamilton DOI dataset pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with default settings (parallel processing)
    python refactored_pv_doi_pipeline.py

    # Run with sequential processing for debugging
    python refactored_pv_doi_pipeline.py --sequential

    # Force fresh downloads without cache
    python refactored_pv_doi_pipeline.py --no-cache --force-download

    # Clear cache and exit
    python refactored_pv_doi_pipeline.py --clear-cache
        """
    )

    parser.add_argument(
        "--database",
        default="../db/eo_pv_data.duckdb",
        help="Path to DuckDB database (default: ../db/eo_pv_data.duckdb)"
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Use sequential processing instead of parallel (better for debugging)"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable Hamilton's built-in caching system"
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Force re-download (triggers Hamilton cache recomputation)"
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear download cache and exit"
    )
    parser.add_argument(
        "--max-mb",
        type=int,
        default=250,
        help="Maximum file size in MB for downloads (default: 250)"
    )
    parser.add_argument(
        "--manifest-path",
        help="Explicit path to doi_manifest.json file (auto-detected if not provided)"
    )
    parser.add_argument(
        "--cache-path",
        default="./.hamilton_cache",
        help="Path to Hamilton cache directory (default: ./.hamilton_cache)"
    )

    args = parser.parse_args()

    # Handle cache clearing
    if args.clear_cache:
        print("🧹 Clearing download cache...")
        clear_all_cache()
        print("✅ Cache cleared successfully")
        exit(0)

    # Show cache status
    if not args.no_cache:
        cache = load_download_cache()
        if cache:
            print(f"📦 Found {len(cache)} cached datasets:")
            for dataset_name, cache_dir in cache.items():
                try:
                    cache_size = sum(f.stat().st_size for f in Path(cache_dir).rglob("*") if f.is_file())
                    print(f"   {dataset_name}: {cache_dir} ({cache_size / 1024 / 1024:.1f} MB)")
                except:
                    print(f"   {dataset_name}: {cache_dir} (size unknown)")
        else:
            print("📦 No cached datasets found")

    # Run the pipeline
    try:
        result = run_doi_pipeline(
            database_path=args.database,
            use_parallel=not args.sequential,
            enable_caching=not args.no_cache,
            force_download=args.force_download,
            max_mb=args.max_mb,
            manifest_path=args.manifest_path,
            cache_path=args.cache_path
        )

        print(f"\n🎉 Pipeline completed successfully: {result}")

    except Exception as main_error:
        print(f"\n💥 Pipeline failed: {main_error}")
        print(f"Error type: {type(main_error).__name__}")
        exit(1)
