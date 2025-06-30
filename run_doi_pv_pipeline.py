#!/usr/bin/env python3
"""
DOI PV Locations Pipeline Runner

Executes the Hamilton dataflow for processing scientific publication (DOI) 
PV location datasets. Supports both parallel and sequential execution modes
with comprehensive caching and output options.

Usage:
    python run_doi_pv_pipeline.py --parallel
    python run_doi_pv_pipeline.py --sequential --no-cache
    python run_doi_pv_pipeline.py --database custom_db.duckdb --max-mb 500
"""

import argparse
import sys
from pathlib import Path

# Add dataflows to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from hamilton import driver
from hamilton.execution import executors

# Import the dataflow module
from dataflows import doi_pv_locations


def run_doi_pv_pipeline(
    database_path: str = "db/eo_pv_data.duckdb",
    manifest_path: str = "data_loaders/doi_manifest.json", 
    max_mb: int = 250,
    export_geoparquet: bool = True,
    enable_caching: bool = True,
    force_download: bool = False,
    cache_path: str = "data_loaders/cache",
    use_parallel: bool = True
) -> dict:
    """
    Run the DOI PV locations processing pipeline.
    
    Args:
        database_path: Path to DuckDB database file
        manifest_path: Path to DOI manifest JSON file
        max_mb: Maximum file size for downloads (MB)
        export_geoparquet: Whether to export GeoParquet files
        enable_caching: Whether to enable Hamilton caching
        force_download: Whether to force re-download of cached data
        cache_path: Path for Hamilton cache storage
        use_parallel: Whether to use parallel execution mode
        
    Returns:
        Dict with pipeline results and metadata
    """
    
    # Ensure database directory exists
    db_path = Path(database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Ensure cache directory exists if caching enabled
    if enable_caching:
        Path(cache_path).mkdir(parents=True, exist_ok=True)
    
    # Configure Hamilton driver
    config = {
        "execution_mode": "parallel" if use_parallel else "sequential"
    }
    
    # Set up caching if enabled
    if enable_caching:
        from hamilton.caching import file_cache
        cache_adapter = file_cache.FileBasedCache(cache_path=cache_path)
        
        dr = driver.Builder()\
            .with_config(config)\
            .with_modules(doi_pv_locations)\
            .with_cache(cache_adapter)\
            .build()
    else:
        dr = driver.Builder()\
            .with_config(config)\
            .with_modules(doi_pv_locations)\
            .build()
    
    # Set up parallel execution if requested
    if use_parallel:
        # Use thread-based executor for I/O bound operations
        executor = executors.MultiThreadingExecutor(max_tasks=4)
        dr = dr.with_remote_executor(executor)
    
    # Define pipeline inputs
    inputs = {
        "manifest_path": manifest_path,
        "max_mb": max_mb,
        "database_path": database_path,
        "export_geoparquet": export_geoparquet
    }
    
    # Execute pipeline
    print(f"🚀 Starting DOI PV pipeline in {'parallel' if use_parallel else 'sequential'} mode")
    print(f"   📁 Database: {database_path}")
    print(f"   📋 Manifest: {manifest_path}")
    print(f"   💾 Caching: {'enabled' if enable_caching else 'disabled'}")
    print(f"   📦 GeoParquet export: {'enabled' if export_geoparquet else 'disabled'}")
    
    # Choose the appropriate result function based on execution mode
    result_function = "pipeline_result__parallel" if use_parallel else "pipeline_result__sequential"
    
    result = dr.execute([result_function], inputs=inputs)
    
    return result[result_function]


def main():
    """Command line interface for the DOI PV pipeline."""
    parser = argparse.ArgumentParser(
        description="Run DOI PV locations processing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_doi_pv_pipeline.py --parallel
  python run_doi_pv_pipeline.py --sequential --no-cache
  python run_doi_pv_pipeline.py --database custom.duckdb --max-mb 500
        """
    )
    
    # Execution mode
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--parallel", action="store_true", default=True,
        help="Use parallel execution mode (default)"
    )
    mode_group.add_argument(
        "--sequential", action="store_true",
        help="Use sequential execution mode"
    )
    
    # File paths
    parser.add_argument(
        "--database", default="db/eo_pv_data.duckdb",
        help="Path to DuckDB database file (default: db/eo_pv_data.duckdb)"
    )
    parser.add_argument(
        "--manifest", default="data_loaders/doi_manifest.json",
        help="Path to DOI manifest file (default: data_loaders/doi_manifest.json)"
    )
    parser.add_argument(
        "--cache-path", default="data_loaders/cache",
        help="Path for Hamilton cache storage (default: data_loaders/cache)"
    )
    
    # Processing options
    parser.add_argument(
        "--max-mb", type=int, default=250,
        help="Maximum file size for downloads in MB (default: 250)"
    )
    
    # Feature flags
    parser.add_argument(
        "--no-geoparquet", action="store_true",
        help="Disable GeoParquet export"
    )
    parser.add_argument(
        "--no-cache", action="store_true",
        help="Disable Hamilton caching"
    )
    parser.add_argument(
        "--force-download", action="store_true",
        help="Force re-download of cached data"
    )
    
    args = parser.parse_args()
    
    # Determine execution mode
    use_parallel = not args.sequential
    
    try:
        result = run_doi_pv_pipeline(
            database_path=args.database,
            manifest_path=args.manifest,
            max_mb=args.max_mb,
            export_geoparquet=not args.no_geoparquet,
            enable_caching=not args.no_cache,
            force_download=args.force_download,
            cache_path=args.cache_path,
            use_parallel=use_parallel
        )
        
        print("✅ Pipeline completed successfully!")
        print(f"📊 Results:")
        print(f"   📁 Database: {result['database_path']}")
        print(f"   📋 Tables created: {len(result['tables_created'])}")
        print(f"   📄 GeoParquet files: {len(result['geoparquet_files'])}")
        print(f"   📊 Total records: {result['total_records']}")
        
        if result['tables_created']:
            table_names = [table_info['table_name'] for table_info in result['tables_created']]
            print(f"   🗂️  Tables: {', '.join(table_names)}")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
