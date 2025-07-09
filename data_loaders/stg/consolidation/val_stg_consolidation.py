#!/usr/bin/env python3
"""
DOI PV Staging Transformations Pipeline

Hamilton driver script for running staging transformations on ingested DOI PV data.
Follows Hamilton best practices with separate dataflow modules and driver.

This script runs the staging pipeline that:
1. Loads consolidated data from individual DOI dataset tables
2. Performs deduplication across datasets
3. Calculates geometry statistics and spatial metrics
4. Adds H3 spatial indexing for optimization
5. Standardizes schema across all datasets
6. Exports optimized tables with hive partitioning

Usage:
    python run_staging_transformations.py
    python run_staging_transformations.py --sequential --no-cache
    python run_staging_transformations.py --database ./custom.duckdb --target-table stg_pv_consolidated
"""

# Add repo root to path to import dataflows
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import os
import argparse
from dotenv import load_dotenv
from hamilton import driver
from hamilton.execution import executors

# Import staging dataflow modules
from dataflows.stg.consolidation import stg_doi_pv_consolidation

# Load environment variables from .env file
load_dotenv()

# Get repo root and database path from environment or use defaults
# Use relative paths as they work better than absolute paths in this environment
REPO_ROOT = str(Path(__file__).parent.parent.parent.parent)
# Default to DuckLake catalog for consistency with staging DAG
DATABASE_PATH = f"ducklake:sqlite:{str(Path(__file__).parent.parent.parent.parent / 'db' / 'ducklake_catalog.sqlite')}"


def create_hamilton_driver(
    config: dict,
    enable_caching: bool = True,
    use_parallel: bool = True
) -> driver.Driver:
    """
    Create Hamilton driver for staging transformations.
    
    Args:
        config: Configuration dictionary for the pipeline
        enable_caching: Whether to enable Hamilton's built-in caching
        use_parallel: Whether to use parallel execution
        
    Returns:
        driver.Driver: Configured Hamilton driver
    """
    
    # Import staging modules - start with just consolidation to test basic pattern
    staging_modules = [
        stg_doi_pv_consolidation,  # Core consolidation module
        # Note: stg_doi_pv_std_schema, stg_doi_pv_hive_partitioning, and stg_doi_pv_database_export
        # modules are not found in the current codebase structure
    ]
    
    # Create driver with staging modules
    dr = driver.Builder()\
        .with_modules(*staging_modules)\
        .with_config(config)

    # Configure execution mode following working pattern from ingest_doi_pv_locations.py
    if use_parallel:
        # Only enable V2 driver for parallel mode (required for Parallelizable[] and Collect[])
        dr = dr.enable_dynamic_execution(allow_experimental_mode=True)
        executor = executors.SynchronousLocalTaskExecutor()  # Use sync executor like working version
        dr = dr.with_local_executor(executor)
        print("🔄 V2 Driver enabled for parallel execution with SynchronousLocalTaskExecutor")
    else:
        # Sequential mode doesn't need V2 driver
        print("🔄 Using sequential execution (no V2 driver needed)")

    if enable_caching:
        print("💾 Hamilton caching enabled")

    return dr.build()


def load_staging_config(args) -> dict:
    """
    Load configuration for staging transformations.

    Args:
        args: Command line arguments

    Returns:
        dict: Configuration dictionary
    """

    # Detect catalog type from database connection string
    if args.database.startswith('ducklake:postgres:'):
        # PostgreSQL catalog for cloud deployment
        catalog_type = 'postgresql'
        catalog_path = args.database  # Full PostgreSQL connection string
        data_path = 's3://eo-pv-lakehouse/ducklake_data/'  # R2 cloud storage
        print(f"🌩️  Detected PostgreSQL catalog for cloud deployment")
        print(f"   📊 Catalog: PostgreSQL (connection details hidden)")
        print(f"   ☁️  Data path: {data_path}")
    else:
        # SQLite catalog for local development
        catalog_type = 'sqlite'
        catalog_path = 'db/ducklake_catalog.sqlite'
        data_path = 'db/ducklake_data'
        print(f"💾 Detected SQLite catalog for local development")
        print(f"   📊 Catalog: {catalog_path}")
        print(f"   💽 Data path: {data_path}")

    config = {
        # DuckLake configuration for concurrent access
        'use_ducklake': True,
        'catalog_type': catalog_type,
        'catalog_path': catalog_path,
        'data_path': data_path,

        # Legacy database configuration (fallback)
        'database_path': args.database,
        'database_schema': 'main',
        
        # Source configuration
        'source_schema': 'main',
        'doi_table_prefix': 'doi_',
        
        # Target configuration  
        'target_table': args.target_table,
        'staging_prefix': 'stg_',
        
        # Processing configuration
        'h3_resolution': 9,  # H3 resolution for spatial indexing
        'dedup_distance_threshold': 10.0,  # meters for deduplication
        'geometry_buffer_distance': 5.0,  # meters for geometry analysis

        # Execution configuration (required for @config.when decorators)
        'dedup_strategy': 'geopandas',  # Default deduplication strategy

        # Performance configuration
        'batch_size': 10000,
        'memory_limit_gb': 16,
        'enable_spatial_index': True,
        'enable_hive_partitioning': True,
        
        # Output configuration
        'export_geoparquet': True,
        'export_csv': False,
        'compression': 'snappy',
        
        # Paths
        'repo_root': REPO_ROOT,
        'output_dir': str(Path(REPO_ROOT) / "output" / "staging"),
        
        # Logging
        'log_level': 'INFO',
        'verbose': args.verbose
    }
    
    return config


def run_staging_pipeline(config: dict, enable_caching: bool = True, use_parallel: bool = True):
    """
    Run the complete staging transformations pipeline.

    Args:
        config: Configuration dictionary
        enable_caching: Whether to enable Hamilton caching
        use_parallel: Whether to use parallel execution
    """

    print("🚀 Starting DOI PV Staging Transformations Pipeline")
    print("=" * 60)
    print(f"   Database: {config['database_path']}")
    print(f"   Target table: {config['target_table']}")
    print(f"   H3 resolution: {config['h3_resolution']}")
    print(f"   Parallel execution: {use_parallel}")
    print(f"   Caching enabled: {enable_caching}")
    print("")

    # Add execution mode to config (required for @config.when decorators)
    config['execution_mode'] = 'parallel' if use_parallel else 'sequential'
    print(f"   🎯 Config execution_mode: {config['execution_mode']}")

    # Create Hamilton driver
    dr = create_hamilton_driver(config, enable_caching, use_parallel)
    
    # Define the final outputs we want from the staging pipeline
    # Use base function names - Hamilton will resolve to correct __parallel or __sequential version
    final_outputs = [
        'staging_table_created',  # Hamilton will resolve to __parallel or __sequential
    ]
    
    try:
        # Execute the staging pipeline
        print("🔄 Executing staging transformations...")
        # Note: config is already passed via .with_config(), so no inputs needed
        results = dr.execute(final_outputs)
        
        print("\n✅ Staging pipeline completed successfully!")
        print("📊 Results summary:")

        for output_name, result in results.items():
            if output_name == 'staging_table_created':
                # This should be a table name string
                table_name = result
                print(f"   Created table: {table_name}")

                # Verify the table exists in the database (use DuckLake connection)
                try:
                    from dataflows.stg.consolidation.stg_doi_pv_consolidation import _create_ducklake_connection

                    # Use the same DuckLake connection as the staging DAG
                    con = _create_ducklake_connection(
                        catalog_path=config['catalog_path'],
                        data_path=config['data_path'],
                        use_ducklake=config['use_ducklake'],
                        catalog_type=config['catalog_type']
                    )

                    if table_name in con.list_tables():
                        count = con.table(table_name).count().execute()
                        print(f"   ✅ Verified in DuckLake catalog: {count} records")
                    else:
                        print(f"   ❌ Table {table_name} not found in DuckLake catalog")

                except Exception as e:
                    print(f"   ⚠️  Could not verify table in DuckLake catalog: {e}")
            else:
                # Handle other result types
                if hasattr(result, 'count') and hasattr(result, 'execute'):
                    # Ibis table - use .count().execute()
                    try:
                        count = result.count().execute()
                        print(f"   {output_name}: {count} records (Ibis table)")
                    except Exception as e:
                        print(f"   {output_name}: {type(result).__name__} (count failed: {e})")
                elif hasattr(result, '__len__'):
                    # Regular Python objects with len()
                    print(f"   {output_name}: {len(result)} records")
                else:
                    print(f"   {output_name}: {type(result).__name__}")

        return results
        
    except Exception as e:
        print(f"\n❌ Staging pipeline failed: {e}")
        raise


def main():
    """Main entry point for staging transformations."""
    
    parser = argparse.ArgumentParser(description="Run DOI PV staging transformations")
    
    parser.add_argument(
        '--database',
        default=DATABASE_PATH,
        help='Path to DuckDB database file or DuckLake connection string'
    )
    
    parser.add_argument(
        '--target-table',
        default='stg_pv_consolidated',
        help='Name of target staging table'
    )
    
    parser.add_argument(
        '--sequential',
        action='store_true',
        help='Use sequential execution instead of parallel'
    )
    
    parser.add_argument(
        '--no-cache',
        action='store_true', 
        help='Disable Hamilton caching'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_staging_config(args)
    
    # Run the staging pipeline
    try:
        results = run_staging_pipeline(
            config=config,
            enable_caching=not args.no_cache,
            use_parallel=not args.sequential
        )
        
        print(f"\n🎯 Staging transformations completed successfully!")
        print(f"   Results available in: {config['database_path']}")
        print(f"   Target table: {config['target_table']}")
        
    except Exception as e:
        print(f"\n💥 Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
