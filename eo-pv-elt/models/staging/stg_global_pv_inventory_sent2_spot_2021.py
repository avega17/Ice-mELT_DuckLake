"""
dbt Python staging model for Global PV Inventory Sent2 Spot 2021 dataset.

This model runs Hamilton DAG with spatial processing for a single dataset,
including geometry statistics calculation and H3 spatial indexing.

Flow:
1. Load raw table from DuckLake catalog via dbt.ref()
2. Calculate geometry stats (area_m2, centroid_lat/lon) 
3. Assign H3 spatial index for efficient deduplication
4. Apply final standardization
5. Return pandas DataFrame for dbt materialization

Note: Uses Hamilton DAG with GeoPandas for spatial processing, then converts to pandas for dbt.
"""

def model(dbt, session):
    """
    Staging model for Global PV Inventory Sent2 Spot 2021.
    Runs Hamilton DAG with spatial processing and H3 indexing.

    Args:
        dbt: dbt context object with ref(), source(), config(), etc.
        session: Database session for executing queries

    Returns:
        pandas.DataFrame: Processed dataset with spatial processing complete and H3 index assigned
    """
    import pandas as pd
    import sys
    import os
    from pathlib import Path
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    # Add repo root to path for Hamilton imports
    repo_root = Path(os.getenv('REPO_ROOT', '.')).resolve()
    sys.path.insert(0, str(repo_root))

    print("🚀 dbt Python staging: Processing Global PV Inventory Sent2 Spot 2021 with spatial processing...")

    # Configure model
    dbt.config(
        materialized='table',
        indexes=[
            {'columns': ['dataset_name'], 'type': 'btree'},
            {'columns': ['h3_index_12'], 'type': 'btree'},
            {'columns': ['area_m2'], 'type': 'btree'}
        ]
    )

    # Import Hamilton staging modules
    from dataflows.stg.consolidation import stg_doi_pv_consolidation
    from hamilton import driver
    from hamilton.execution import executors


    # Use dbt's built-in target information
    target_name = dbt.config.get('target_name', 'dev')
    is_prod_target = target_name == 'prod'

    os.environ['DBT_TARGET'] = target_name  # Ensure it's set for Hamilton

    print(f"   🎯 dbt target: {target_name}")
    print(f"   🎯 Target detected: {'PROD' if is_prod_target else 'DEV'}")

    if is_prod_target:
        # Production: Use complete connection string with cloud storage
        database_path = os.getenv('DUCKLAKE_CONNECTION_STRING_PROD')
        print(f"   🌐 Using production catalog: MotherDuck + Neon PostgreSQL + R2")
        print(f"   🔗 Database path: {database_path[:50]}...")
    else:
        # Development: Use complete connection string with local storage
        database_path = os.getenv('DUCKLAKE_CONNECTION_STRING_DEV')
        print(f"   💻 Using development catalog: Local SQLite + Local Data")
        print(f"   🔗 Database path: {database_path}")

    # Configuration with spatial processing enabled
    h3_dedup_res = int(os.getenv('H3_DEDUP_RES', '12'))  # Use env var or default to 12
    config = {
        'dataset_name': 'global_pv_inventory_sent2_spot_2021',
        'database_path': database_path,
        'h3_resolution': h3_dedup_res,  # Use configurable H3 resolution
        'calculate_geometry_stats': True,  # Enable geometry stats calculation
        'assign_h3_index': True,  # Enable H3 spatial indexing
        'memory_limit_gb': 8,
        'repo_root': str(repo_root),
        'log_level': 'INFO',
        'verbose': False,
        'execution_mode': 'sequential'  # Use sequential for dbt builds to avoid config resolution issues
    }

    print(f"   📊 Hamilton config for dataset: {config['dataset_name']}")
    print(f"   🗺️  Spatial processing enabled: geometry_stats={config['calculate_geometry_stats']}, h3_index={config['assign_h3_index']}")

    # Create Hamilton driver following working pattern from ingest_doi_pv_locations.py
    # Enable dynamic execution for Parallelizable/Collect patterns
    dr = (driver.Builder()
          .with_modules(stg_doi_pv_consolidation)
          .with_config(config)
          .enable_dynamic_execution(allow_experimental_mode=True)
          .with_local_executor(executors.SynchronousLocalTaskExecutor())
          .build())

    # Execute through spatial processing to standardized table
    # Flow: raw_table_from_catalog → geometry_stats_calculated → h3_spatial_index_assigned → standardized_dataset_table
    final_outputs = ['standardized_dataset_table']
    results = dr.execute(final_outputs,
                       inputs={'dataset_names': 'global_pv_inventory_sent2_spot_2021'})

    standardized_table = results['standardized_dataset_table']

    # Hamilton now returns GeoPandas DataFrame directly
    # Convert to regular pandas DataFrame for dbt (preserve geometry as WKT)
    if hasattr(standardized_table, 'drop') and 'geometry' in standardized_table.columns:
        # GeoPandas DataFrame - convert geometry to WKT and then to pandas
        df = standardized_table.copy()
        df['geometry'] = df['geometry'].apply(lambda geom: geom.wkt if geom else None)
        df = pd.DataFrame(df)  # Convert to regular pandas DataFrame
        print(f"   🔄 Converted GeoPandas to pandas DataFrame (preserved geometry as WKT)")
    else:
        # Already a pandas DataFrame
        df = standardized_table

    # Get record count for verification
    count = len(df)
    print(f"   ✅ Spatial processing complete: {count:,} records with H3 index and geometry stats")
    print(f"      - Columns: {len(df.columns)}")

    # Verify spatial columns are present
    columns = list(df.columns)
    h3_column_name = f"h3_index_{h3_dedup_res}"
    spatial_columns = ['area_m2', 'centroid_lat', 'centroid_lon', h3_column_name]
    present_spatial = [col for col in spatial_columns if col in columns]
    print(f"      - Spatial columns: {present_spatial}")

    # Ensure proper data types for H3 column (handle None values first)
    if h3_column_name in df.columns:
        # Handle None values in H3 column before type conversion
        h3_series = df[h3_column_name]
        non_null_count = h3_series.notna().sum()
        print(f"      - H3 column has {non_null_count}/{len(h3_series)} non-null values")

        if non_null_count > 0:
            # Convert to string first, then to uint64 (handles None values gracefully)
            df[h3_column_name] = pd.to_numeric(df[h3_column_name], errors='coerce').astype('Int64')
            print(f"      - Converted {h3_column_name} to Int64 (nullable) for optimal performance")
        else:
            print(f"      - Warning: All H3 values are null, keeping as-is")

    print(f"   📋 Final DataFrame shape: {df.shape}")
    print(f"   📋 Final columns: {list(df.columns)}")

    # Return the pandas DataFrame for dbt materialization
    return df
