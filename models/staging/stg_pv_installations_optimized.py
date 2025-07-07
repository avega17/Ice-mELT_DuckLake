"""
dbt Python model for spatially optimized PV installations.

This model runs the complete Hamilton DAG pipeline and returns an Ibis dataframe
for dbt to materialize. Follows proper dbt patterns by letting dbt handle
database persistence rather than manual exports.

Pipeline sequence:
1. DOI PV locations ingestion
2. Deduplication and geometry stats  
3. H3 spatial indexing
4. Schema standardization
5. Overture Maps integration (admin, buildings, land cover, land use)
6. Spatial optimization (Hilbert curves, spatial indexes)
7. Return final Ibis dataframe for dbt materialization
"""

def model(dbt, session):
    """
    dbt Python model that executes Hamilton DAG and returns Ibis dataframe.
    
    Args:
        dbt: dbt context object
        session: dbt session object
        
    Returns:
        Ibis dataframe with spatially optimized PV installations
    """
    import sys
    import os
    from pathlib import Path
    import warnings
    
    # Suppress warnings for cleaner dbt output
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)
    
    # Add project root to Python path for Hamilton imports
    project_root = Path(dbt.config.project_root).resolve()
    sys.path.insert(0, str(project_root))
    
    try:
        # Import Hamilton modules
        from hamilton import driver
        import dataflows.doi_pv_locations as doi_locations
        import dataflows.stg_doi_pv_dedup as dedup
        import dataflows.stg_doi_pv_geom_stats as geom_stats
        import dataflows.stg_doi_pv_h3_res as h3_indexing
        import dataflows.stg_doi_pv_std_schema as std_schema
        import dataflows.stg_doi_pv_overture_admin as overture_admin
        import dataflows.stg_doi_pv_overture_buildings as overture_buildings
        import dataflows.stg_doi_pv_overture_land_cover as overture_land_cover
        import dataflows.stg_doi_pv_overture_land_use as overture_land_use
        import dataflows.stg_doi_pv_overture_unified as overture_unified
        import dataflows.stg_doi_pv_spatial_optimization as spatial_opt
        
        print("🚀 Starting Hamilton DAG execution for PV installations optimization...")
        
        # Configuration for Hamilton DAGs
        config = {
            "execution_mode": "sequential",  # Use sequential for dbt integration
            "use_cache": True,
            "database_path": str(project_root / "db" / "eo_pv_data.duckdb"),
            "max_mb": 300,
            "parallel_downloads": 4,
            "target_sensor": "sentinel-2-l2a",
            "target_use_case": "detection_model"
        }
        
        print(f"   📋 Configuration: execution_mode={config['execution_mode']}, use_cache={config['use_cache']}")
        
        # Build Hamilton driver with all required modules
        hamilton_modules = [
            doi_locations,
            dedup, 
            geom_stats,
            h3_indexing,
            std_schema,
            overture_admin,
            overture_buildings, 
            overture_land_cover,
            overture_land_use,
            overture_unified,
            spatial_opt
        ]
        
        dr = driver.Builder().with_modules(*hamilton_modules).with_config(config).build()
        
        print(f"   🔧 Hamilton driver built with {len(hamilton_modules)} modules")
        
        # Execute the complete pipeline to get spatially optimized data
        # The final output should be an Ibis table from spatial optimization
        final_vars = ["spatial_optimization_summary", "spatial_partitioning"]
        
        print(f"   ⚙️  Executing Hamilton DAG for variables: {final_vars}")
        results = dr.execute(final_vars)
        
        # Get the final spatially optimized GeoDataFrame
        optimized_gdf = results["spatial_partitioning"]
        
        print(f"   ✅ Hamilton DAG execution complete")
        print(f"      - Records processed: {len(optimized_gdf):,}")
        print(f"      - Columns: {len(optimized_gdf.columns)}")
        print(f"      - Spatial optimizations: {len(results['spatial_optimization_summary']['optimizations_applied'])}")
        
        # Convert GeoDataFrame to Ibis dataframe for dbt
        # This is the proper way - let dbt handle the database persistence
        ibis_table = _convert_gdf_to_ibis(optimized_gdf, session)
        
        print(f"   🎯 Converted to Ibis dataframe for dbt materialization")
        print(f"      - Ibis table schema: {len(ibis_table.columns)} columns")
        
        return ibis_table
        
    except ImportError as e:
        print(f"❌ Failed to import Hamilton modules: {e}")
        print(f"   Make sure Hamilton dataflows are available in the project")
        raise
        
    except Exception as e:
        print(f"❌ Hamilton DAG execution failed: {e}")
        print(f"   Check Hamilton module configurations and dependencies")
        raise


def _convert_gdf_to_ibis(gdf, session):
    """
    Convert GeoDataFrame to Ibis dataframe for dbt materialization.
    
    Args:
        gdf: GeoDataFrame from Hamilton pipeline
        session: dbt session object
        
    Returns:
        Ibis dataframe ready for dbt materialization
    """
    import ibis
    import pandas as pd
    from shapely import wkb
    
    # Convert GeoDataFrame to regular DataFrame for Ibis
    # Handle geometry column conversion to WKB for DuckDB compatibility
    df = gdf.copy()
    
    # Convert geometry to WKB bytes for DuckDB storage
    if 'geometry' in df.columns:
        df['geometry_wkb'] = df['geometry'].apply(lambda x: x.wkb if x else None)
        df = df.drop(columns=['geometry'])
    
    # Add metadata columns for lineage tracking
    df['processed_at'] = pd.Timestamp.now()
    df['source_system'] = 'hamilton_spatial_optimization'
    df['optimization_version'] = '1.0'
    
    # Ensure all columns have appropriate types for DuckDB
    df = _standardize_column_types(df)
    
    # Convert to Ibis table using DuckDB backend
    # This leverages dbt's connection to create the Ibis table
    ibis_table = ibis.memtable(df)
    
    return ibis_table


def _standardize_column_types(df):
    """
    Standardize DataFrame column types for DuckDB compatibility.
    
    Args:
        df: pandas DataFrame
        
    Returns:
        DataFrame with standardized column types
    """
    import pandas as pd
    import numpy as np
    
    # Handle common type conversions for DuckDB
    for col in df.columns:
        if df[col].dtype == 'object':
            # Try to convert object columns to appropriate types
            if col.endswith('_iso') or col.endswith('_code'):
                # Country/region codes should be strings
                df[col] = df[col].astype('string')
            elif col.endswith('_id') or col.endswith('_index'):
                # IDs and indexes should be strings or integers
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    df[col] = df[col].astype('string')
            else:
                # Default to string for other object columns
                df[col] = df[col].astype('string')
        
        elif df[col].dtype in ['int64', 'int32']:
            # Ensure integers are int64 for consistency
            df[col] = df[col].astype('int64')
        
        elif df[col].dtype in ['float64', 'float32']:
            # Ensure floats are float64 for consistency
            df[col] = df[col].astype('float64')
    
    # Handle any remaining NaN values
    df = df.fillna({
        col: 0 if df[col].dtype in ['int64', 'float64'] else ''
        for col in df.columns if df[col].dtype in ['int64', 'float64', 'string', 'object']
    })
    
    return df
