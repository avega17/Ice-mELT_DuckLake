"""
dbt Python staging model for consolidated PV installations.

This model demonstrates proper dbt patterns:
1. Uses {{ source() }} function to reference Hamilton-created raw tables
2. Leverages our Hamilton staging DAGs for complex processing
3. Returns Ibis dataframe for dbt materialization
4. Preserves ground-truth values from individual datasets

Hamilton DAG Integration:
- stg_doi_pv_dedup: Remove duplicate installations across datasets
- stg_doi_pv_geom_stats: Calculate geometry statistics
- stg_doi_pv_h3_res: Add H3 spatial indexing
- stg_doi_pv_std_schema: Standardize schema across datasets

This follows the proper dbt pattern where:
- Raw data comes from {{ source() }} references
- Complex processing happens in Hamilton DAGs
- dbt handles materialization and testing
"""

def model(dbt, session):
    """
    Consolidate PV installations from multiple DOI datasets using Hamilton staging pipeline.
    
    Args:
        dbt: dbt context object with access to {{ source() }} function
        session: dbt session object
        
    Returns:
        Ibis dataframe with consolidated and processed PV installations
    """
    import sys
    import os
    from pathlib import Path
    import warnings
    import pandas as pd
    import ibis
    
    # Suppress warnings for cleaner dbt output
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)
    
    # Add project root to Python path for Hamilton imports
    project_root = Path(dbt.config.project_root).resolve()
    sys.path.insert(0, str(project_root))
    
    try:
        print("🚀 Starting consolidated PV staging with proper dbt {{ source() }} usage...")

        # Step 1: Load available datasets dynamically from DOI manifest
        source_datasets = _load_available_datasets_from_manifest(dbt, project_root)

        print(f"   📋 Referencing {len(source_datasets)} source tables via {{ source() }}")
        
        # Step 2: Load and combine source data while preserving dataset-specific columns
        combined_datasets = []
        total_records = 0
        
        for dataset_name, source_ref in source_datasets:
            try:
                # Convert dbt source reference to pandas DataFrame
                df = source_ref.to_pandas()
                
                # Add source tracking
                df['source_dataset'] = dataset_name
                df['source_table'] = f'doi_{dataset_name}'
                
                combined_datasets.append(df)
                total_records += len(df)
                
                print(f"      ✅ {dataset_name}: {len(df):,} records")
                
                # Log dataset-specific ground-truth columns
                ground_truth_cols = []
                if 'installation_date' in df.columns:
                    ground_truth_cols.append('installation_date')
                if 'capacity_mw' in df.columns:
                    ground_truth_cols.append('capacity_mw')
                if 'building_type' in df.columns:
                    ground_truth_cols.append('building_type')
                
                if ground_truth_cols:
                    print(f"         Ground-truth columns: {', '.join(ground_truth_cols)}")
                    
            except Exception as e:
                print(f"      ⚠️  Could not load {dataset_name}: {e}")
                continue
        
        if not combined_datasets:
            print("   ❌ No source data loaded")
            return _create_empty_consolidated_result()
        
        # Combine all datasets
        combined_df = pd.concat(combined_datasets, ignore_index=True)
        print(f"   📊 Combined: {total_records:,} records from {len(combined_datasets)} datasets")
        
        # Step 3: Run Hamilton staging DAGs on the combined data
        print(f"   🔧 Executing Hamilton staging DAGs...")
        
        processed_gdf = _run_hamilton_staging_pipeline(combined_df, project_root)
        
        print(f"   ✅ Hamilton staging pipeline complete:")
        print(f"      - Input records: {len(combined_df):,}")
        print(f"      - Output records: {len(processed_gdf):,}")
        print(f"      - Columns: {len(processed_gdf.columns)}")
        
        # Step 4: Convert to Ibis dataframe for dbt materialization
        ibis_table = _convert_to_ibis_for_dbt(processed_gdf)
        
        print(f"   🎯 Converted to Ibis dataframe for dbt materialization")
        print(f"      - Final schema: {len(ibis_table.columns)} columns")
        
        return ibis_table
        
    except ImportError as e:
        print(f"❌ Failed to import Hamilton modules: {e}")
        print(f"   Make sure Hamilton dataflows are available in the project")
        raise
        
    except Exception as e:
        print(f"❌ Consolidated PV staging failed: {e}")
        raise


def _run_hamilton_staging_pipeline(combined_df, project_root):
    """
    Execute Hamilton staging DAGs on combined dataset.
    
    Args:
        combined_df: Combined pandas DataFrame from all source datasets
        project_root: Project root path
        
    Returns:
        Processed GeoDataFrame from Hamilton staging pipeline
    """
    from hamilton import driver
    import dataflows.stg_doi_pv_dedup as dedup
    import dataflows.stg_doi_pv_geom_stats as geom_stats
    import dataflows.stg_doi_pv_h3_res as h3_indexing
    import dataflows.stg_doi_pv_std_schema as std_schema
    
    # Configuration for Hamilton staging pipeline
    config = {
        "execution_mode": "sequential",  # Use sequential for dbt integration
        "use_cache": True,
        "target_sensor": "sentinel-2-l2a",
        "target_use_case": "detection_model",
        "h3_resolution": 9  # Appropriate resolution for PV installations
    }
    
    # Build Hamilton driver with our staging modules
    staging_modules = [dedup, geom_stats, h3_indexing, std_schema]
    dr = driver.Builder().with_modules(*staging_modules).with_config(config).build()
    
    print(f"      - Hamilton driver built with {len(staging_modules)} staging modules")
    
    # Convert DataFrame to GeoDataFrame for Hamilton processing
    input_gdf = _prepare_geodataframe_for_hamilton(combined_df)
    
    # Execute Hamilton staging pipeline
    final_vars = ["standardized_geodataframe"]
    
    results = dr.execute(final_vars, inputs={"raw_geodataframe": input_gdf})
    
    return results["standardized_geodataframe"]


def _prepare_geodataframe_for_hamilton(df):
    """Prepare pandas DataFrame as GeoDataFrame for Hamilton processing."""
    import geopandas as gpd
    from shapely.geometry import Point
    from shapely import wkb
    
    # Handle geometry column conversion
    if 'geometry' in df.columns:
        # Geometry is already present, ensure it's proper Shapely objects
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    elif 'centroid_lon' in df.columns and 'centroid_lat' in df.columns:
        # Create Point geometries from centroids
        geometry = [Point(lon, lat) for lon, lat in zip(df['centroid_lon'], df['centroid_lat'])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    else:
        raise ValueError("No suitable geometry information found in DataFrame")
    
    return gdf


def _convert_to_ibis_for_dbt(gdf):
    """Convert processed GeoDataFrame to Ibis dataframe for dbt materialization."""
    import ibis
    import pandas as pd
    
    # Convert GeoDataFrame to regular DataFrame for Ibis
    df = gdf.copy()
    
    # Convert geometry to WKB bytes for DuckDB storage
    if 'geometry' in df.columns:
        df['geometry_wkb'] = df['geometry'].apply(lambda x: x.wkb if x else None)
        df = df.drop(columns=['geometry'])
    
    # Add processing metadata
    df['processed_at'] = pd.Timestamp.now()
    df['source_system'] = 'hamilton_staging_consolidated'
    df['dbt_model'] = 'stg_pv_consolidated'
    
    # Standardize column types for DuckDB
    df = _standardize_types_for_duckdb(df)
    
    # Convert to Ibis table
    return ibis.memtable(df)


def _standardize_types_for_duckdb(df):
    """Standardize DataFrame column types for DuckDB compatibility."""
    import pandas as pd
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype('string')
        elif df[col].dtype in ['int64', 'int32']:
            df[col] = df[col].astype('int64')
        elif df[col].dtype in ['float64', 'float32']:
            df[col] = df[col].astype('float64')
    
    # Handle NaN values
    df = df.fillna({
        col: 0 if df[col].dtype in ['int64', 'float64'] else ''
        for col in df.columns if df[col].dtype in ['int64', 'float64', 'string']
    })
    
    return df


def _load_available_datasets_from_manifest(dbt, project_root):
    """
    Load available DOI datasets from manifest.json, excluding those with skip=true.

    Args:
        dbt: dbt context object
        project_root: Project root path

    Returns:
        List of tuples: (dataset_name, dbt_source_reference)
    """
    import json
    import os

    # Load DOI manifest
    manifest_path = os.path.join(project_root, "data_loaders", "doi_manifest.json")

    try:
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)

        print(f"   📄 Loaded DOI manifest from {manifest_path}")

        # Filter datasets that are not skipped and build source references
        source_datasets = []
        skipped_datasets = []

        for dataset_name, dataset_config in manifest.items():
            # Skip datasets with skip=true
            if dataset_config.get('skip', False):
                skipped_datasets.append(dataset_name)
                continue

            # Build dbt source reference
            table_name = f"doi_{dataset_name}"
            try:
                source_ref = dbt.source('doi_pv_raw', table_name)
                source_datasets.append((dataset_name, source_ref))
                print(f"      ✅ {dataset_name} → {table_name}")
            except Exception as e:
                print(f"      ⚠️  Could not reference {table_name}: {e}")
                continue

        if skipped_datasets:
            print(f"   ⏭️  Skipped datasets: {', '.join(skipped_datasets)}")

        print(f"   📊 Available datasets: {len(source_datasets)}")
        return source_datasets

    except FileNotFoundError:
        print(f"   ❌ DOI manifest not found at {manifest_path}")
        print(f"   🔄 Falling back to hardcoded dataset list")

        # Fallback to known ingested datasets
        fallback_datasets = [
            'uk_crowdsourced_pv_2020',
            'chn_med_res_pv_2024',
            'usa_cali_usgs_pv_2016',
            'ind_pv_solar_farms_2022',
            'global_pv_inventory_sent2_spot_2021',
            'global_harmonized_large_solar_farms_2020'
        ]

        source_datasets = []
        for dataset_name in fallback_datasets:
            table_name = f"doi_{dataset_name}"
            try:
                source_ref = dbt.source('doi_pv_raw', table_name)
                source_datasets.append((dataset_name, source_ref))
            except Exception as e:
                print(f"      ⚠️  Could not reference {table_name}: {e}")
                continue

        return source_datasets

    except Exception as e:
        print(f"   ❌ Error loading DOI manifest: {e}")
        return []


def _create_empty_consolidated_result():
    """Create empty Ibis table with expected consolidated schema."""
    import ibis
    import pandas as pd

    empty_df = pd.DataFrame(columns=[
        'source_dataset', 'source_table', 'dataset_name', 'geometry_wkb',
        'area_m2', 'centroid_lon', 'centroid_lat', 'h3_index', 'h3_resolution',
        'installation_date', 'capacity_mw', 'building_type', 'installation_type',
        'dedup_group_id', 'is_primary_duplicate', 'compactness_ratio',
        'processed_at', 'source_system', 'dbt_model'
    ])
    return ibis.memtable(empty_df)
