"""
dbt Python staging model for DOI PV datasets.

This model properly uses dbt sources via {{ source() }} function and leverages
Hamilton DAGs for complex processing. It combines individual dataset tables
created by Hamilton DOI pipeline while preserving ground-truth values like
installation dates and precise area estimates.

Pipeline:
1. Reference Hamilton-created source tables via {{ source() }}
2. Run Hamilton staging DAGs for deduplication, geometry stats, H3 indexing
3. Return Ibis dataframe for dbt materialization

Follows proper dbt patterns:
- Uses {{ source() }} function to reference raw data
- Leverages Hamilton DAGs we spent time developing
- Returns Ibis dataframe for dbt to materialize
"""

def model(dbt, session):
    """
    dbt Python model that executes Hamilton DOI pipeline and returns Ibis dataframe.

    Args:
        dbt: dbt context object
        session: dbt session object

    Returns:
        Ibis dataframe with raw DOI PV installation data
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
        # Import Hamilton staging modules we developed
        from hamilton import driver
        import dataflows.stg_doi_pv_dedup as dedup
        import dataflows.stg_doi_pv_geom_stats as geom_stats
        import dataflows.stg_doi_pv_h3_res as h3_indexing
        import dataflows.stg_doi_pv_std_schema as std_schema

        print("🚀 Starting Hamilton staging pipeline for DOI PV datasets...")

        # First, get source data using proper dbt {{ source() }} function
        # This references the individual dataset tables created by Hamilton DOI pipeline
        source_tables = {
            'bdappv': dbt.source('doi_pv_raw', 'doi_bdappv'),
            'duke_solar': dbt.source('doi_pv_raw', 'doi_duke_solar'),
            'distributed_solar': dbt.source('doi_pv_raw', 'doi_distributed_solar'),
            'solar_power_map': dbt.source('doi_pv_raw', 'doi_solar_power_map'),
            'pv_rooftops': dbt.source('doi_pv_raw', 'doi_pv_rooftops'),
            'solar_installations': dbt.source('doi_pv_raw', 'doi_solar_installations')
        }

        print(f"   📋 Referenced {len(source_tables)} source tables via dbt {{ source() }} function")

        # Combine source tables while preserving dataset-specific columns
        combined_data = []
        for dataset_name, source_ref in source_tables.items():
            try:
                # Convert dbt source reference to pandas DataFrame
                df = source_ref.to_pandas()
                df['source_dataset'] = dataset_name
                combined_data.append(df)
                print(f"      - Loaded {len(df):,} records from {dataset_name}")
            except Exception as e:
                print(f"      ⚠️  Could not load {dataset_name}: {e}")
                continue

        if not combined_data:
            print("   ⚠️  No source data loaded, returning empty result")
            return _create_empty_result()

        # Combine all datasets
        combined_df = pd.concat(combined_data, ignore_index=True)
        print(f"   📊 Combined {len(combined_df):,} total records from {len(combined_data)} datasets")

        # Now run Hamilton staging DAGs on the combined data
        config = {
            "execution_mode": "sequential",  # Use sequential for dbt integration
            "use_cache": True,
            "target_sensor": "sentinel-2-l2a",
            "target_use_case": "detection_model"
        }

        # Build Hamilton driver with staging modules
        staging_modules = [dedup, geom_stats, h3_indexing, std_schema]
        dr = driver.Builder().with_modules(*staging_modules).with_config(config).build()

        print(f"   🔧 Hamilton staging driver built with {len(staging_modules)} modules")

        # Execute Hamilton staging pipeline
        final_vars = ["standardized_geodataframe"]

        print(f"   ⚙️  Executing Hamilton staging pipeline...")
        # Pass the combined data as input to Hamilton
        results = dr.execute(final_vars, inputs={"raw_geodataframe": _convert_to_gdf(combined_df)})

        # Get the processed result
        processed_gdf = results["standardized_geodataframe"]

        print(f"   ✅ Hamilton staging pipeline complete")
        print(f"      - Records processed: {len(processed_gdf):,}")
        print(f"      - Columns: {len(processed_gdf.columns)}")

        # Convert to Ibis dataframe for dbt materialization
        ibis_table = _convert_gdf_to_ibis(processed_gdf, session)

        print(f"   🎯 Converted to Ibis dataframe for dbt materialization")

        return ibis_table

    except ImportError as e:
        print(f"❌ Failed to import Hamilton DOI module: {e}")
        print(f"   Make sure Hamilton dataflows are available in the project")
        raise

    except Exception as e:
        print(f"❌ Hamilton DOI pipeline execution failed: {e}")
        print(f"   Check Hamilton module configurations and dependencies")
        raise


def _create_empty_result():
    """Create empty Ibis table with expected schema."""
    import ibis
    import pandas as pd

    empty_df = pd.DataFrame(columns=[
        'dataset_name', 'source_dataset', 'geometry_wkb', 'area_m2',
        'centroid_lon', 'centroid_lat', 'h3_index', 'h3_resolution',
        'installation_date', 'capacity_mw', 'building_type', 'installation_type',
        'processed_at', 'source_system'
    ])
    return ibis.memtable(empty_df)


def _convert_to_gdf(df):
    """Convert pandas DataFrame to GeoDataFrame for Hamilton processing."""
    import geopandas as gpd
    from shapely import wkb

    # Convert geometry from WKB if needed
    if 'geometry' in df.columns:
        # Assume geometry is already in proper format
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    else:
        # Create Point geometries from centroids
        from shapely.geometry import Point
        geometry = [Point(lon, lat) for lon, lat in zip(df['centroid_lon'], df['centroid_lat'])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

    return gdf


def _convert_gdf_to_ibis(gdf, session):
    """Convert GeoDataFrame to Ibis dataframe for dbt materialization."""
    import ibis
    import pandas as pd

    # Convert GeoDataFrame to regular DataFrame for Ibis
    df = gdf.copy()

    # Convert geometry to WKB bytes for DuckDB storage
    if 'geometry' in df.columns:
        df['geometry_wkb'] = df['geometry'].apply(lambda x: x.wkb if x else None)
        df = df.drop(columns=['geometry'])

    # Add metadata columns for lineage tracking
    df['processed_at'] = pd.Timestamp.now()
    df['source_system'] = 'hamilton_staging_pipeline'
    df['processing_version'] = '1.0'

    # Ensure all columns have appropriate types for DuckDB
    df = _standardize_column_types(df)

    # Convert to Ibis table
    ibis_table = ibis.memtable(df)

    return ibis_table


def _standardize_column_types(df):
    """Standardize DataFrame column types for DuckDB compatibility."""
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
