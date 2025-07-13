"""
dbt Python consolidated staging model for all PV datasets.

This model unions all individual staging models that have completed spatial processing
with H3 indexing. It performs basic union operations and exact duplicate removal.

Flow:
1. Reference all individual staging models via dbt.ref() with literal strings
2. Union all DataFrames using pandas.concat()
3. Remove exact duplicates
4. Return consolidated pandas DataFrame for dbt materialization

Note: Converted from dbt-ibis to dbt Python model to avoid database context issues.
Individual staging models handle Hamilton DAG spatial processing.
This model focuses on simple union operations with H3 column preservation.
"""

def model(dbt, session):
    """
    Consolidated staging model that unions all individual PV staging models.
    
    Args:
        dbt: dbt context object with ref(), source(), config(), etc.
        session: Database session for executing queries
        
    Returns:
        pandas.DataFrame: Consolidated dataset with all PV locations and H3 spatial indexing
    """
    import pandas as pd
    import sys
    import os
    from pathlib import Path
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    print("🚀 dbt Python consolidated: Creating consolidated table with basic union and exact duplicate removal...")

    # Configure model
    dbt.config(
        materialized='table',
        indexes=[
            {'columns': ['dataset_name'], 'type': 'btree'},
            {'columns': ['h3_index_12'], 'type': 'btree'},
            {'columns': ['area_m2'], 'type': 'btree'}
        ]
    )

    print("   🔗 Unioning individual staging models...")

    def convert_to_pandas(relation, model_name):
        """Convert dbt relation to pandas DataFrame with proper error handling."""
        try:
            # Try different conversion methods based on the relation type
            if hasattr(relation, 'df'):
                # DuckDB relation
                df = relation.df()
                print(f"      - {model_name}: {len(df):,} records (DuckDB relation)")
                return df
            elif hasattr(relation, 'to_pandas'):
                # Ibis table
                df = relation.to_pandas()
                print(f"      - {model_name}: {len(df):,} records (Ibis table)")
                return df
            elif hasattr(relation, 'toPandas'):
                # Spark DataFrame
                df = relation.toPandas()
                print(f"      - {model_name}: {len(df):,} records (Spark DataFrame)")
                return df
            elif hasattr(relation, '__len__'):
                # Already a pandas DataFrame
                print(f"      - {model_name}: {len(relation):,} records (pandas DataFrame)")
                return relation
            else:
                print(f"      ⚠️  {model_name}: Unknown relation type {type(relation)}")
                return None
        except Exception as e:
            print(f"      ⚠️  {model_name}: Conversion failed ({e})")
            return None

    # Reference all individual staging models using literal strings (required by dbt Python models)
    # These models have spatial processing complete with H3 indexing
    staging_dataframes = []

    # Get each staging model using literal strings in dbt.ref()
    try:
        rel1 = dbt.ref('stg_global_pv_inventory_sent2_spot_2021')
        df1 = convert_to_pandas(rel1, 'stg_global_pv_inventory_sent2_spot_2021')
        if df1 is not None:
            staging_dataframes.append(df1)
    except Exception as e:
        print(f"      ⚠️  stg_global_pv_inventory_sent2_spot_2021: Not available ({e})")

    try:
        rel2 = dbt.ref('stg_usa_cali_usgs_pv_2016')
        df2 = convert_to_pandas(rel2, 'stg_usa_cali_usgs_pv_2016')
        if df2 is not None:
            staging_dataframes.append(df2)
    except Exception as e:
        print(f"      ⚠️  stg_usa_cali_usgs_pv_2016: Not available ({e})")

    try:
        rel3 = dbt.ref('stg_uk_crowdsourced_pv_2020')
        df3 = convert_to_pandas(rel3, 'stg_uk_crowdsourced_pv_2020')
        if df3 is not None:
            staging_dataframes.append(df3)
    except Exception as e:
        print(f"      ⚠️  stg_uk_crowdsourced_pv_2020: Not available ({e})")

    try:
        rel4 = dbt.ref('stg_chn_med_res_pv_2024')
        df4 = convert_to_pandas(rel4, 'stg_chn_med_res_pv_2024')
        if df4 is not None:
            staging_dataframes.append(df4)
    except Exception as e:
        print(f"      ⚠️  stg_chn_med_res_pv_2024: Not available ({e})")

    try:
        rel5 = dbt.ref('stg_ind_pv_solar_farms_2022')
        df5 = convert_to_pandas(rel5, 'stg_ind_pv_solar_farms_2022')
        if df5 is not None:
            staging_dataframes.append(df5)
    except Exception as e:
        print(f"      ⚠️  stg_ind_pv_solar_farms_2022: Not available ({e})")

    try:
        rel6 = dbt.ref('stg_global_harmonized_large_solar_farms_2020')
        df6 = convert_to_pandas(rel6, 'stg_global_harmonized_large_solar_farms_2020')
        if df6 is not None:
            staging_dataframes.append(df6)
    except Exception as e:
        print(f"      ⚠️  stg_global_harmonized_large_solar_farms_2020: Not available ({e})")

    # Ensure we have at least one DataFrame (create empty one if needed)
    if not staging_dataframes:
        print("   ⚠️  No staging DataFrames available, creating empty DataFrame")
        staging_dataframes = [pd.DataFrame({
            'dataset_name': [],
            'geometry': [],
            'area_m2': [],
            'centroid_lat': [],
            'centroid_lon': [],
            'processed_at': [],
            'h3_index_12': [],
            'unified_id': [],
            'source_area_m2': [],
            'capacity_mw': [],
            'install_date': []
        })]

    print(f"   📊 Total staging DataFrames to union: {len(staging_dataframes)}")

    # Union all staging DataFrames using pandas concat
    # This preserves H3 columns that were processed in individual staging models
    print("   🔗 Performing union with pandas.concat()...")
    all_staging = pd.concat(staging_dataframes, ignore_index=True)
    
    total_before = len(all_staging)
    print(f"   📊 Total records after union: {total_before:,}")
    print(f"   📊 Total columns: {len(all_staging.columns)}")
    print(f"   📊 Columns: {list(all_staging.columns)}")

    # Verify H3 columns are preserved
    h3_columns = [col for col in all_staging.columns if col.startswith('h3_index_')]
    if h3_columns:
        print(f"   ✅ H3 columns preserved: {h3_columns}")
        # Ensure H3 columns are uint64 for optimal performance
        for h3_col in h3_columns:
            if all_staging[h3_col].dtype != 'uint64':
                all_staging[h3_col] = all_staging[h3_col].astype('uint64')
                print(f"      - Converted {h3_col} to uint64")
    else:
        print("   ⚠️  No H3 columns found in union result")

    # Basic deduplication - remove exact duplicates
    print("   🧹 Removing exact duplicates...")
    deduplicated = all_staging.drop_duplicates()
    
    total_after = len(deduplicated)
    duplicates_removed = total_before - total_after
    print(f"   📊 Records after deduplication: {total_after:,}")
    print(f"   📊 Exact duplicates removed: {duplicates_removed:,}")

    # Final verification
    print(f"   ✅ Consolidated processing complete")
    print(f"      - Final shape: {deduplicated.shape}")
    print(f"      - Datasets included: {deduplicated['dataset_name'].nunique() if 'dataset_name' in deduplicated.columns else 0}")
    
    if 'dataset_name' in deduplicated.columns:
        dataset_counts = deduplicated['dataset_name'].value_counts()
        for dataset, count in dataset_counts.items():
            print(f"        * {dataset}: {count:,} records")

    # Return the consolidated pandas DataFrame for dbt materialization
    return deduplicated
