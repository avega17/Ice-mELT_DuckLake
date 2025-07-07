"""
Hamilton dataflow for consolidating DOI PV datasets - Simple GeoPandas approach.

This version bypasses the DuckDB → pandas → GeoPandas conversion chain that was
causing geometry conversion issues and .count() errors by using GeoPandas directly.
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional
import json
import os
from pathlib import Path

import ibis
import ibis.expr.types as ir
from dotenv import load_dotenv

from hamilton.function_modifiers import cache, tag, config
from hamilton.htypes import Parallelizable, Collect

# Load environment variables
load_dotenv()

# Get repo root and manifest path from environment or use defaults
# Use relative paths as they work better than absolute paths in this environment
REPO_ROOT = str(Path(__file__).parent.parent)
INGEST_METADATA = str(Path(__file__).parent.parent / "data_loaders" / "doi_manifest.json")


# =============================================================================
# CONSOLIDATION CONFIGURATION
# =============================================================================

# Default database configuration
DEFAULT_DATABASE_PATH = "db/eo_pv_data.duckdb"
DEFAULT_SCHEMA = "main"
DOI_TABLE_PREFIX = "doi_"

# Required columns for consolidated schema (matching original consolidation module)
REQUIRED_COLUMNS = [
    'dataset_name', 'source_file', 'geometry', 'centroid_lon', 'centroid_lat',
    'area_m2', 'processed_at', 'unified_id', 'source_area_m2', 'capacity_mw',
    'install_date', 'country_code', 'region_code'
]

# Field candidates for COALESCE-based standardization (following fetch_and_preprocess.py pattern)
STANDARDIZED_FIELDS = {
    'unified_id': ['fid', 'sol_id', 'GID_0', 'polygon_id', 'unique_id', 'osm_id', 'FID_PV_202'],
    'source_area_m2': ['Shape_Area', 'Area', 'area_sqm', 'panels.area', 'area'],
    'capacity_mw': ['capacity_mw', 'power', 'capacity'],
    'install_date': ['install_date', 'installation_date', 'Date'],
    'country_code': ['iso-3166-1', 'ISO_A2', 'Country'],
    'region_code': ['iso-3166-2', 'Province', 'State']
}



# Default CRS for spatial data
DEFAULT_CRS = 'EPSG:4326'


# =============================================================================
# HAMILTON DATAFLOW NODES - SIMPLE CONSOLIDATION
# =============================================================================


@tag(stage="consolidation", data_type="list")
def available_datasets() -> List[str]:
    """Get list of available (non-skipped) datasets."""
    with open(INGEST_METADATA, 'r') as f:
        doi_manifest = json.load(f)

    if not doi_manifest:
        # Fallback to known ingested datasets
        fallback_datasets = [
            'uk_crowdsourced_pv_2020',
            'chn_med_res_pv_2024',
            'usa_cali_usgs_pv_2016', 
            'ind_pv_solar_farms_2022',
            'global_pv_inventory_sent2_spot_2021',
            'global_harmonized_large_solar_farms_2020'
        ]
        print(f"📋 Using fallback dataset list: {len(fallback_datasets)} datasets")
        return fallback_datasets
    
    # Filter out skipped datasets
    available = [
        name for name, config in doi_manifest.items()
        if not config.get('skip', False)
    ]
    
    print(f"📋 Available datasets: {len(available)}")
    if available:
        print(f"   ✅ {', '.join(available)}")
    
    return available


@tag(stage="consolidation", data_type="parallelizable", execution_mode="parallel")
@config.when(execution_mode="parallel")
def dataset_names__parallel(
    available_datasets: List[str]
) -> Parallelizable[str]:
    """
    Extract dataset names for parallel processing using Hamilton's Parallelizable.

    Following the working pattern from doi_pv_locations.py.
    """
    print(f"🔄 Processing {len(available_datasets)} datasets in parallel")

    for dataset_name in available_datasets:
        yield dataset_name


@tag(stage="consolidation", data_type="list", execution_mode="sequential")
@config.when(execution_mode="sequential")
def dataset_names__sequential(
    available_datasets: List[str]
) -> List[str]:
    """
    Extract dataset names for sequential processing as a simple list.

    Following the working pattern from doi_pv_locations.py.
    """
    print(f"🔄 Processing {len(available_datasets)} datasets sequentially")

    return available_datasets


@tag(stage="consolidation", data_type="table", execution_mode="parallel")
@config.when(execution_mode="parallel")
def standardized_dataset_table__parallel(
    dataset_names: str,  # Individual dataset name from parallel processing
    database_path: str = DEFAULT_DATABASE_PATH,
    database_schema: str = DEFAULT_SCHEMA
) -> ir.Table:
    """
    Load and standardize a single dataset table using Ibis DuckDB backend.

    This function processes each dataset independently, applying column filtering
    and schema standardization to ensure all datasets have compatible schemas
    for downstream consolidation.

    Args:
        dataset_name: Name of the dataset to process
        database_path: Path to DuckDB database
        database_schema: Database schema name

    Returns:
        Standardized Ibis table with consistent schema
    """
    # Load manifest configuration
    with open(INGEST_METADATA, 'r') as f:
        doi_manifest = json.load(f)
    table_name = f"{DOI_TABLE_PREFIX}{dataset_names}"

    try:
        print(f"   🔄 Processing {dataset_names}...")

        # Create Ibis DuckDB connection
        con = ibis.duckdb.connect(database_path)

        # Load spatial extension with error handling (may already be loaded)
        try:
            con.raw_sql("INSTALL spatial")
            con.raw_sql("LOAD spatial")
        except Exception as e:
            # Spatial extension may already be loaded, which is fine
            if "already exists" not in str(e).lower():
                print(f"      ⚠️  Spatial extension warning: {e}")
            pass

        # Check if table exists and load it
        try:
            table = con.table(table_name)
        except Exception:
            print(f"   ⚠️  Table {table_name} not found")
            # Return empty table with standardized schema
            return _create_empty_standardized_table(con)

        # Get column filtering configuration from manifest (if available)
        dataset_config = doi_manifest.get(dataset_names, {})
        columns_to_keep = dataset_config.get('keep_cols', [])

        # Build ONLY standardized schema - no original columns
        available_columns = set(table.columns)
        print(f"      🔍 Available columns: {sorted(available_columns)}")

        select_columns = [
            table.dataset_name,  # Always include dataset_name
            table.source_file,   # Always include source_file for traceability
            table.geometry,
            table.geometry.centroid().x().name('centroid_lon'),  # ST_X(ST_Centroid(geometry))
            table.geometry.centroid().y().name('centroid_lat'),  # ST_Y(ST_Centroid(geometry))
            table.geometry.area().fillna(100.0).name('area_m2'),  # ST_Area equivalent
            ibis.now().name('processed_at')
        ]

        # Generate COALESCE expressions for each standardized field with type casting
        for std_field, candidates in STANDARDIZED_FIELDS.items():
            present_candidates = [col for col in candidates if col in available_columns]
            if present_candidates:
                # Create COALESCE expression with proper type casting
                if std_field in ['unified_id', 'country_code', 'region_code', 'install_date']:
                    # Cast to string for ID and text fields
                    coalesce_expr = table[present_candidates[0]].cast('string')
                    for col in present_candidates[1:]:
                        coalesce_expr = coalesce_expr.fillna(table[col].cast('string'))
                    select_columns.append(coalesce_expr.name(std_field))
                elif std_field in ['source_area_m2', 'capacity_mw']:
                    # Cast to float64 for numeric fields, handling None/NULL values
                    first_col = table[present_candidates[0]]
                    # Handle different data types and 'None' strings
                    if first_col.type().is_string():
                        # String column: use TRY_CAST to handle 'None' and invalid values
                        coalesce_expr = ibis.case().when(
                            first_col.isin(['None', 'NULL', '']),
                            ibis.null()
                        ).else_(
                            first_col.try_cast('float64')
                        ).end()
                    else:
                        # Numeric column: cast directly to float64
                        coalesce_expr = first_col.cast('float64')

                    for col in present_candidates[1:]:
                        next_col = table[col]
                        if next_col.type().is_string():
                            next_expr = ibis.case().when(
                                next_col.isin(['None', 'NULL', '']),
                                ibis.null()
                            ).else_(
                                next_col.try_cast('float64')
                            ).end()
                        else:
                            next_expr = next_col.cast('float64')
                        coalesce_expr = coalesce_expr.fillna(next_expr)
                    select_columns.append(coalesce_expr.name(std_field))
                else:
                    # Default: no casting
                    coalesce_expr = table[present_candidates[0]]
                    for col in present_candidates[1:]:
                        coalesce_expr = coalesce_expr.fillna(table[col])
                    select_columns.append(coalesce_expr.name(std_field))
                print(f"      ✅ {std_field}: COALESCE({', '.join(present_candidates)})")
            else:
                # Add NULL placeholder for missing fields with proper type
                if std_field in ['unified_id', 'country_code', 'region_code', 'install_date']:
                    select_columns.append(ibis.null().cast('string').name(std_field))
                elif std_field in ['source_area_m2', 'capacity_mw']:
                    select_columns.append(ibis.null().cast('float64').name(std_field))
                else:
                    select_columns.append(ibis.null().name(std_field))
                print(f"      ⚠️  {std_field}: No candidates found, using NULL")

        # Log column filtering
        if not columns_to_keep:
            print(f"      - No column filter specified for {dataset_names}")
        else:
            print(f"      - Using column filter for {dataset_names}: {columns_to_keep}")

        # Create standardized table with selected columns
        standardized_table = table.select(select_columns)

        # Create individual staging table to catch issues early
        staging_table_name = f"stg_{dataset_names}"
        try:
            # Drop table if it exists
            try:
                con.raw_sql(f"DROP TABLE IF EXISTS {staging_table_name}")
            except:
                pass

            # Create the individual staging table
            con.create_table(staging_table_name, standardized_table)

            # Verify and get count from the created table
            count = con.table(staging_table_name).count().execute()
            if count == 0:
                print(f"   ⚠️  No data in {staging_table_name}")
                return _create_empty_standardized_table(con)

            print(f"   ✅ {dataset_names}: {count} records standardized and staged as {staging_table_name}")

            # Return the table reference for further processing
            return con.table(staging_table_name)

        except Exception as e:
            print(f"   ❌ Failed to create staging table {staging_table_name}: {e}")
            raise

    except Exception as e:
        print(f"   ❌ Error processing {dataset_names}: {e}")
        # Return empty table with standardized schema
        con = ibis.duckdb.connect(database_path)
        return _create_empty_standardized_table(con)


@tag(stage="consolidation", data_type="table", execution_mode="sequential")
@config.when(execution_mode="sequential")
def standardized_dataset_table__sequential(
    dataset_names: List[str],  # List of dataset names for sequential processing
    database_path: str = DEFAULT_DATABASE_PATH,
    database_schema: str = DEFAULT_SCHEMA
) -> ir.Table:
    """
    Process all datasets sequentially and return consolidated table.

    In sequential mode, we process all datasets in a loop and return the consolidated result.
    This follows the working pattern where sequential functions handle the full list.
    """
    print(f"🔄 Processing {len(dataset_names)} datasets sequentially...")

    # Load manifest configuration
    with open(INGEST_METADATA, 'r') as f:
        doi_manifest = json.load(f)

    consolidated_tables = []

    for dataset_name in dataset_names:
        try:
            print(f"   🔄 Processing {dataset_name}...")

            # Create Ibis DuckDB connection
            con = ibis.duckdb.connect(database_path)
            table_name = f"{DOI_TABLE_PREFIX}{dataset_name}"

            # Check if table exists
            if table_name not in con.list_tables():
                print(f"   ⚠️  Table {table_name} not found, skipping")
                continue

            # Load table
            table = con.table(table_name)

            # Get column filtering configuration from manifest (if available)
            dataset_config = doi_manifest.get(dataset_name, {})
            columns_to_keep = dataset_config.get('keep_cols', [])

            # Build ONLY standardized schema - no original columns
            available_columns = set(table.columns)
            print(f"      🔍 Available columns: {sorted(available_columns)}")

            select_columns = [
                table.dataset_name,  # Always include dataset_name
                table.source_file,   # Always include source_file for traceability
                table.geometry,
                table.geometry.centroid().x().name('centroid_lon'),  # ST_X(ST_Centroid(geometry))
                table.geometry.centroid().y().name('centroid_lat'),  # ST_Y(ST_Centroid(geometry))
                table.geometry.area().fillna(100.0).name('area_m2'),  # ST_Area equivalent
                ibis.now().name('processed_at')
            ]

            # Generate COALESCE expressions for each standardized field with type casting
            for std_field, candidates in STANDARDIZED_FIELDS.items():
                present_candidates = [col for col in candidates if col in available_columns]
                if present_candidates:
                    # Create COALESCE expression with proper type casting
                    if std_field in ['unified_id', 'country_code', 'region_code', 'install_date']:
                        # Cast to string for ID and text fields
                        coalesce_expr = table[present_candidates[0]].cast('string')
                        for col in present_candidates[1:]:
                            coalesce_expr = coalesce_expr.fillna(table[col].cast('string'))
                        select_columns.append(coalesce_expr.name(std_field))
                    elif std_field in ['source_area_m2', 'capacity_mw']:
                        # Cast to float64 for numeric fields, handling None/NULL values
                        first_col = table[present_candidates[0]]
                        # Handle different data types and 'None' strings
                        if first_col.type().is_string():
                            # String column: use TRY_CAST to handle 'None' and invalid values
                            coalesce_expr = ibis.case().when(
                                first_col.isin(['None', 'NULL', '']),
                                ibis.null()
                            ).else_(
                                first_col.try_cast('float64')
                            ).end()
                        else:
                            # Numeric column: cast directly to float64
                            coalesce_expr = first_col.cast('float64')

                        for col in present_candidates[1:]:
                            next_col = table[col]
                            if next_col.type().is_string():
                                next_expr = ibis.case().when(
                                    next_col.isin(['None', 'NULL', '']),
                                    ibis.null()
                                ).else_(
                                    next_col.try_cast('float64')
                                ).end()
                            else:
                                next_expr = next_col.cast('float64')
                            coalesce_expr = coalesce_expr.fillna(next_expr)
                        select_columns.append(coalesce_expr.name(std_field))
                    else:
                        # Default: no casting
                        coalesce_expr = table[present_candidates[0]]
                        for col in present_candidates[1:]:
                            coalesce_expr = coalesce_expr.fillna(table[col])
                        select_columns.append(coalesce_expr.name(std_field))
                    print(f"      ✅ {std_field}: COALESCE({', '.join(present_candidates)})")
                else:
                    # Add NULL placeholder for missing fields with proper type
                    if std_field in ['unified_id', 'country_code', 'region_code', 'install_date']:
                        select_columns.append(ibis.null().cast('string').name(std_field))
                    elif std_field in ['source_area_m2', 'capacity_mw']:
                        select_columns.append(ibis.null().cast('float64').name(std_field))
                    else:
                        select_columns.append(ibis.null().name(std_field))
                    print(f"      ⚠️  {std_field}: No candidates found, using NULL")

            # Debug: Print what we're selecting
            print(f"      🔧 Selecting {len(select_columns)} columns for standardization")

            # Create standardized table with selected columns
            standardized_table = table.select(select_columns)

            # Create individual staging table to catch issues early
            staging_table_name = f"stg_{dataset_name}"
            try:
                # Drop table if it exists
                try:
                    con.raw_sql(f"DROP TABLE IF EXISTS {staging_table_name}")
                except:
                    pass

                # Create the individual staging table
                con.create_table(staging_table_name, standardized_table)

                # Verify and get count from the created table
                count = con.table(staging_table_name).count().execute()
                if count > 0:
                    # Use the table reference from the database
                    consolidated_tables.append(con.table(staging_table_name))
                    print(f"   ✅ {dataset_name}: {count} records standardized and staged as {staging_table_name}")
                else:
                    print(f"   ⚠️  {dataset_name}: No data found")

            except Exception as e:
                print(f"   ❌ Failed to create staging table {staging_table_name}: {e}")
                continue

        except Exception as e:
            print(f"   ❌ Error processing {dataset_name}: {e}")
            continue

    if not consolidated_tables:
        print("⚠️  No datasets successfully processed")
        con = ibis.duckdb.connect(database_path)
        return _create_empty_standardized_table(con)

    # Union all tables
    if len(consolidated_tables) == 1:
        result_table = consolidated_tables[0]
    else:
        result_table = consolidated_tables[0]
        for table in consolidated_tables[1:]:
            result_table = result_table.union(table)

    total_count = result_table.count().execute()
    print(f"   ✅ Sequential processing complete: {total_count} total records from {len(consolidated_tables)} datasets")

    return result_table


@tag(stage="consolidation", data_type="table", execution_mode="parallel")
@cache(behavior="disable")  # Disable caching for large tables
@config.when(execution_mode="parallel")
def geometry_processed_geodataframe__parallel(
    standardized_dataset_table: Collect[ir.Table]
) -> ir.Table:
    """
    Consolidate individual datasets into unified Ibis table.

    This creates a clean, consolidated Ibis table ready for staging transformations.
    """
    # Convert Collect to list for processing
    standardized_tables = list(standardized_dataset_table)

    if not standardized_tables:
        print("⚠️  No datasets to consolidate")
        # Return empty table with expected schema
        con = ibis.duckdb.connect()
        return con.sql("SELECT NULL as dataset_name, NULL as geometry, NULL as centroid_lon, NULL as centroid_lat, NULL as area_m2, NULL as processed_at WHERE FALSE")

    print(f"🔄 Consolidating {len(standardized_tables)} standardized datasets...")

    tables_to_union = []
    total_records = 0

    for table in standardized_tables:
        try:
            # Get record count
            count = table.count().execute()
            if count == 0:
                print(f"   ⚠️  Skipping empty table")
                continue

            tables_to_union.append(table)
            total_records += count

            # Get dataset name from the table for logging
            try:
                dataset_name = table.select('dataset_name').limit(1).execute()['dataset_name'].iloc[0]
                print(f"   ✅ {dataset_name}: {count} records")
            except:
                print(f"   ✅ Table: {count} records")

        except Exception as e:
            print(f"   ⚠️  Error processing table: {e}")
            continue

    if not tables_to_union:
        print("⚠️  No valid datasets to consolidate")
        con = ibis.duckdb.connect()
        return con.sql("SELECT NULL as dataset_name, NULL as geometry, NULL as centroid_lon, NULL as centroid_lat, NULL as area_m2, NULL as processed_at WHERE FALSE")

    # Union all tables
    try:
        if len(tables_to_union) == 1:
            consolidated_table = tables_to_union[0]
        else:
            # Use Ibis union to combine all tables
            consolidated_table = tables_to_union[0]
            for table in tables_to_union[1:]:
                consolidated_table = consolidated_table.union(table)

        # Get final count
        final_count = consolidated_table.count().execute()
        unique_datasets = consolidated_table.dataset_name.nunique().execute()

        print(f"✅ Consolidation complete:")
        print(f"   📊 Total records: {final_count}")
        print(f"   🗂️  Datasets: {unique_datasets}")
        print(f"   📋 Columns: {len(consolidated_table.columns)}")

        return consolidated_table

    except Exception as e:
        print(f"❌ Error during consolidation: {e}")
        con = ibis.duckdb.connect()
        return con.sql("SELECT NULL as dataset_name, NULL as geometry, NULL as centroid_lon, NULL as centroid_lat, NULL as area_m2, NULL as processed_at WHERE FALSE")


@tag(stage="consolidation", data_type="table", execution_mode="sequential")
@cache(behavior="disable")  # Disable caching for large tables
@config.when(execution_mode="sequential")
def geometry_processed_geodataframe__sequential(
    standardized_dataset_table: ir.Table  # Single consolidated table from sequential processing
) -> ir.Table:
    """
    Return the already consolidated table from sequential processing.

    In sequential mode, standardized_dataset_table__sequential already does all the consolidation,
    so we just return it directly.
    """
    print("✅ Sequential consolidation already complete - returning consolidated table")
    return standardized_dataset_table


@tag(stage="consolidation", data_type="string", execution_mode="sequential")
@config.when(execution_mode="sequential")
def staging_table_created__sequential(
    geometry_processed_geodataframe: ir.Table,
    target_table: str = "stg_pv_consolidated",
    database_path: str = DEFAULT_DATABASE_PATH
) -> str:
    """
    Create a consolidated view that unions all individual staging tables.

    This creates a view instead of a materialized table, which is more efficient
    and allows querying individual staging tables separately.
    """
    print(f"🔄 Creating consolidated staging view: {target_table}")

    # Get the connection
    con = geometry_processed_geodataframe._find_backend()

    try:
        # Find all staging tables
        all_tables = con.list_tables()
        staging_tables = [t for t in all_tables if t.startswith('stg_') and t != target_table]

        if not staging_tables:
            raise Exception("No individual staging tables found to consolidate")

        print(f"   📋 Found {len(staging_tables)} staging tables: {staging_tables}")

        # Create UNION ALL query for all staging tables
        union_query = " UNION ALL ".join([f"SELECT * FROM {table}" for table in staging_tables])
        view_sql = f"CREATE OR REPLACE VIEW {target_table} AS {union_query}"

        # Create the view
        con.raw_sql(view_sql)

        # Verify the view was created and get count
        view_table = con.table(target_table)
        count = view_table.count().execute()

        print(f"✅ Consolidated staging view created: {target_table} with {count} records from {len(staging_tables)} tables")
        return target_table

    except Exception as e:
        print(f"❌ Failed to create staging view: {e}")
        raise


@tag(stage="consolidation", data_type="string", execution_mode="parallel")
@config.when(execution_mode="parallel")
def staging_table_created__parallel(
    geometry_processed_geodataframe: ir.Table,
    target_table: str = "stg_pv_consolidated",
    database_path: str = DEFAULT_DATABASE_PATH
) -> str:
    """
    Create a consolidated view that unions all individual staging tables.

    This creates a view instead of a materialized table, which is more efficient
    and allows querying individual staging tables separately.
    """
    print(f"🔄 Creating consolidated staging view: {target_table}")

    # Get the connection
    con = geometry_processed_geodataframe._find_backend()

    try:
        # Find all staging tables
        all_tables = con.list_tables()
        print(f"   🔍 All tables in database: {sorted(all_tables)}")

        staging_tables = [t for t in all_tables if t.startswith('stg_') and t != target_table]
        print(f"   🔍 Staging tables found: {staging_tables}")

        if not staging_tables:
            print(f"   ❌ No staging tables found. Looking for tables starting with 'stg_' excluding '{target_table}'")
            raise Exception("No individual staging tables found to consolidate")

        print(f"   📋 Found {len(staging_tables)} staging tables: {staging_tables}")

        # Create UNION ALL query for all staging tables
        union_query = " UNION ALL ".join([f"SELECT * FROM {table}" for table in staging_tables])
        view_sql = f"CREATE OR REPLACE VIEW {target_table} AS {union_query}"

        # Create the view
        con.raw_sql(view_sql)

        # Verify the view was created and get count
        view_table = con.table(target_table)
        count = view_table.count().execute()

        print(f"✅ Consolidated staging view created: {target_table} with {count} records from {len(staging_tables)} tables")
        return target_table

    except Exception as e:
        print(f"❌ Failed to create staging view: {e}")
        raise


# =============================================================================
# HELPER FUNCTIONS (prefixed with _ to exclude from DAG visualization)
# =============================================================================

def _create_empty_standardized_table(con) -> ir.Table:
    """
    Create an empty table with the standardized schema.

    This ensures all tables have the same schema for union operations.
    """
    return con.sql("""
        SELECT
            NULL::VARCHAR as dataset_name,
            NULL::VARCHAR as source_file,
            NULL::GEOMETRY as geometry,
            NULL::DOUBLE as centroid_lon,
            NULL::DOUBLE as centroid_lat,
            NULL::DOUBLE as area_m2,
            NULL::TIMESTAMP as processed_at,
            NULL::VARCHAR as unified_id,
            NULL::DOUBLE as source_area_m2,
            NULL::DOUBLE as capacity_mw,
            NULL::VARCHAR as install_date,
            NULL::VARCHAR as country_code,
            NULL::VARCHAR as region_code
        WHERE FALSE
    """)
