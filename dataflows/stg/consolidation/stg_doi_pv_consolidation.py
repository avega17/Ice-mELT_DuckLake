"""
Hamilton dataflow for consolidating DOI PV datasets - Simple GeoPandas approach.

This version bypasses the DuckDB → pandas → GeoPandas conversion chain that was
causing geometry conversion issues and .count() errors by using GeoPandas directly.
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional, Union
import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import geopandas as gpd
from shapely import wkb, wkt
from dotenv import load_dotenv

# TODO: Re-enable geoarrow when multi-geometry support is stable
# Import arro3 for geometry-aware operations (following _doi_pv_helpers_storage.py pattern)
# try:
#     import arro3.core
#     # The working code shows arro3_table = from_geopandas(gdf_copy)
#     # So the correct type is arro3.core.Table
#     Arro3Table = arro3.core.Table
# except ImportError:
#     # Fallback type for environments without arro3
#     Arro3Table = Any

from hamilton.function_modifiers import cache, tag, config
# Removed Parallelizable imports - using sequential only for dbt

# Load environment variables
load_dotenv()

# DuckLake configuration for multi-client concurrent access
# DUCKLAKE_CATALOG = "db/ducklake_catalog.sqlite"
# DUCKLAKE_DATA_PATH = "db/ducklake_data"
DUCKLAKE_CATALOG = os.getenv('DUCKLAKE_CATALOG', 'db/ducklake_catalog.sqlite')
DUCKLAKE_DATA_PATH = os.getenv('DUCKLAKE_DATA_PATH', 'db/ducklake_data')
DEFAULT_SCHEMA = "main"
INGEST_METADATA = os.getenv('INGEST_METADATA', 'data_loaders/doi_manifest.json')

# Standardized table prefix for consistency across local and cloud deployments
RAW_TABLE_PREFIX = "raw_"

H3_DEDUP_RES = int(os.getenv('H3_DEDUP_RES', '12'))  # Use env var or default to 12

# Legacy database path for backward compatibility
DEFAULT_DATABASE_PATH = "db/eo_pv_data.duckdb"


# =============================================================================
# DUCKLAKE CONNECTION HELPERS
# =============================================================================



def _create_ducklake_connection(
    catalog_path: str = DUCKLAKE_CATALOG,
    data_path: str = DUCKLAKE_DATA_PATH,
    use_ducklake: bool = True,
    catalog_type: str = "sqlite"
) -> duckdb.DuckDBPyConnection:
    """
    Create Ibis connection to DuckLake using environment variable configuration.

    Uses DUCKLAKE_CONNECTION_STRING environment variable for dynamic connection:
    - DEV: ducklake:postgres:host=localhost port=5432 dbname=neondb user=neon password=npg (DATA_PATH '/path/to/data/')
    - PROD: ducklake:postgres:dbname=... (DATA_PATH 's3://bucket/path/')

    Args:
        catalog_path: Legacy parameter (ignored when using env var)
        data_path: Legacy parameter (ignored when using env var)
        use_ducklake: Whether to use DuckLake (True) or fallback to direct DuckDB (False)
        catalog_type: Legacy parameter (ignored when using env var)

    Returns:
        Ibis connection to DuckLake or DuckDB
    """
    if use_ducklake:
        # Use target-based environment variable selection
        target_name = os.getenv('DBT_TARGET', 'dev')

        if target_name == 'prod':
            ducklake_connection_string = os.getenv('DUCKLAKE_CONNECTION_STRING_PROD')
        else:
            ducklake_connection_string = os.getenv('DUCKLAKE_CONNECTION_STRING_DEV')

        if ducklake_connection_string:
            print(f"🔗 Using DuckLake connection for target '{target_name}': {ducklake_connection_string[:75]}...")

            # Use proven DuckDB connection pattern from doi_pv_locations.py
            import time
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    print(f"   🔗 Attempting to connect to DuckLake catalog (attempt {attempt + 1}/{max_retries})")

                    # Create DuckDB connection
                    conn = duckdb.connect()

                    # Debug: Show exactly what we're trying to execute
                    print(f"   🔍 Raw connection string: {repr(ducklake_connection_string)}")

                    # Use the exact 3 lines that work in CLI
                    conn.execute("INSTALL ducklake; LOAD ducklake;")

                    # Parse the connection string to match your working CLI format exactly
                    if ' (DATA_PATH ' in ducklake_connection_string:
                        # Split: "ducklake:postgres:... (DATA_PATH 'datapath')" or legacy sqlite format
                        catalog_part = ducklake_connection_string.split(' (DATA_PATH ')[0]
                        data_part = ducklake_connection_string.split(' (DATA_PATH ')[1].rstrip(')')
                        # Reconstruct like your CLI: ATTACH 'catalog' AS alias (DATA_PATH 'path');
                        attach_sql = f"ATTACH IF NOT EXISTS '{catalog_part}' AS eo_pv_lakehouse (DATA_PATH {data_part});"
                    else:
                        # Simple format without DATA_PATH
                        attach_sql = f"ATTACH IF NOT EXISTS '{ducklake_connection_string}' AS eo_pv_lakehouse;"

                    print(f"   🔍 Executing: {attach_sql}")
                    conn.execute(attach_sql)

                    conn.execute("USE eo_pv_lakehouse;")

                    # Install additional extensions
                    conn.execute("INSTALL spatial; LOAD spatial;")
                    conn.execute("INSTALL h3 FROM community; LOAD h3;")

                    # Configure S3 settings for R2 access (both dev and prod use R2)
                    if target_name in ['dev', 'prod']:
                        try:
                            conn.execute("INSTALL httpfs; LOAD httpfs;")

                            # Configure R2 credentials from environment variables
                            r2_access_key = os.getenv('R2_ACCESS_KEY_ID')
                            r2_secret_key = os.getenv('R2_SECRET_KEY')

                            if r2_access_key and r2_secret_key:
                                # Configure S3 settings for Cloudflare R2 (DuckDB S3 format)
                                conn.execute(f"SET s3_access_key_id='{r2_access_key}'")
                                conn.execute(f"SET s3_secret_access_key='{r2_secret_key}'")
                                # Configure R2 endpoint for DuckDB S3 compatibility
                                r2_account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
                                if r2_account_id:
                                    conn.execute(f"SET s3_endpoint='{r2_account_id}.r2.cloudflarestorage.com'")
                                else:
                                    # Fallback: let DuckDB auto-detect R2 endpoint
                                    print("   ⚠️  CLOUDFLARE_ACCOUNT_ID not set, using auto-detection")
                                conn.execute("SET s3_use_ssl=true")
                                conn.execute("SET s3_url_style='path'")
                                print("   ✅ R2 cloud storage credentials configured")
                            else:
                                print("   ⚠️  R2 credentials not found in environment")
                        except Exception as e:
                            print(f"   ⚠️  S3/R2 configuration warning: {e}")

                    # Debug: Check what tables are available
                    tables_result = conn.execute("SHOW TABLES;").fetchall()
                    print(f"   📋 Available tables after connection: {[row[0] for row in tables_result]}")

                    print(f"   ✅ Successfully connected to DuckLake catalog")
                    break
                except Exception as e:
                    if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2  # Increased wait time: 2s, 4s, 6s, 8s
                        print(f"⚠️  Database locked, retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"⚠️  DuckLake connection warning: {e}")
                        # Continue with the connection anyway
                        break

            return conn

        # Fallback to legacy path-based approach if env var not set
        # Use native Ibis DuckLake support - much simpler!
        repo_root = Path(REPO_ROOT)
        full_catalog_path = repo_root / catalog_path
        full_data_path = repo_root / data_path

        # Ensure paths exist
        full_catalog_path.parent.mkdir(parents=True, exist_ok=True)
        full_data_path.mkdir(parents=True, exist_ok=True)

        # Create DuckDB connection with DuckLake extension
        con = ibis.duckdb.connect(extensions=["ducklake", "spatial"])

        # Install community extensions manually
        try:
            con.raw_sql("INSTALL h3 FROM community")
            con.raw_sql("LOAD h3")
            # Note: geography extension temporarily commented out due to availability issues
            # con.raw_sql("INSTALL geography FROM community")
            # con.raw_sql("LOAD geography")
        except Exception as e:
            print(f"⚠️  Community extension warning: {e}")

        # Configure DuckLake connection based on catalog type
        if catalog_type == "postgresql":
            # PostgreSQL catalog for cloud deployment (Neon)
            # Install postgres extension for DuckLake PostgreSQL support
            try:
                con.raw_sql("INSTALL postgres")
                con.raw_sql("LOAD postgres")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"      ⚠️  PostgreSQL extension warning: {e}")

            # Install and configure S3 extension for R2 cloud storage
            try:
                con.raw_sql("INSTALL httpfs")
                con.raw_sql("LOAD httpfs")

                # Configure R2 credentials from environment variables
                r2_access_key = os.getenv('R2_ACCESS_KEY_ID')
                r2_secret_key = os.getenv('R2_SECRET_KEY')

                if r2_access_key and r2_secret_key:
                    # Configure S3 settings for Cloudflare R2 (DuckDB S3 format)
                    con.raw_sql(f"SET s3_access_key_id='{r2_access_key}'")
                    con.raw_sql(f"SET s3_secret_access_key='{r2_secret_key}'")
                    # Configure R2 endpoint for DuckDB S3 compatibility
                    r2_account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
                    if r2_account_id:
                        con.raw_sql(f"SET s3_endpoint='{r2_account_id}.r2.cloudflarestorage.com'")
                    else:
                        # Fallback: let DuckDB auto-detect R2 endpoint
                        print("   ⚠️  CLOUDFLARE_ACCOUNT_ID not set, using auto-detection")
                    con.raw_sql("SET s3_use_ssl=true")
                    con.raw_sql("SET s3_url_style='path'")
                    print("✅ R2 cloud storage credentials configured")
                else:
                    print("⚠️  R2 credentials not found, using local storage")

            except Exception as e:
                print(f"⚠️  S3/R2 extension warning: {e}")

            # Build PostgreSQL connection string from environment variables
            pg_host = os.getenv('PGHOST')
            pg_database = os.getenv('PGDATABASE')
            pg_user = os.getenv('PGUSER')
            pg_password = os.getenv('PGPASSWORD')
            pg_port = os.getenv('PGPORT', '5432')

            if not all([pg_host, pg_database, pg_user, pg_password]):
                raise ValueError("PostgreSQL environment variables not configured: PGHOST, PGDATABASE, PGUSER, PGPASSWORD")

            # DuckLake PostgreSQL connection string format
            ducklake_connection_string = f"ducklake:postgres:dbname={pg_database} host={pg_host} port={pg_port} user={pg_user} password={pg_password} sslmode=require"

            # Configure data path for cloud or local storage
            if data_path.startswith('s3://'):
                # Use S3-compatible R2 path directly
                cloud_data_path = data_path
                print(f"☁️  Using R2 cloud storage: {cloud_data_path}")
            else:
                # Check if R2 credentials are configured and create cloud path
                r2_access_key = os.getenv('R2_ACCESS_KEY_ID')
                r2_secret_key = os.getenv('R2_SECRET_KEY')
                if r2_access_key and r2_secret_key:
                    # Default R2 bucket and path structure
                    cloud_data_path = "r2://eo_pv_lakehouse/ducklake_data"
                    print(f"☁️  Using default R2 path: {cloud_data_path}")
                else:
                    # Fallback to local path for testing
                    repo_root = Path(REPO_ROOT)
                    full_data_path = repo_root / data_path
                    full_data_path.mkdir(parents=True, exist_ok=True)
                    cloud_data_path = str(full_data_path) + "/"
                    print(f"💾 Using local storage: {cloud_data_path}")

            print(f"🔗 Attaching DuckLake PostgreSQL: {pg_host}/{pg_database}")

            attach_sql = f"""
            ATTACH '{ducklake_connection_string}' AS eo_pv_lakehouse
                (DATA_PATH '{cloud_data_path}');
            """

        else:
            # PostgreSQL catalog for local deployment (unified with prod)
            ducklake_connection_string = os.getenv('DUCKLAKE_CONNECTION_STRING',
                                                  'ducklake:postgres:host=localhost port=5432 dbname=neondb user=neon password=npg')
            print(f"🔗 Attaching DuckLake PostgreSQL: {ducklake_connection_string}")

            attach_sql = f"""
            ATTACH '{ducklake_connection_string}' AS eo_pv_lakehouse
                (DATA_PATH '{full_data_path}/');
            """

        # Retry logic for SQLite database lock issues
        import time
        max_retries = 5  # Increased from 3 to 5
        for attempt in range(max_retries):
            try:
                # Use raw SQL to attach DuckLake with DATA_PATH
                con.raw_sql(attach_sql)
                con.raw_sql("USE eo_pv_lakehouse")
                # install and load extensions (spatial is core, no repo needed)
                con.raw_sql("LOAD spatial;")
                con.raw_sql("INSTALL arrow FROM community; LOAD arrow;")
                # load geoarrow type definitions from arrow extension
                con.raw_sql("CALL register_geoarrow_extensions();")
                con.raw_sql("INSTALL h3 FROM community; LOAD h3;")
                print(f"✅ Connected to DuckLake: {ducklake_connection_string}")
                break
            except Exception as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # Increased wait time: 2s, 4s, 6s, 8s
                    print(f"⚠️  Database locked, retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"⚠️  DuckLake connection warning: {e}")
                    # Continue with the connection anyway
                    break

        return con
    else:
        # Fallback to direct DuckDB connection (legacy mode)
        database_path = Path(REPO_ROOT) / DEFAULT_DATABASE_PATH
        con = ibis.duckdb.connect(str(database_path))

        # Load spatial extensions (spatial is core, no install needed)
        try:
            con.raw_sql("LOAD spatial")
        except Exception as e:
            print(f"⚠️  Spatial extension warning: {e}")

        return con


# =============================================================================
# CONSOLIDATION CONFIGURATION
# =============================================================================

# Required columns for consolidated schema (matching original consolidation module)
REQUIRED_COLUMNS = [
    'dataset_name', 'geometry', 'centroid_lon', 'centroid_lat',
    'area_m2', 'processed_at', 'unified_id', 'source_area_m2', 'capacity_mw',
    'install_date'
]

# Field candidates for COALESCE-based standardization (following fetch_and_preprocess.py pattern)
STANDARDIZED_FIELDS = {
    'unified_id': ['fid', 'sol_id', 'GID_0', 'polygon_id', 'unique_id', 'osm_id', 'FID_PV_202'],
    'source_area_m2': ['Shape_Area', 'Area', 'area_sqm', 'panels.area', 'area'],
    'capacity_mw': ['capacity_mw', 'power', 'capacity'],
    'install_date': ['install_date', 'installation_date', 'Date'],
    f'h3_index_{H3_DEDUP_RES}': ['h3_index_12', 'h3_index_10', 'h3_index_8']
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


# Removed parallel version - using sequential only for dbt


@tag(stage="consolidation", data_type="list")
def dataset_list(
    available_datasets: List[str]
) -> List[str]:
    """
    Extract dataset names for sequential processing as a simple list.

    Following the working pattern from doi_pv_locations.py.
    """
    print(f"🔄 Processing {len(available_datasets)} datasets sequentially")

    return available_datasets


@tag(stage="spatial", data_type="table")
@config.when(calculate_geometry_stats=True)
def geometry_stats_calculated(
    raw_table_from_catalog: Any,  # GeoArrow table or GeoPandas from raw_table_from_catalog
    use_ducklake: bool = True
) -> gpd.GeoDataFrame:
    """Calculate geometry statistics from WKB geometry using GeoPandas."""

    # Simplified: assume input is GeoPandas DataFrame (no geoarrow for now)
    gdf = raw_table_from_catalog.copy()
    print(f"   ✅ Using GeoPandas input for geometry stats")

    # Project to appropriate CRS for accurate area/centroid calculations
    # Use Web Mercator (EPSG:3857) for global datasets - good balance of accuracy
    gdf_proj = gdf.to_crs(epsg=3857)
    print(f"   🗺️  Projected to EPSG:3857 for accurate calculations")

    # Calculate geometry statistics using projected CRS
    gdf['area_m2'] = gdf_proj.geometry.area  # Already in square meters for EPSG:3857

    # Calculate centroids in projected CRS, then convert back to lat/lon
    centroids_proj = gdf_proj.geometry.centroid
    centroids_wgs84 = centroids_proj.to_crs(epsg=4326)  # Convert back to WGS84
    gdf['centroid_lat'] = centroids_wgs84.y
    gdf['centroid_lon'] = centroids_wgs84.x

    print(f"   ✅ Calculated geometry stats for {len(gdf)} features")
    print(f"   📊 Latitude range: {gdf['centroid_lat'].min():.2f} to {gdf['centroid_lat'].max():.2f}")
    print(f"   📊 Longitude range: {gdf['centroid_lon'].min():.2f} to {gdf['centroid_lon'].max():.2f}")

    return gdf

@tag(stage="spatial", data_type="table")
@config.when(assign_h3_index=True)
def h3_spatial_index_assigned(
    geometry_stats_calculated: gpd.GeoDataFrame,  # GeoPandas from geometry_stats_calculated
    h3_resolution: int = 8
) -> gpd.GeoDataFrame:
    """Assign H3 spatial index using H3 Python bindings directly."""

    # Simplified: assume input is GeoPandas DataFrame
    gdf = geometry_stats_calculated.copy()
    print(f"   ✅ Using GeoPandas input for H3 indexing")

    # Use H3 Python bindings directly (avoids geometry format battles with DuckDB)
    import h3.api.memview_int as h3

    h3_column_name = f"h3_index_{h3_resolution}"

    # Apply H3 indexing using Python h3 library
    def get_h3_index(lat, lon, resolution):
        if pd.notna(lat) and pd.notna(lon):
            return h3.latlng_to_cell(lat, lon, resolution)
        else:
            return None

    gdf[h3_column_name] = gdf.apply(
        lambda row: get_h3_index(row['centroid_lat'], row['centroid_lon'], h3_resolution),
        axis=1
    )

    # Count successful H3 assignments
    h3_count = gdf[h3_column_name].notna().sum()
    print(f"   ✅ Added H3 index (resolution {h3_resolution}) to {h3_count}/{len(gdf)} features")

    return gdf


@tag(stage="consolidation", data_type="table")
def raw_table_from_catalog(
    dataset_name: str,  # Renamed to match staging model config
    use_ducklake: bool = True,
    catalog_path: str = DUCKLAKE_CATALOG,
    data_path: str = DUCKLAKE_DATA_PATH
) -> gpd.GeoDataFrame:
    """Load single dataset from DuckLake catalog and convert geometry to WKB.

    Returns GeoPandas DataFrame with WKB geometry (following _doi_pv_helpers_storage.py pattern).
    """
    conn = _create_ducklake_connection(catalog_path, data_path, use_ducklake, "sqlite")
    # Note: conn is now a DuckDB connection, not Ibis

    table_name = f"{RAW_TABLE_PREFIX}{dataset_name}"

    # Use direct DuckDB query (no Ibis needed for simple operations)
    try:
        # Check if table exists first
        check_query = f"SELECT COUNT(*) as count FROM {table_name} LIMIT 1"
        result = conn.execute(check_query).fetchone()
        print(f"   ✅ Successfully found table: {table_name} (sample count: {result[0] if result else 'unknown'})")
    except Exception as e:
        print(f"   ❌ Table not found: {table_name}")
        # Show available tables for debugging
        try:
            tables_result = conn.execute("SHOW TABLES").fetchall()
            available_tables = [row[0] for row in tables_result]
            print(f"   📋 Available tables: {available_tables}")
        except:
            print(f"   📋 Could not list available tables")
        raise e

    # Check which geometry columns are available and their types
    # Raw models now provide both geometry_wkt and geometry_wkb
    columns_query = f"DESCRIBE {table_name}"
    columns_result = conn.execute(columns_query).fetchall()
    available_columns = [row[0] for row in columns_result]

    # Check for geometry columns (all raw models now have both)
    has_wkt = 'geometry_wkt' in available_columns
    has_wkb = 'geometry_wkb' in available_columns

    print(f"   🔍 Geometry columns: WKT={has_wkt}, WKB={has_wkb}")

    # Use the best available option (prefer WKT for simplicity)
    if has_wkt:
        geometry_column = "geometry_wkt"
        geometry_type = "WKT"
        print(f"   ✅ Using geometry_wkt column")
    elif has_wkb:
        geometry_column = "geometry_wkb"
        geometry_type = "WKB"
        print(f"   ✅ Using geometry_wkb column")
    else:
        raise ValueError(f"No geometry_wkt or geometry_wkb column found in table {table_name}")

    query = f"""
        SELECT * EXCLUDE ({geometry_column}),
            {geometry_column} as geometry,
            '{dataset_name}' as dataset_name,
            CURRENT_TIMESTAMP as processed_at
        FROM {table_name}
        WHERE {geometry_column} IS NOT NULL AND {geometry_column} != ''  -- Filter out null/empty geometries
    """

    # Execute query and get pandas DataFrame (not .arrow() which loses geometry!)
    result = conn.execute(query)
    df = result.df()
    print(f"   🔄 Converting df with {len(df)} geometries to GeoDataFrame")

   
    # Use the correct conversion based on the detected geometry type
    if geometry_type == "WKB":
        # WKB binary data - use proper WKB conversion
        df['geometry'] = df['geometry'].apply(lambda x: wkb.loads(x) if x and pd.notna(x) else None)
        print(f"   🔄 Converted WKB binary to geometry objects")
    elif geometry_type == "WKT":
        # WKT text data
        df['geometry'] = df['geometry'].apply(lambda x: wkt.loads(x) if x and pd.notna(x) and x != '' else None)
        print(f"   🔄 Converted WKT text to geometry objects")
    else:
        print(f"   ⚠️  Unknown geometry type: {geometry_type}")
        df['geometry'] = None

    # Create GeoPandas DataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=DEFAULT_CRS)
    print(f"   �️  Created GeoPandas DataFrame with {len(gdf)} features")

    # TODO: Add GeoArrow conversion back later when multi-geometry support is stable
    # For now, just return GeoPandas DataFrame for Hamilton processing
    return gdf


def standardized_dataset_table(
    h3_spatial_index_assigned: gpd.GeoDataFrame,
    dataset_name: str,  # Single dataset name (renamed to avoid Hamilton dependency)
    h3_resolution: int = 8
) -> gpd.GeoDataFrame:
    """Apply final standardization with COALESCE field mapping and proper null handling."""
    gdf = h3_spatial_index_assigned.copy()
    available_columns = gdf.columns
    h3_column_name = f"h3_index_{h3_resolution}"

    print(f"   🔄 Standardizing dataset table with {len(gdf)} features")
    # print(f"   📋 Available columns: {list(available_columns)}")

    # Ensure H3 column is included in the final output
    if h3_column_name not in available_columns:
        print(f"   ⚠️  H3 column {h3_column_name} not found in available columns")
    else:
        print(f"   ✅ H3 column {h3_column_name} found")

    # Build standardized DataFrame with ONLY the columns we need
    standardized_data = {}

    # Required metadata columns
    standardized_data['dataset_name'] = gdf['dataset_name']
    standardized_data['processed_at'] = gdf['processed_at']

    # VITAL: Include geometry column (the whole point of the project!)
    if 'geometry' in gdf.columns:
        standardized_data['geometry'] = gdf['geometry']
        print(f"   ✅ Geometry column included")
    else:
        print(f"   ⚠️  No geometry column found!")

    # Geometry statistics (already calculated)
    standardized_data['area_m2'] = gdf['area_m2']
    standardized_data['centroid_lat'] = gdf['centroid_lat']
    standardized_data['centroid_lon'] = gdf['centroid_lon']

    # H3 index (already calculated)
    if h3_column_name in available_columns:
        standardized_data[h3_column_name] = gdf[h3_column_name]
        print(f"   ✅ H3 column included: {h3_column_name}")

    # Apply COALESCE logic for standardized fields
    for std_field, candidates in STANDARDIZED_FIELDS.items():
        present_candidates = [col for col in candidates if col in available_columns]
        if present_candidates:
            if std_field in ['unified_id', 'install_date']:
                # String fields - use pandas fillna for COALESCE behavior
                result_series = gdf[present_candidates[0]].astype(str)
                for col in present_candidates[1:]:
                    result_series = result_series.fillna(gdf[col].astype(str))
                standardized_data[std_field] = result_series
                print(f"   ✅ {std_field}: COALESCE({', '.join(present_candidates)})")
            elif std_field in ['source_area_m2', 'capacity_mw']:
                # Numeric fields - use pandas to_numeric for safe conversion
                result_series = pd.to_numeric(gdf[present_candidates[0]], errors='coerce')
                for col in present_candidates[1:]:
                    result_series = result_series.fillna(pd.to_numeric(gdf[col], errors='coerce'))
                standardized_data[std_field] = result_series
                print(f"   ✅ {std_field}: COALESCE({', '.join(present_candidates)})")
        else:
            # Add NULL placeholder for missing fields
            if std_field in ['unified_id', 'install_date']:
                standardized_data[std_field] = None
            elif std_field in ['source_area_m2', 'capacity_mw']:
                standardized_data[std_field] = None
            print(f"   ⚠️  {std_field}: No candidates found, using NULL")

    standardized_gdf = gpd.GeoDataFrame(standardized_data, geometry='geometry', crs=DEFAULT_CRS)

    print(f"   ✅ Standardized DataFrame created with {len(standardized_gdf)} records and {len(standardized_gdf.columns)} columns")
    print(f"   📋 Standardized columns: {list(standardized_gdf.columns)}")

    return standardized_gdf


@tag(stage="consolidation", data_type="table")
@cache(behavior="disable")  # Disable caching for large tables
def geometry_processed_geodataframe(
    standardized_dataset_table: gpd.GeoDataFrame  # Single consolidated table from sequential processing
) -> gpd.GeoDataFrame:
    """
    Return the already consolidated table from sequential processing.

    In sequential mode, standardized_dataset_table__sequential already does all the consolidation,
    so we just return it directly.
    """
    print("✅ Sequential consolidation already complete - returning consolidated table")
    return standardized_dataset_table


@tag(stage="consolidation", data_type="string")
def staging_table_created(
    geometry_processed_geodataframe: gpd.GeoDataFrame,
    target_table: str = "stg_pv_consolidated"
) -> str:
    """
    Return the target table name since we're working with GeoPandas DataFrames directly.

    In the current approach, we don't need to create database views since we're
    processing everything in memory with GeoPandas.
    """
    print(f"✅ Consolidated staging complete: {target_table}")
    print(f"   📊 Final dataset: {len(geometry_processed_geodataframe)} records")
    print(f"   📋 Columns: {list(geometry_processed_geodataframe.columns)}")

    return target_table
