"""
Hamilton dataflow for standardizing and consolidating DOI PV datasets



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

import sys as _sys
# Prefer REPO_ROOT from .env if available; fall back to relative project root
load_dotenv()
_PROJECT_ROOT = os.getenv("REPO_ROOT") or os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if _PROJECT_ROOT not in _sys.path:
    _sys.path.insert(0, _PROJECT_ROOT)

from utils.ducklake import _create_ducklake_connection
from utils.hamilton_driver import build_driver

from hamilton.htypes import Parallelizable, Collect

from hamilton.execution import executors

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

from hamilton.function_modifiers import cache, config

# Load environment variables (already loaded above for REPO_ROOT, safe to call again if needed)

# DuckLake configuration for multi-client concurrent access
DUCKLAKE_CATALOG = os.getenv('DUCKLAKE_CATALOG', 'db/ducklake_catalog.sqlite')
DUCKLAKE_DATA_PATH = os.getenv('DUCKLAKE_DATA_PATH', 'db/ducklake_data')
DEFAULT_SCHEMA = "main"
INGEST_METADATA = os.getenv('INGEST_METADATA', 'ingest/doi_manifest.json')

# Standardized table prefix for consistency across local and cloud deployments
RAW_TABLE_PREFIX = "raw_"

H3_DEDUP_RES = int(os.getenv('H3_DEDUP_RES', '12'))  # Use env var or default to 12

# Legacy database path for backward compatibility
DEFAULT_DATABASE_PATH = "db/eo_pv_data.duckdb"


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


def dataset_list(
    available_datasets: List[str],
    dataset_list_override: Optional[List[str]] = None,
) -> List[str]:
    """
    Extract dataset names for sequential processing as a simple list.

    Following the working pattern from doi_pv_locations.py.
    """
    names = dataset_list_override if dataset_list_override else available_datasets
    print(f"🔄 Processing the following {len(names)} datasets:")
    for ds in names:
        print(f"   - {ds}")
    return names

@config.when(execution_mode="parallel")
def dataset_names(
    available_datasets: List[str],
    dataset_list_override: Optional[List[str]] = None,
) -> Parallelizable[str]:
    """Parallelizable dataset iterator fed by available_datasets or an override.

    - In dynamic execution mode, this yields one item per dataset.
    - To restrict datasets from CLI, pass `inputs={"dataset_list_override": [..]}`.
    """
    names = dataset_list_override if dataset_list_override else available_datasets
    for name in names:
        yield name


@config.when(execution_mode="parallel")
def dataset_name(dataset_names: str) -> str:
    """Adapter node to reuse existing nodes that expect `dataset_name` input."""
    return dataset_names


@config.when(execution_mode="parallel")
def consolidated_standardized_table(
    standardized_dataset_table: Collect[gpd.GeoDataFrame],
) -> gpd.GeoDataFrame:
    """Collect standardized tables from all datasets and concatenate into one GeoDataFrame."""

    parts = list(standardized_dataset_table)
    if not parts:
        return gpd.GeoDataFrame(columns=[
            'dataset_name', 'geometry', 'centroid_lon', 'centroid_lat',
            'area_m2', 'processed_at', 'unified_id', 'source_area_m2', 'capacity_mw',
            'install_date'
        ], geometry='geometry', crs=DEFAULT_CRS)

    combined = gpd.GeoDataFrame(
        pd.concat(parts, ignore_index=True),
        geometry='geometry',
        crs=DEFAULT_CRS,
    )
    return combined


@config.when(execution_mode="parallel")
def export_consolidated_geoparquet(
    consolidated_standardized_table: gpd.GeoDataFrame,
    export_path: str,
) -> str:
    """Export the consolidated GeoDataFrame to a GeoParquet path and return the path."""
    out_path = Path(export_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    consolidated_standardized_table.to_parquet(out_path, compression="snappy")
    print(f"💾 Exported consolidated GeoParquet to: {out_path}")
    return str(out_path)

    # print(f"🔄 Processing the following {len(available_datasets)} datasets sequentially:")
    # print(f"   {',\n    '.join(available_datasets)}")

    # return available_datasets


@config.when(calculate_geometry_stats=True)
def geometry_stats_calculated(
    raw_table_from_catalog: gpd.GeoDataFrame,  # GeoArrow table or GeoDataFrame from raw_table_from_catalog
    use_ducklake: bool = True
) -> gpd.GeoDataFrame:
    """Calculate geometry statistics from WKB geometry using GeoPandas."""

    # Simplified: assume input is GeoPandas DataFrame (no geoarrow for now)
    print(f"   📊 Calculating geometry stats for {len(raw_table_from_catalog)} features...")
    gdf = raw_table_from_catalog
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

@config.when(assign_h3_index=True)
def h3_spatial_index_assigned(
    geometry_stats_calculated: gpd.GeoDataFrame,
    grid_resolution: int = 8
) -> gpd.GeoDataFrame:
    """Assign H3 spatial index using H3 Python bindings directly."""

    # Simplified: assume input is GeoPandas DataFrame
    gdf = geometry_stats_calculated.copy()
    print(f"   ✅ Using GeoPandas input for H3 indexing")

    # Use H3 Python bindings directly (avoids geometry format battles with DuckDB)
    import h3.api.memview_int as h3

    h3_column_name = f"h3_index_{grid_resolution}"

    # Apply H3 indexing using Python h3 library
    def get_h3_index(lat, lon, resolution):
        if pd.notna(lat) and pd.notna(lon):
            return h3.latlng_to_cell(lat, lon, resolution)
        else:
            return None

    gdf[h3_column_name] = gdf.apply(
        lambda row: get_h3_index(row['centroid_lat'], row['centroid_lon'], grid_resolution),
        axis=1
    )

    # Count successful H3 assignments
    h3_count = gdf[h3_column_name].notna().sum()
    print(f"   ✅ Added H3 index (resolution {grid_resolution}) to {h3_count}/{len(gdf)} features")

    return gdf


def raw_table_from_catalog(
    dataset_name: str,  # Renamed to match staging model config
    use_ducklake: bool = True,
    catalog_path: str = DUCKLAKE_CATALOG,
    data_path: str = DUCKLAKE_DATA_PATH
) -> gpd.GeoDataFrame:
    """Load single dataset from DuckLake catalog and convert geometry to WKB.

    Returns GeoPandas DataFrame with WKB geometry (following _doi_pv_helpers_storage.py pattern).
    """
    conn = _create_ducklake_connection()
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
    if geometry_type == "WKT":
        # WKT text data
        df['geometry'] = df['geometry'].apply(lambda x: wkt.loads(x) if x and pd.notna(x) and x != '' else None)
        # only keep one geometry column
        if has_wkb:
            df.drop(columns=['geometry_wkb'], inplace=True)
        print(f"   🔄 Converted WKT text to geometry objects")

    elif geometry_type == "WKB":
        # WKB binary data - use proper WKB conversion
        df['geometry'] = df['geometry'].apply(lambda x: wkb.loads(x) if x and pd.notna(x) else None)
        print(f"   🔄 Converted WKB binary to geometry objects")
    # else:
    #     print(f"   ⚠️  Unknown geometry type: {geometry_type}")
    #     df['geometry'] = None

    # Create GeoPandas DataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=DEFAULT_CRS)
    #
    print(f"   ✅ Created GeoPandas DataFrame with {len(gdf)} features")

    # TODO: Add GeoArrow conversion back later when multi-geometry support is stable
    # For now, just return GeoPandas DataFrame for Hamilton processing
    return gdf


def standardized_dataset_table(
    h3_spatial_index_assigned: gpd.GeoDataFrame,
    dataset_name: str,
    grid_resolution: int = 8
) -> gpd.GeoDataFrame:
    """Apply final standardization with COALESCE field mapping and proper null handling."""
    gdf = h3_spatial_index_assigned.copy()
    available_columns = gdf.columns
    h3_column_name = f"h3_index_{grid_resolution}"

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

if __name__ == "__main__":
    import argparse
    import __main__ as this_module

    parser = argparse.ArgumentParser(description="Run stg_doi_pv_consolidation across all datasets.")
    parser.add_argument("--execution-mode", choices=["sequential", "parallel"], default="sequential", help="Execution mode: sequential or parallel (dynamic)")
    parser.add_argument("--datasets", help="Comma-separated list of dataset names to process (optional)")
    parser.add_argument("--no-cache", action="store_true", help="Disable Hamilton cache (cache enabled by default)")
    parser.add_argument("--export-path", nargs="?", const="db/geoparquet/stg_pv_consolidated.parquet", help="If provided, export consolidated data to this path (or default if omitted)")
    parser.add_argument("--grid-resolution", type=int, default=8, help="Spatial grid resolution (e.g., H3)")
    args = parser.parse_args()

    # Build Driver using the Builder pattern (docs-aligned)
    cfg = {
        "calculate_geometry_stats": True,
        "assign_h3_index": True,
    }
    exec_mode = args.execution_mode

    dr = build_driver(
        [this_module],
        config={**cfg, "execution_mode": exec_mode},
        enable_parallel=(exec_mode == "parallel"),
        enable_cache=(not args.no_cache),
    )

    # Compute dataset list for logging and optional filtering
    datasets_override = None
    if args.datasets:
        datasets_override = [s.strip() for s in args.datasets.split(",") if s.strip()]
    ds_result = dr.execute(["dataset_list"], inputs={"dataset_list_override": datasets_override} if datasets_override else None)
    ds_list = ds_result.get("dataset_list", [])
    if not ds_list:
        print("No datasets available to process.")
        raise SystemExit(0)

    # Branch on execution mode
    if exec_mode == "parallel":
        print(f"Processing {len(ds_list)} datasets in parallel via dynamic execution...")
        outputs = ["export_consolidated_geoparquet"] if args.export_path else ["consolidated_standardized_table"]
        inputs = {"grid_resolution": args.grid_resolution}
        if args.export_path:
            inputs["export_path"] = args.export_path
        if datasets_override:
            inputs["dataset_list_override"] = datasets_override
        result = dr.execute(outputs, inputs=inputs)
        if not args.export_path:
            combined = result["consolidated_standardized_table"]
            print(f"\n✅ Consolidation complete: {len(combined)} rows, {len(combined.columns)} columns")
            print("(Tip) Use --export-path to write a GeoParquet under db/geoparquet/")
    else:
        print(f"Processing {len(ds_list)} datasets sequentially...")
        standardized_parts = []
        for ds_name in ds_list:
            print(f"\n=== Dataset: {ds_name} ===")
            # Build a fresh Driver per dataset to mirror dbt behavior and avoid cross-iteration state
            dr_ds = build_driver(
                [this_module],
                config={**cfg, "execution_mode": exec_mode},
                enable_parallel=False,
                local_executor=executors.SynchronousLocalTaskExecutor(),
            )
            result = dr_ds.execute(
                ["standardized_dataset_table"],
                inputs={
                    "dataset_name": ds_name,
                    "grid_resolution": args.grid_resolution,
                },
            )
            gdf = result["standardized_dataset_table"]
            if isinstance(gdf, gpd.GeoDataFrame) and len(gdf) > 0:
                standardized_parts.append(gdf)
            else:
                print(f"   ⚠️ No records produced for {ds_name}")

        if not standardized_parts:
            print("No standardized data produced across datasets.")
            raise SystemExit(0)

        combined = gpd.GeoDataFrame(
            pd.concat(standardized_parts, ignore_index=True),
            geometry="geometry",
            crs=DEFAULT_CRS,
        )

        print(f"\n✅ Consolidation complete: {len(combined)} rows, {len(combined.columns)} columns")

        if args.export_path:
            out_path = Path(args.export_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            combined.to_parquet(out_path, compression="snappy")
            print(f"💾 Exported consolidated GeoParquet to: {out_path}")
        else:
            print("(Tip) Use --export-path to write a GeoParquet under db/geoparquet/")

