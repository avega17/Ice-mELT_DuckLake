"""
Hamilton dataflow for unified Overture Maps integration.

Phase 3: Overture Maps Integration
Task 3.6: Unified Overture Integration Module

Objective: Combine all Overture Maps themes with H3-indexed PV data into comprehensive spatial dataset
Key Functions:
- overture_unified_context() - Combine admin, building, land use, and land cover contexts
- pv_comprehensive_analysis() - Perform comprehensive spatial analysis across all themes
- pv_overture_unified_dataset() - Create final unified dataset with all Overture Maps context
Output: Comprehensive PV dataset with administrative, building, land use, and land cover context

References:
- Overture Maps Documentation: https://docs.overturemaps.org/
- H3 DuckDB extension: https://github.com/isaacbrodsky/h3-duckdb
- Ibis geospatial: https://ibis-project.org/reference/expression-geospatial
- geoarrow-rs: https://geoarrow.org/geoarrow-rs/python/v0.4.0-beta.3/
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional
import json

import geopandas as gpd
import pandas as pd
import ibis
import ibis.expr.types as ir
import duckdb

from hamilton.function_modifiers import cache, tag, config

# Import storage helpers
from ._doi_pv_helpers_storage import _geoarrow_table, _duckdb_table_from_geoarrow


@tag(stage="overture_integration", data_type="unified_context")
@cache(behavior="disable")
def overture_unified_context(
    pv_with_admin_context: ir.Table,
    pv_with_building_context: ir.Table,
    pv_with_land_use_context: ir.Table,
    pv_with_land_cover_context: ir.Table
) -> gpd.GeoDataFrame:
    """
    Combine all Overture Maps contexts into unified dataset.
    
    Merges administrative boundaries, building footprints, land use, and land cover
    contexts for each PV installation to create comprehensive spatial dataset.
    
    Args:
        pv_with_admin_context: PV locations with administrative context
        pv_with_building_context: PV locations with building context
        pv_with_land_use_context: PV locations with land use context
        pv_with_land_cover_context: PV locations with land cover context
        
    Returns:
        GeoDataFrame with comprehensive Overture Maps context for each PV installation
    """
    print(f"   🌐 Combining all Overture Maps contexts into unified dataset...")
    
    # Convert all Ibis tables to DataFrames
    admin_df = pv_with_admin_context.to_pandas()
    building_df = pv_with_building_context.to_pandas()
    land_use_df = pv_with_land_use_context.to_pandas()
    land_cover_df = pv_with_land_cover_context.to_pandas()
    
    print(f"      - Admin context: {len(admin_df)} records")
    print(f"      - Building context: {len(building_df)} records")
    print(f"      - Land use context: {len(land_use_df)} records")
    print(f"      - Land cover context: {len(land_cover_df)} records")
    
    # Start with admin context as base (should have all original PV records)
    if admin_df.empty:
        print(f"   ⚠️  No admin context available, cannot create unified dataset")
        return gpd.GeoDataFrame()
    
    # Reconstruct geometry from WKB if needed
    base_df = _reconstruct_geometry(admin_df)
    base_gdf = gpd.GeoDataFrame(base_df, geometry='geometry', crs='EPSG:4326')
    
    print(f"      - Base dataset: {len(base_gdf)} PV installations")
    
    # Merge building context
    if not building_df.empty:
        unified_gdf = _merge_building_context(base_gdf, building_df)
    else:
        unified_gdf = _add_empty_building_columns(base_gdf)
    
    # Merge land use context
    if not land_use_df.empty:
        unified_gdf = _merge_land_use_context(unified_gdf, land_use_df)
    else:
        unified_gdf = _add_empty_land_use_columns(unified_gdf)
    
    # Merge land cover context
    if not land_cover_df.empty:
        unified_gdf = _merge_land_cover_context(unified_gdf, land_cover_df)
    else:
        unified_gdf = _add_empty_land_cover_columns(unified_gdf)
    
    print(f"   ✅ Unified context creation complete: {len(unified_gdf)} PV installations")
    
    return unified_gdf


@tag(stage="overture_integration", data_type="comprehensive_analysis")
@cache(behavior="disable")
def pv_comprehensive_analysis(
    overture_unified_context: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Perform comprehensive spatial analysis across all Overture Maps themes.
    
    Creates derived metrics and classifications that combine insights from
    administrative, building, land use, and land cover contexts.
    
    Args:
        overture_unified_context: Unified dataset with all Overture Maps contexts
        
    Returns:
        GeoDataFrame with comprehensive analysis metrics and classifications
    """
    print(f"   📊 Performing comprehensive analysis across all Overture Maps themes...")
    
    if overture_unified_context.empty:
        print(f"   ⚠️  No unified context available for analysis")
        return gpd.GeoDataFrame()
    
    result_gdf = overture_unified_context.copy()
    
    # Site classification based on all contexts
    result_gdf['site_classification'] = result_gdf.apply(_classify_pv_site_type, axis=1)
    
    # Solar potential assessment
    result_gdf['solar_potential_score'] = result_gdf.apply(_calculate_solar_potential_score, axis=1)
    
    # Environmental sensitivity assessment
    result_gdf['environmental_sensitivity'] = result_gdf.apply(_assess_environmental_sensitivity, axis=1)
    
    # Development feasibility assessment
    result_gdf['development_feasibility'] = result_gdf.apply(_assess_development_feasibility, axis=1)
    
    # Context completeness score
    result_gdf['context_completeness'] = result_gdf.apply(_calculate_context_completeness, axis=1)
    
    # Add summary flags
    result_gdf['is_rooftop_installation'] = result_gdf['building_relationship'].isin(['rooftop_confirmed', 'rooftop_likely'])
    result_gdf['is_ground_mount'] = ~result_gdf['is_rooftop_installation']
    result_gdf['has_complete_context'] = result_gdf['context_completeness'] >= 0.75
    
    print(f"   ✅ Comprehensive analysis complete")
    
    # Print analysis summary
    _print_analysis_summary(result_gdf)
    
    return result_gdf


def _reconstruct_geometry(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reconstruct geometry from WKB if needed.
    
    Ensures geometry column contains proper Shapely geometry objects.
    """
    result_df = df.copy()
    
    if 'geometry' in result_df.columns and result_df['geometry'].dtype == 'object':
        if not result_df.empty and not hasattr(result_df['geometry'].iloc[0], 'geom_type'):
            from shapely import wkb
            result_df['geometry'] = result_df['geometry'].apply(lambda x: wkb.loads(x) if x is not None else None)
    
    return result_df


def _merge_building_context(base_gdf: gpd.GeoDataFrame, building_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Merge building context with base dataset.
    
    Adds building-related columns while preserving base dataset structure.
    """
    print(f"      - Merging building context")
    
    # Reconstruct geometry for building data
    building_df = _reconstruct_geometry(building_df)
    
    # Select building-specific columns
    building_columns = [col for col in building_df.columns if 'building' in col.lower() or col in [
        'height', 'num_floors', 'roof_material', 'roof_shape', 'potential_rooftop', 'confirmed_rooftop'
    ]]
    
    # Add geometry column for merging
    if 'geometry' not in building_columns:
        building_columns.append('geometry')
    
    building_subset = building_df[building_columns]
    
    # Merge on geometry (spatial join)
    try:
        building_gdf = gpd.GeoDataFrame(building_subset, geometry='geometry', crs='EPSG:4326')
        merged = gpd.sjoin(base_gdf, building_gdf, how='left', predicate='intersects')
        
        # Remove duplicate geometry columns
        if 'geometry_right' in merged.columns:
            merged = merged.drop(columns=['geometry_right'])
        
        print(f"      - Building context merged: {merged['building_id'].notna().sum()} PV with building data")
        
        return merged
        
    except Exception as e:
        print(f"      - Building context merge failed: {e}, adding empty columns")
        return _add_empty_building_columns(base_gdf)


def _merge_land_use_context(base_gdf: gpd.GeoDataFrame, land_use_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Merge land use context with base dataset.
    
    Adds land use-related columns while preserving base dataset structure.
    """
    print(f"      - Merging land use context")
    
    # Select land use-specific columns
    land_use_columns = [col for col in land_use_df.columns if 'land_use' in col.lower()]
    
    # Add index for merging (assuming same order)
    if len(land_use_df) == len(base_gdf):
        for col in land_use_columns:
            if col in land_use_df.columns:
                base_gdf[col] = land_use_df[col].values
        
        print(f"      - Land use context merged: {base_gdf['has_land_use_context'].sum()} PV with land use data")
    else:
        print(f"      - Land use context size mismatch, adding empty columns")
        base_gdf = _add_empty_land_use_columns(base_gdf)
    
    return base_gdf


def _merge_land_cover_context(base_gdf: gpd.GeoDataFrame, land_cover_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Merge land cover context with base dataset.
    
    Adds land cover-related columns while preserving base dataset structure.
    """
    print(f"      - Merging land cover context")
    
    # Select land cover-specific columns
    land_cover_columns = [col for col in land_cover_df.columns if 'land_cover' in col.lower() or col in [
        'environmental_impact', 'ecosystem_type'
    ]]
    
    # Add index for merging (assuming same order)
    if len(land_cover_df) == len(base_gdf):
        for col in land_cover_columns:
            if col in land_cover_df.columns:
                base_gdf[col] = land_cover_df[col].values
        
        print(f"      - Land cover context merged: {base_gdf['has_land_cover_context'].sum()} PV with land cover data")
    else:
        print(f"      - Land cover context size mismatch, adding empty columns")
        base_gdf = _add_empty_land_cover_columns(base_gdf)
    
    return base_gdf


def _add_empty_building_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add empty building context columns."""
    building_columns = {
        'building_id': None, 'building_subtype': None, 'building_class': None,
        'height': 0, 'num_floors': 0, 'roof_material': 'Unknown', 'roof_shape': 'Unknown',
        'potential_rooftop': False, 'confirmed_rooftop': False, 'building_relationship': 'no_buildings_available'
    }
    
    for col, default in building_columns.items():
        gdf[col] = default
    
    return gdf


def _add_empty_land_use_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add empty land use context columns."""
    land_use_columns = {
        'land_use_id': None, 'land_use_type': 'unknown', 'land_use_class': 'unknown',
        'has_land_use_context': False, 'land_use_suitability': 'unknown'
    }
    
    for col, default in land_use_columns.items():
        gdf[col] = default
    
    return gdf


def _add_empty_land_cover_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add empty land cover context columns."""
    land_cover_columns = {
        'land_cover_id': None, 'land_cover_type': 'unknown',
        'has_land_cover_context': False, 'environmental_impact': 'unknown', 'ecosystem_type': 'unknown'
    }

    for col, default in land_cover_columns.items():
        gdf[col] = default

    return gdf


def _classify_pv_site_type(row) -> str:
    """
    Classify PV site type based on all available contexts.

    Combines building, land use, and land cover information for comprehensive classification.
    """
    # Check building relationship first
    building_rel = row.get('building_relationship', 'unknown')
    if building_rel in ['rooftop_confirmed', 'rooftop_likely']:
        return 'rooftop'

    # Check land use for ground-mount classification
    land_use = row.get('land_use_type', '').lower()
    land_cover = row.get('land_cover_type', '').lower()

    if land_use == 'industrial':
        return 'industrial_ground_mount'
    elif land_use == 'commercial':
        return 'commercial_ground_mount'
    elif land_use == 'agricultural':
        return 'agrivoltaic'
    elif land_cover == 'built_up':
        return 'urban_ground_mount'
    elif land_cover in ['grass', 'bare']:
        return 'utility_scale'
    else:
        return 'ground_mount_other'


def _calculate_solar_potential_score(row) -> float:
    """
    Calculate solar potential score based on site characteristics.

    Combines multiple factors to assess solar development potential (0-1 scale).
    """
    score = 0.5  # Base score

    # Building context factors
    building_rel = row.get('building_relationship', 'unknown')
    if building_rel == 'rooftop_confirmed':
        score += 0.2
    elif building_rel == 'rooftop_likely':
        score += 0.1

    # Land use suitability
    land_use_suit = row.get('land_use_suitability', 'unknown')
    if land_use_suit == 'highly_suitable':
        score += 0.2
    elif land_use_suit == 'suitable':
        score += 0.1
    elif land_use_suit == 'moderately_suitable':
        score += 0.05

    # Environmental impact (lower impact = higher potential)
    env_impact = row.get('environmental_impact', 'unknown')
    if env_impact == 'minimal_impact':
        score += 0.1
    elif env_impact == 'moderate_impact':
        score += 0.05
    elif env_impact == 'high_impact':
        score -= 0.1

    # Administrative context (developed areas generally better)
    admin_level = row.get('admin_level', 'unknown')
    if admin_level in ['locality', 'county']:
        score += 0.05

    return min(max(score, 0.0), 1.0)  # Clamp to 0-1 range


def _assess_environmental_sensitivity(row) -> str:
    """
    Assess environmental sensitivity based on land cover and ecosystem type.

    Provides guidance for environmental impact assessment.
    """
    ecosystem = row.get('ecosystem_type', 'unknown')
    env_impact = row.get('environmental_impact', 'unknown')

    if ecosystem == 'forest' or env_impact == 'high_impact':
        return 'high_sensitivity'
    elif ecosystem in ['aquatic', 'polar'] or env_impact == 'special_consideration':
        return 'special_consideration'
    elif ecosystem in ['grassland', 'agricultural'] or env_impact == 'moderate_impact':
        return 'moderate_sensitivity'
    elif ecosystem in ['urban', 'barren'] or env_impact == 'minimal_impact':
        return 'low_sensitivity'
    else:
        return 'unknown_sensitivity'


def _assess_development_feasibility(row) -> str:
    """
    Assess development feasibility based on multiple factors.

    Considers administrative, land use, and environmental factors.
    """
    # Start with land use suitability
    land_use_suit = row.get('land_use_suitability', 'unknown')
    env_sensitivity = row.get('environmental_sensitivity', 'unknown')

    if land_use_suit == 'highly_suitable' and env_sensitivity == 'low_sensitivity':
        return 'highly_feasible'
    elif land_use_suit in ['suitable', 'highly_suitable'] and env_sensitivity in ['low_sensitivity', 'moderate_sensitivity']:
        return 'feasible'
    elif land_use_suit == 'moderately_suitable' or env_sensitivity == 'moderate_sensitivity':
        return 'moderately_feasible'
    elif env_sensitivity in ['high_sensitivity', 'special_consideration']:
        return 'challenging'
    else:
        return 'unknown_feasibility'


def _calculate_context_completeness(row) -> float:
    """
    Calculate completeness of context data (0-1 scale).

    Measures how much Overture Maps context is available for the PV installation.
    """
    context_flags = [
        row.get('has_admin_context', False),
        row.get('has_building_context', False),
        row.get('has_land_use_context', False),
        row.get('has_land_cover_context', False)
    ]

    return sum(context_flags) / len(context_flags)


def _print_analysis_summary(gdf: gpd.GeoDataFrame):
    """Print comprehensive analysis summary."""
    print(f"      - Site classifications: {gdf['site_classification'].value_counts().to_dict()}")
    print(f"      - Solar potential (avg): {gdf['solar_potential_score'].mean():.2f}")
    print(f"      - Environmental sensitivity: {gdf['environmental_sensitivity'].value_counts().to_dict()}")
    print(f"      - Development feasibility: {gdf['development_feasibility'].value_counts().to_dict()}")
    print(f"      - Context completeness (avg): {gdf['context_completeness'].mean():.2f}")
    print(f"      - Rooftop installations: {gdf['is_rooftop_installation'].sum()}")
    print(f"      - Ground-mount installations: {gdf['is_ground_mount'].sum()}")


@tag(stage="overture_integration", data_type="table")
@cache(behavior="disable")
def pv_overture_unified_dataset(
    pv_comprehensive_analysis: gpd.GeoDataFrame,
    database_path: str = "db/eo_pv_data.duckdb",
    export_geoparquet: bool = True
) -> ir.Table:
    """
    Create final unified dataset with all Overture Maps context and convert to Ibis table.

    Finalizes the comprehensive PV dataset with all Overture Maps themes integrated
    and converts to Arrow/Ibis format for efficient downstream processing.

    Args:
        pv_comprehensive_analysis: Comprehensive analysis results
        database_path: Path to DuckDB database for storage
        export_geoparquet: Whether to export to GeoParquet format

    Returns:
        Ibis Table with comprehensive PV and Overture Maps dataset
    """
    print(f"   🎯 Creating final unified PV-Overture Maps dataset...")

    if pv_comprehensive_analysis.empty:
        print(f"   ⚠️  No data to process, returning empty table")
        return _create_empty_unified_table()

    # Final data cleaning and standardization
    result_gdf = _finalize_unified_dataset(pv_comprehensive_analysis)

    # Convert to Arrow table using geoarrow-rs
    arrow_table = _geoarrow_table(result_gdf, "pv_overture_unified")

    # Store in DuckDB if database path provided
    if database_path:
        conn = duckdb.connect(database_path)
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("INSTALL h3 FROM community; LOAD h3;")

        table_name = "prepared.pv_overture_unified"
        row_count = _duckdb_table_from_geoarrow(conn, arrow_table, table_name)

        print(f"   ✅ Stored {row_count} records in {table_name}")
        conn.close()

    # Convert to Ibis table for downstream processing
    ibis_table = _convert_to_ibis_table(arrow_table, "pv_overture_unified")

    print(f"   ✅ Phase 3: Overture Maps Integration COMPLETE")
    print(f"      - Final dataset: {len(result_gdf)} PV installations")
    print(f"      - Complete context: {result_gdf['has_complete_context'].sum()} installations")
    print(f"      - Ready for dbt analytical models")

    return ibis_table


def _finalize_unified_dataset(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Final data cleaning and standardization for unified dataset.

    Ensures all fields are properly typed and formatted for downstream use.
    """
    print(f"      - Finalizing unified dataset")

    result_gdf = gdf.copy()

    # Remove any remaining duplicate index columns
    index_cols = [col for col in result_gdf.columns if col.startswith('index_')]
    if index_cols:
        result_gdf = result_gdf.drop(columns=index_cols)

    # Ensure numeric columns are properly typed
    numeric_columns = ['height', 'num_floors', 'solar_potential_score', 'context_completeness']
    for col in numeric_columns:
        if col in result_gdf.columns:
            result_gdf[col] = pd.to_numeric(result_gdf[col], errors='coerce').fillna(0)

    # Ensure boolean columns are properly typed
    boolean_columns = [
        'has_admin_context', 'has_building_context', 'has_land_use_context',
        'has_land_cover_context', 'potential_rooftop', 'confirmed_rooftop',
        'is_rooftop_installation', 'is_ground_mount', 'has_complete_context'
    ]
    for col in boolean_columns:
        if col in result_gdf.columns:
            result_gdf[col] = result_gdf[col].astype(bool)

    print(f"      - Dataset finalized: {len(result_gdf)} records")

    return result_gdf


def _convert_to_ibis_table(arrow_table, dataset_name: str) -> ir.Table:
    """Convert Arrow table to Ibis table for downstream processing."""
    print(f"      - Converting to Ibis table with DuckDB backend")

    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("INSTALL h3 FROM community; LOAD h3;")

    ibis_conn = ibis.duckdb.connect(conn)
    table_name = f"temp_{dataset_name}"
    ibis_table = ibis_conn.register(arrow_table, table_name)

    print(f"      - Ibis table created: {len(ibis_table.execute())} rows")

    return ibis_table


def _create_empty_unified_table() -> ir.Table:
    """Create empty Ibis table with expected unified schema."""
    import pyarrow as pa

    schema = pa.schema([
        pa.field("geometry", pa.binary()),
        pa.field("site_classification", pa.string()),
        pa.field("solar_potential_score", pa.float64()),
        pa.field("environmental_sensitivity", pa.string()),
        pa.field("development_feasibility", pa.string()),
        pa.field("context_completeness", pa.float64()),
        pa.field("is_rooftop_installation", pa.bool_()),
        pa.field("is_ground_mount", pa.bool_()),
        pa.field("has_complete_context", pa.bool_())
    ])

    empty_table = pa.table([], schema=schema)

    conn = duckdb.connect()
    ibis_conn = ibis.duckdb.connect(conn)
    return ibis_conn.register(empty_table, "empty_unified_table")
