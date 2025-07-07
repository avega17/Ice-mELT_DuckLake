"""
Hamilton dataflow for Overture Maps building footprints integration.

Phase 3: Overture Maps Integration
Task 3.3: Building Footprints Module

Objective: Integrate Overture Maps building footprints with PV installations for rooftop analysis
Key Functions:
- overture_building_footprints() - Load Overture Maps building features using overturemaestro
- pv_building_spatial_analysis() - Analyze spatial relationships between PV and buildings
- pv_with_building_context() - Enrich PV data with building context for rooftop analysis
Output: PV installations with building footprint context for rooftop solar potential analysis

References:
- Overture Maps Buildings Schema: https://docs.overturemaps.org/schema/reference/buildings/building/
- OvertureMaestro: https://kraina-ai.github.io/overturemaestro/latest/api/OvertureMaestro/basic/
- H3 DuckDB extension: https://github.com/isaacbrodsky/h3-duckdb
- Ibis connection API: https://ibis-project.org/reference/connection
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional
import json

import geopandas as gpd
import pandas as pd
import ibis
import ibis.expr.types as ir
import overturemaestro as om

from hamilton.function_modifiers import cache, tag, config
from hamilton.htypes import Parallelizable, Collect

# Import storage helpers
from .doi_pv._doi_pv_helpers_storage import _geoarrow_table, _duckdb_table_from_geoarrow


@tag(stage="overture_integration", data_type="vector", execution_mode="parallel")
@cache(behavior="disable")
@config.when(execution_mode="parallel")
def overture_building_footprints__parallel(
    country_iso_codes: str,  # Individual country from parallel processing
    collected_pv_country_aggregates: ir.Table,
    building_subtypes: List[str] = ["residential", "commercial", "industrial"],
    overture_release: Optional[str] = None
) -> gpd.GeoDataFrame:
    """
    Load Overture Maps building footprints for a single country (parallel processing).

    Uses country aggregates to get bbox for the specific country and loads
    building footprints using overturemaestro's convert_bounding_box_to_geodataframe function.

    Args:
        country_iso_codes: Individual country ISO code from parallel processing
        collected_pv_country_aggregates: Country aggregates with bboxes from admin module
        building_subtypes: List of building subtypes to include
        overture_release: Specific Overture Maps release version (None = latest)

    Returns:
        GeoDataFrame with building footprints for the specific country
    """
    print(f"   🏢 Loading Overture Maps building footprints for {country_iso_codes}...")
    print(f"      - Building subtypes: {building_subtypes}")

    # Get country aggregates for bbox filtering
    country_df = collected_pv_country_aggregates.to_pandas()

    # Filter to specific country
    country_row = country_df[country_df['country_iso'] == country_iso_codes]

    if country_row.empty:
        print(f"   ⚠️  No country aggregate found for {country_iso_codes}")
        return gpd.GeoDataFrame()

    country_data = country_row.iloc[0]
    bbox = (
        country_data['bbox_xmin'],
        country_data['bbox_ymin'],
        country_data['bbox_xmax'],
        country_data['bbox_ymax']
    )

    # Load buildings for this country using bbox filtering
    building_gdfs = []

    try:
        # Use overturemaestro to get buildings for country bbox
        for subtype in building_subtypes:
            buildings_gdf = om.functions.convert_bounding_box_to_geodataframe(
                bbox=bbox,
                theme="buildings",
                type="building",
                overture_maps_release=overture_release,
                pyarrow_filters=[
                    ("subtype", "=", subtype)
                ]
            )

            if not buildings_gdf.empty:
                # Add country context for later filtering
                buildings_gdf['source_country_iso'] = country_iso_codes
                building_gdfs.append(buildings_gdf)
                print(f"         - Loaded {len(buildings_gdf)} {subtype} buildings")

    except Exception as e:
        print(f"         - Error loading buildings for {country_iso_codes}: {e}")
        return gpd.GeoDataFrame()

    if not building_gdfs:
        print(f"   ⚠️  No buildings found for {country_iso_codes}")
        return gpd.GeoDataFrame()

    # Combine all building data for this country
    combined_gdf = gpd.pd.concat(building_gdfs, ignore_index=True)

    # Standardize column names to match schema
    combined_gdf = _standardize_building_columns(combined_gdf)

    print(f"   ✅ Loaded {len(combined_gdf)} building footprints for {country_iso_codes}")
    print(f"      - Building subtypes: {combined_gdf['building_subtype'].value_counts().to_dict()}")

    return combined_gdf


@tag(stage="overture_integration", data_type="vector", execution_mode="sequential")
@cache(behavior="disable")
@config.when(execution_mode="sequential")
def overture_building_footprints__sequential(
    country_iso_codes: List[str],  # List of countries for sequential processing
    collected_pv_country_aggregates: ir.Table,
    building_subtypes: List[str] = ["residential", "commercial", "industrial"],
    overture_release: Optional[str] = None
) -> gpd.GeoDataFrame:
    """
    Load Overture Maps building footprints for all countries (sequential processing).

    Processes all countries sequentially and combines results.

    Args:
        country_iso_codes: List of country ISO codes for sequential processing
        collected_pv_country_aggregates: Country aggregates with bboxes from admin module
        building_subtypes: List of building subtypes to include
        overture_release: Specific Overture Maps release version (None = latest)

    Returns:
        GeoDataFrame with building footprints for all countries
    """
    print(f"   🏢 Loading Overture Maps building footprints for {len(country_iso_codes)} countries sequentially...")
    print(f"      - Building subtypes: {building_subtypes}")

    # Get country aggregates for bbox filtering
    country_df = collected_pv_country_aggregates.to_pandas()

    if country_df.empty:
        print(f"   ⚠️  No country aggregates available")
        return gpd.GeoDataFrame()

    # Load buildings for each country
    all_building_gdfs = []

    for country_iso in country_iso_codes:
        print(f"      - Processing {country_iso}...")

        # Filter to specific country
        country_row = country_df[country_df['country_iso'] == country_iso]

        if country_row.empty:
            print(f"         - No country aggregate found for {country_iso}")
            continue

        country_data = country_row.iloc[0]
        bbox = (
            country_data['bbox_xmin'],
            country_data['bbox_ymin'],
            country_data['bbox_xmax'],
            country_data['bbox_ymax']
        )

        # Load buildings for this country
        building_gdfs = []

        try:
            for subtype in building_subtypes:
                buildings_gdf = om.functions.convert_bounding_box_to_geodataframe(
                    bbox=bbox,
                    theme="buildings",
                    type="building",
                    overture_maps_release=overture_release,
                    pyarrow_filters=[
                        ("subtype", "=", subtype)
                    ]
                )

                if not buildings_gdf.empty:
                    buildings_gdf['source_country_iso'] = country_iso
                    building_gdfs.append(buildings_gdf)
                    print(f"         - Loaded {len(buildings_gdf)} {subtype} buildings")

        except Exception as e:
            print(f"         - Error loading buildings for {country_iso}: {e}")
            continue

        if building_gdfs:
            country_combined = gpd.pd.concat(building_gdfs, ignore_index=True)
            all_building_gdfs.append(country_combined)

    if not all_building_gdfs:
        print(f"   ⚠️  No buildings found for any countries")
        return gpd.GeoDataFrame()

    # Combine all country building data
    combined_gdf = gpd.pd.concat(all_building_gdfs, ignore_index=True)

    # Standardize column names to match schema
    combined_gdf = _standardize_building_columns(combined_gdf)

    print(f"   ✅ Loaded {len(combined_gdf)} building footprints")
    print(f"      - Building subtypes: {combined_gdf['building_subtype'].value_counts().to_dict()}")
    print(f"      - Countries: {combined_gdf['source_country_iso'].nunique()}")

    return combined_gdf


@tag(stage="overture_integration", data_type="vector_list")
@cache(behavior="disable")
@config.when(execution_mode="parallel")
def collected_building_footprints__parallel(overture_building_footprints: Collect[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """Collect all building footprints from parallel processing."""
    building_gdfs = list(overture_building_footprints)

    # Filter out empty dataframes
    non_empty_gdfs = [gdf for gdf in building_gdfs if not gdf.empty]

    if not non_empty_gdfs:
        print(f"   ⚠️  No building footprints collected from parallel processing")
        return gpd.GeoDataFrame()

    # Combine all building data
    combined_gdf = gpd.pd.concat(non_empty_gdfs, ignore_index=True)

    print(f"   ✅ Collected {len(combined_gdf)} building footprints from {len(non_empty_gdfs)} countries")
    print(f"      - Building subtypes: {combined_gdf['building_subtype'].value_counts().to_dict()}")
    print(f"      - Countries: {combined_gdf['source_country_iso'].nunique()}")

    return combined_gdf


@tag(stage="overture_integration", data_type="vector_list")
@cache(behavior="disable")
@config.when(execution_mode="sequential")
def collected_building_footprints__sequential(overture_building_footprints: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Pass through building footprints from sequential processing."""
    return overture_building_footprints


def _standardize_building_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardize building column names to match expected schema.

    Maps Overture Maps building schema fields to consistent naming.
    Uses only actual schema fields, no hallucinated ones.
    """
    # Map Overture schema fields to our expected names
    column_mapping = {
        'subtype': 'building_subtype',
        'class': 'building_class'
    }
    # Note: global 'id' is ignored, we don't need building IDs for our analysis

    result_gdf = gdf.copy()

    # Rename columns if they exist
    for old_name, new_name in column_mapping.items():
        if old_name in result_gdf.columns:
            result_gdf = result_gdf.rename(columns={old_name: new_name})

    # Extract primary name from names object if it exists
    if 'names' in result_gdf.columns:
        result_gdf['building_name'] = result_gdf['names'].apply(
            lambda x: x.get('primary', None) if isinstance(x, dict) else None
        )
    else:
        result_gdf['building_name'] = None

    # Ensure numeric fields are properly typed
    numeric_fields = ['height', 'num_floors', 'num_floors_underground']
    for field in numeric_fields:
        if field in result_gdf.columns:
            result_gdf[field] = pd.to_numeric(result_gdf[field], errors='coerce')

    # Ensure boolean fields are properly typed
    if 'has_parts' in result_gdf.columns:
        result_gdf['has_parts'] = result_gdf['has_parts'].astype(bool)

    return result_gdf


@tag(stage="overture_integration", data_type="spatial_analysis")
@cache(behavior="disable")
def pv_building_spatial_analysis(
    pv_with_admin_context: ir.Table,
    collected_building_footprints: gpd.GeoDataFrame,
    spatial_method: str = "intersects"
) -> gpd.GeoDataFrame:
    """
    Analyze spatial relationships between PV installations and building footprints.

    Performs spatial join to identify PV installations that intersect with buildings.
    Filters buildings by country to improve efficiency.

    Args:
        pv_with_admin_context: PV locations with administrative context
        collected_building_footprints: Building footprints from Overture Maps (collected)
        spatial_method: Method for spatial analysis ("intersects", "within")

    Returns:
        GeoDataFrame with PV-building spatial analysis results
    """
    print(f"   🔍 Analyzing PV-building spatial relationships...")
    print(f"      - Spatial method: {spatial_method}")

    if collected_building_footprints.empty:
        print(f"   ⚠️  No buildings available for analysis")
        pv_df = pv_with_admin_context.to_pandas()
        pv_gdf = _reconstruct_geometry_from_wkb(pv_df)
        return _add_empty_building_context(pv_gdf)

    # Convert PV Ibis table to GeoDataFrame
    pv_df = pv_with_admin_context.to_pandas()
    pv_gdf = _reconstruct_geometry_from_wkb(pv_df)

    print(f"      - PV installations: {len(pv_gdf)} features")
    print(f"      - Building footprints: {len(collected_building_footprints)} features")

    # Filter buildings by country for efficiency
    if 'country_iso' in pv_gdf.columns and 'source_country_iso' in collected_building_footprints.columns:
        pv_countries = set(pv_gdf['country_iso'].dropna())
        building_countries = set(collected_building_footprints['source_country_iso'].dropna())
        common_countries = pv_countries.intersection(building_countries)

        if common_countries:
            filtered_buildings = collected_building_footprints[
                collected_building_footprints['source_country_iso'].isin(common_countries)
            ]
            print(f"      - Filtered to {len(filtered_buildings)} buildings in {len(common_countries)} countries")
        else:
            filtered_buildings = collected_building_footprints
    else:
        filtered_buildings = collected_building_footprints

    # Perform spatial join
    if spatial_method == "intersects":
        joined = gpd.sjoin(pv_gdf, filtered_buildings, how='left', predicate='intersects')
    elif spatial_method == "within":
        joined = gpd.sjoin(pv_gdf, filtered_buildings, how='left', predicate='within')
    else:
        print(f"      - Unknown spatial method: {spatial_method}, using 'intersects'")
        joined = gpd.sjoin(pv_gdf, filtered_buildings, how='left', predicate='intersects')

    # Handle multiple building matches per PV installation
    if len(joined) > len(pv_gdf):
        print(f"      - Multiple building matches found, selecting primary building per PV installation")
        joined = _resolve_multiple_building_matches(joined)

    # Add building context flag
    joined['has_building_context'] = joined['building_subtype'].notna()

    print(f"   ✅ Spatial analysis complete: {len(joined)} PV installations analyzed")
    print(f"      - PV with building data: {joined['has_building_context'].sum()}")

    return joined


def _reconstruct_geometry_from_wkb(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Reconstruct geometry from WKB if needed."""
    result_df = df.copy()

    if 'geometry' in result_df.columns and result_df['geometry'].dtype == 'object':
        if not result_df.empty and not hasattr(result_df['geometry'].iloc[0], 'geom_type'):
            from shapely import wkb
            result_df['geometry'] = result_df['geometry'].apply(lambda x: wkb.loads(x) if x is not None else None)

    return gpd.GeoDataFrame(result_df, geometry='geometry', crs='EPSG:4326')


def _resolve_multiple_building_matches(joined_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Resolve cases where PV installations intersect multiple buildings.

    Selects the building with the largest area overlap or highest building.
    """
    print(f"      - Resolving multiple building matches")

    # Group by original PV installation index
    grouped = joined_gdf.groupby(joined_gdf.index)

    resolved_records = []

    for pv_idx, group in grouped:
        if len(group) == 1:
            # Single match - keep as is
            resolved_records.append(group.iloc[0])
        else:
            # Multiple matches - select best match
            best_match = _select_best_building_match(group)
            resolved_records.append(best_match)

    result_gdf = gpd.GeoDataFrame(resolved_records, crs=joined_gdf.crs)

    print(f"      - Resolved to {len(result_gdf)} unique PV installations")

    return result_gdf


def _select_best_building_match(matches: gpd.GeoDataFrame) -> pd.Series:
    """
    Select the best building match from multiple candidates.

    Prioritizes buildings with height data, then largest buildings.
    """
    # Prefer buildings with height data
    with_height = matches[matches['height'].notna()]

    if not with_height.empty:
        # Select tallest building
        best_match = with_height.loc[with_height['height'].idxmax()]
    else:
        # Fallback to largest building by area
        matches_copy = matches.copy()
        matches_copy['building_area'] = matches_copy.geometry.area
        best_match = matches_copy.loc[matches_copy['building_area'].idxmax()]

    return best_match


def _add_empty_building_context(pv_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add empty building context columns when no buildings are available.

    Uses only actual schema fields, no hallucinated ones.
    """
    result_gdf = pv_gdf.copy()

    # Add empty building context columns (only actual schema fields)
    building_context_columns = {
        'building_subtype': None,
        'building_class': None,
        'building_name': None,
        'height': None,
        'num_floors': None,
        'num_floors_underground': None,
        'has_parts': None,
        'source_country_iso': None,
        'has_building_context': False
    }

    for col, default_value in building_context_columns.items():
        result_gdf[col] = default_value

    return result_gdf


@tag(stage="overture_integration", data_type="table")
@cache(behavior="disable")
def pv_with_building_context(
    pv_building_spatial_analysis: gpd.GeoDataFrame,
    database_path: str = "db/eo_pv_data.duckdb",
    export_geoparquet: bool = False
) -> ir.Table:
    """
    Finalize PV locations with building context and convert to Ibis table.

    Cleans up spatial analysis results, standardizes building fields, and converts to
    Arrow/Ibis format for efficient downstream processing using Ibis connection API.

    Args:
        pv_building_spatial_analysis: PV data with building spatial analysis
        database_path: Path to DuckDB database for storage
        export_geoparquet: Whether to export to GeoParquet format

    Returns:
        Ibis Table with PV locations enriched with building context
    """
    print(f"   🏗️ Finalizing PV locations with building context...")

    if pv_building_spatial_analysis.empty:
        print(f"   ⚠️  No data to process, returning empty table")
        return _create_empty_building_table()

    # Clean up and standardize building fields
    result_gdf = _standardize_building_fields(pv_building_spatial_analysis)

    # Convert to Arrow table using geoarrow-rs
    arrow_table = _geoarrow_table(result_gdf, "pv_with_building_context")

    # Store in DuckDB and create Ibis connection using connection API
    if database_path:
        conn = ibis.duckdb.connect(database_path)
        conn.raw_sql("INSTALL spatial; LOAD spatial;")
        conn.raw_sql("INSTALL h3 FROM community; LOAD h3;")

        table_name = "raw_data.pv_with_building_context"
        row_count = _duckdb_table_from_geoarrow(conn._backend.con, arrow_table, table_name)

        print(f"   ✅ Stored {row_count} records in {table_name}")

        ibis_table = conn.register(arrow_table, "pv_with_building_context")
    else:
        conn = ibis.duckdb.connect()
        conn.raw_sql("INSTALL spatial; LOAD spatial;")
        conn.raw_sql("INSTALL h3 FROM community; LOAD h3;")

        ibis_table = conn.register(arrow_table, "pv_with_building_context")

    print(f"   ✅ Building context integration complete")
    print(f"      - Records with building data: {len(result_gdf)}")
    print(f"      - Building subtypes: {result_gdf['building_subtype'].value_counts().to_dict()}")

    return ibis_table


def _standardize_building_fields(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardize building field names and handle missing values.

    Uses only actual Overture Maps building schema fields.
    """
    print(f"      - Standardizing building fields")

    result_gdf = gdf.copy()

    # Handle missing building data (only actual schema fields)
    building_columns = [
        'building_subtype', 'building_class', 'building_name',
        'height', 'num_floors', 'num_floors_underground'
    ]

    for col in building_columns:
        if col in result_gdf.columns:
            if col in ['height', 'num_floors', 'num_floors_underground']:
                # Numeric columns - fill with None to preserve nulls
                result_gdf[col] = pd.to_numeric(result_gdf[col], errors='coerce')
            else:
                # String columns - fill with None to preserve nulls
                result_gdf[col] = result_gdf[col].fillna(None)

    # Ensure boolean columns are properly typed
    boolean_columns = ['has_parts', 'has_building_context']

    for col in boolean_columns:
        if col in result_gdf.columns:
            result_gdf[col] = result_gdf[col].fillna(False).astype(bool)

    # Remove duplicate index columns from spatial joins
    index_cols = [col for col in result_gdf.columns if col.startswith('index_')]
    if index_cols:
        result_gdf = result_gdf.drop(columns=index_cols)

    # Add building analysis summary
    building_count = result_gdf['has_building_context'].sum()

    print(f"      - Standardization complete: {building_count} PV installations with building context")

    return result_gdf


def _create_empty_building_table() -> ir.Table:
    """
    Create empty Ibis table with expected building schema.

    Uses only actual Overture Maps building schema fields.
    """
    import pyarrow as pa

    schema = pa.schema([
        pa.field("geometry", pa.binary()),
        pa.field("building_subtype", pa.string()),
        pa.field("building_class", pa.string()),
        pa.field("building_name", pa.string()),
        pa.field("height", pa.float64()),
        pa.field("num_floors", pa.int64()),
        pa.field("num_floors_underground", pa.int64()),
        pa.field("has_parts", pa.bool_()),
        pa.field("source_country_iso", pa.string()),
        pa.field("has_building_context", pa.bool_())
    ])

    empty_table = pa.table([], schema=schema)

    # Convert to Ibis using connection API
    conn = ibis.duckdb.connect()
    conn.raw_sql("INSTALL spatial; LOAD spatial;")

    return conn.register(empty_table, "empty_building_table")
