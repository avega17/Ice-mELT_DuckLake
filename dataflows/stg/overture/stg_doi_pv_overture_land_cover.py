"""
Hamilton dataflow for Overture Maps land cover integration.

Phase 3: Overture Maps Integration
Task 3.5: Land Cover Integration Module

Objective: Integrate Overture Maps land cover features with PV installations for surface context analysis
Key Functions:
- overture_land_cover_features() - Load Overture Maps land_cover features using overturemaestro
- pv_land_cover_spatial_join() - Spatially join PV locations with land cover polygons
- pv_with_land_cover_context() - Enrich PV data with land cover context for surface analysis
Output: PV installations with land cover context for natural surface characterization

References:
- Overture Maps Land Cover Schema: https://docs.overturemaps.org/schema/reference/base/land_cover/
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
def overture_land_cover_features__parallel(
    country_iso_codes: str,  # Individual country from parallel processing
    collected_pv_country_aggregates: ir.Table,
    land_cover_subtypes: List[str] = ["urban", "crop", "forest", "grass", "barren"],
    overture_release: Optional[str] = None
) -> gpd.GeoDataFrame:
    """
    Load Overture Maps land cover features for a single country (parallel processing).

    Uses country aggregates to get bbox for the specific country and loads
    land cover features using overturemaestro's convert_bounding_box_to_geodataframe function.

    Args:
        country_iso_codes: Individual country ISO code from parallel processing
        collected_pv_country_aggregates: Country aggregates with bboxes from admin module
        land_cover_subtypes: List of land cover subtypes to include (from schema enum)
        overture_release: Specific Overture Maps release version (None = latest)

    Returns:
        GeoDataFrame with land cover features for the specific country
    """
    print(f"   🌿 Loading Overture Maps land cover features for {country_iso_codes}...")
    print(f"      - Land cover subtypes: {land_cover_subtypes}")

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

    # Load land cover for this country using bbox filtering
    land_cover_gdfs = []

    try:
        # Use overturemaestro to get land cover for country bbox
        for subtype in land_cover_subtypes:
            land_cover_gdf = om.functions.convert_bounding_box_to_geodataframe(
                bbox=bbox,
                theme="base",
                type="land_cover",
                overture_maps_release=overture_release,
                pyarrow_filters=[
                    ("subtype", "=", subtype)
                ]
            )

            if not land_cover_gdf.empty:
                # Add country context for later filtering
                land_cover_gdf['source_country_iso'] = country_iso_codes
                land_cover_gdfs.append(land_cover_gdf)
                print(f"         - Loaded {len(land_cover_gdf)} {subtype} land cover features")

    except Exception as e:
        print(f"         - Error loading land cover for {country_iso_codes}: {e}")
        return gpd.GeoDataFrame()

    if not land_cover_gdfs:
        print(f"   ⚠️  No land cover found for {country_iso_codes}")
        return gpd.GeoDataFrame()

    # Combine all land cover data for this country
    combined_gdf = gpd.pd.concat(land_cover_gdfs, ignore_index=True)

    # Standardize column names to match schema
    combined_gdf = _standardize_land_cover_columns(combined_gdf)

    print(f"   ✅ Loaded {len(combined_gdf)} land cover features for {country_iso_codes}")
    print(f"      - Land cover subtypes: {combined_gdf['land_cover_subtype'].value_counts().to_dict()}")

    return combined_gdf


@tag(stage="overture_integration", data_type="vector", execution_mode="sequential")
@cache(behavior="disable")
@config.when(execution_mode="sequential")
def overture_land_cover_features__sequential(
    country_iso_codes: List[str],  # List of countries for sequential processing
    collected_pv_country_aggregates: ir.Table,
    land_cover_subtypes: List[str] = ["urban", "crop", "forest", "grass", "barren"],
    overture_release: Optional[str] = None
) -> gpd.GeoDataFrame:
    """
    Load Overture Maps land cover features for all countries (sequential processing).

    Processes all countries sequentially and combines results.

    Args:
        country_iso_codes: List of country ISO codes for sequential processing
        collected_pv_country_aggregates: Country aggregates with bboxes from admin module
        land_cover_subtypes: List of land cover subtypes to include (from schema enum)
        overture_release: Specific Overture Maps release version (None = latest)

    Returns:
        GeoDataFrame with land cover features for all countries
    """
    print(f"   🌿 Loading Overture Maps land cover features for {len(country_iso_codes)} countries sequentially...")
    print(f"      - Land cover subtypes: {land_cover_subtypes}")

    # Get country aggregates for bbox filtering
    country_df = collected_pv_country_aggregates.to_pandas()

    if country_df.empty:
        print(f"   ⚠️  No country aggregates available")
        return gpd.GeoDataFrame()

    # Load land cover for each country
    all_land_cover_gdfs = []

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

        # Load land cover for this country
        land_cover_gdfs = []

        try:
            for subtype in land_cover_subtypes:
                land_cover_gdf = om.functions.convert_bounding_box_to_geodataframe(
                    bbox=bbox,
                    theme="base",
                    type="land_cover",
                    overture_maps_release=overture_release,
                    pyarrow_filters=[
                        ("subtype", "=", subtype)
                    ]
                )

                if not land_cover_gdf.empty:
                    land_cover_gdf['source_country_iso'] = country_iso
                    land_cover_gdfs.append(land_cover_gdf)
                    print(f"         - Loaded {len(land_cover_gdf)} {subtype} land cover features")

        except Exception as e:
            print(f"         - Error loading land cover for {country_iso}: {e}")
            continue

        if land_cover_gdfs:
            country_combined = gpd.pd.concat(land_cover_gdfs, ignore_index=True)
            all_land_cover_gdfs.append(country_combined)

    if not all_land_cover_gdfs:
        print(f"   ⚠️  No land cover found for any countries")
        return gpd.GeoDataFrame()

    # Combine all country land cover data
    combined_gdf = gpd.pd.concat(all_land_cover_gdfs, ignore_index=True)

    # Standardize column names to match schema
    combined_gdf = _standardize_land_cover_columns(combined_gdf)

    print(f"   ✅ Loaded {len(combined_gdf)} land cover features")
    print(f"      - Land cover subtypes: {combined_gdf['land_cover_subtype'].value_counts().to_dict()}")
    print(f"      - Countries: {combined_gdf['source_country_iso'].nunique()}")

    return combined_gdf


@tag(stage="overture_integration", data_type="vector_list")
@cache(behavior="disable")
@config.when(execution_mode="parallel")
def collected_land_cover_features__parallel(overture_land_cover_features: Collect[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """Collect all land cover features from parallel processing."""
    land_cover_gdfs = list(overture_land_cover_features)

    # Filter out empty dataframes
    non_empty_gdfs = [gdf for gdf in land_cover_gdfs if not gdf.empty]

    if not non_empty_gdfs:
        print(f"   ⚠️  No land cover features collected from parallel processing")
        return gpd.GeoDataFrame()

    # Combine all land cover data
    combined_gdf = gpd.pd.concat(non_empty_gdfs, ignore_index=True)

    print(f"   ✅ Collected {len(combined_gdf)} land cover features from {len(non_empty_gdfs)} countries")
    print(f"      - Land cover subtypes: {combined_gdf['land_cover_subtype'].value_counts().to_dict()}")
    print(f"      - Countries: {combined_gdf['source_country_iso'].nunique()}")

    return combined_gdf


@tag(stage="overture_integration", data_type="vector_list")
@cache(behavior="disable")
@config.when(execution_mode="sequential")
def collected_land_cover_features__sequential(overture_land_cover_features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Pass through land cover features from sequential processing."""
    return overture_land_cover_features


def _standardize_land_cover_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardize land cover column names to match expected schema.

    Maps Overture Maps land cover schema fields to consistent naming.
    Uses only actual schema fields, no hallucinated ones.
    """
    # Map Overture schema fields to our expected names
    column_mapping = {
        'subtype': 'land_cover_subtype'
    }
    # Note: global 'id' is ignored, cartography field kept as-is

    result_gdf = gdf.copy()

    # Rename columns if they exist
    for old_name, new_name in column_mapping.items():
        if old_name in result_gdf.columns:
            result_gdf = result_gdf.rename(columns={old_name: new_name})

    # Ensure cartography field is properly handled
    if 'cartography' not in result_gdf.columns:
        result_gdf['cartography'] = None

    return result_gdf


@tag(stage="overture_integration", data_type="spatial_join")
@cache(behavior="disable")
def pv_land_cover_spatial_join(
    pv_with_land_use_context: ir.Table,
    collected_land_cover_features: gpd.GeoDataFrame,
    spatial_method: str = "intersects"
) -> gpd.GeoDataFrame:
    """
    Spatially join PV installations with land cover features.

    Determines the land cover context for each PV installation to understand
    natural surface characteristics.

    Args:
        pv_with_land_use_context: PV locations with land use context
        collected_land_cover_features: Land cover polygons from Overture Maps (collected)
        spatial_method: Method for spatial join ("intersects", "within")

    Returns:
        GeoDataFrame with PV locations enriched with land cover data
    """
    print(f"   🌍 Performing spatial join: PV locations ↔ Land cover features")
    print(f"      - Spatial method: {spatial_method}")

    if collected_land_cover_features.empty:
        print(f"   ⚠️  No land cover features available for analysis")
        pv_df = pv_with_land_use_context.to_pandas()
        pv_gdf = _reconstruct_geometry_from_wkb(pv_df)
        return _add_empty_land_cover_context(pv_gdf)

    # Convert PV Ibis table to GeoDataFrame
    pv_df = pv_with_land_use_context.to_pandas()
    pv_gdf = _reconstruct_geometry_from_wkb(pv_df)

    print(f"      - PV installations: {len(pv_gdf)} features")
    print(f"      - Land cover features: {len(collected_land_cover_features)} features")

    # Filter land cover by country for efficiency
    if 'country_iso' in pv_gdf.columns and 'source_country_iso' in collected_land_cover_features.columns:
        pv_countries = set(pv_gdf['country_iso'].dropna())
        land_cover_countries = set(collected_land_cover_features['source_country_iso'].dropna())
        common_countries = pv_countries.intersection(land_cover_countries)

        if common_countries:
            filtered_land_cover = collected_land_cover_features[
                collected_land_cover_features['source_country_iso'].isin(common_countries)
            ]
            print(f"      - Filtered to {len(filtered_land_cover)} land cover features in {len(common_countries)} countries")
        else:
            filtered_land_cover = collected_land_cover_features
    else:
        filtered_land_cover = collected_land_cover_features

    # Perform spatial join
    if spatial_method == "intersects":
        joined = gpd.sjoin(pv_gdf, filtered_land_cover, how='left', predicate='intersects')
    elif spatial_method == "within":
        joined = gpd.sjoin(pv_gdf, filtered_land_cover, how='left', predicate='within')
    else:
        print(f"      - Unknown spatial method: {spatial_method}, using 'intersects'")
        joined = gpd.sjoin(pv_gdf, filtered_land_cover, how='left', predicate='intersects')

    # Handle multiple land cover matches per PV installation
    if len(joined) > len(pv_gdf):
        print(f"      - Multiple land cover matches found, selecting primary land cover per PV installation")
        joined = _resolve_multiple_land_cover_matches(joined)

    # Add land cover context flag
    joined['has_land_cover_context'] = joined['land_cover_subtype'].notna()

    print(f"   ✅ Spatial join complete: {len(joined)} PV installations with land cover context")
    print(f"      - PV with land cover data: {joined['has_land_cover_context'].sum()}")

    return joined


def _reconstruct_geometry_from_wkb(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Reconstruct geometry from WKB if needed."""
    result_df = df.copy()

    if 'geometry' in result_df.columns and result_df['geometry'].dtype == 'object':
        if not result_df.empty and not hasattr(result_df['geometry'].iloc[0], 'geom_type'):
            from shapely import wkb
            result_df['geometry'] = result_df['geometry'].apply(lambda x: wkb.loads(x) if x is not None else None)

    return gpd.GeoDataFrame(result_df, geometry='geometry', crs='EPSG:4326')


def _resolve_multiple_land_cover_matches(joined_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Resolve cases where PV installations intersect multiple land cover polygons.
    
    Selects the most relevant land cover based on area overlap and environmental priority.
    """
    print(f"      - Resolving multiple land cover matches")
    
    # Group by original PV installation index
    grouped = joined_gdf.groupby(joined_gdf.index)
    
    resolved_records = []
    
    for pv_idx, group in grouped:
        if len(group) == 1:
            # Single match - keep as is
            resolved_records.append(group.iloc[0])
        else:
            # Multiple matches - select best match
            best_match = _select_best_land_cover_match(group)
            resolved_records.append(best_match)
    
    result_gdf = gpd.GeoDataFrame(resolved_records, crs=joined_gdf.crs)
    
    print(f"      - Resolved to {len(result_gdf)} unique PV installations")
    
    return result_gdf


def _select_best_land_cover_match(matches: gpd.GeoDataFrame) -> pd.Series:
    """
    Select the best land cover match from multiple candidates.

    Selects the land cover feature with the largest area.
    """
    # Select largest land cover by area
    matches_copy = matches.copy()
    matches_copy['land_cover_area'] = matches_copy.geometry.area
    best_match = matches_copy.loc[matches_copy['land_cover_area'].idxmax()]

    return best_match





def _add_empty_land_cover_context(pv_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add empty land cover context columns when no land cover data is available.

    Maintains consistent schema for downstream processing.
    """
    result_gdf = pv_gdf.copy()

    # Add empty land cover context columns (only actual schema fields)
    land_cover_context_columns = {
        'land_cover_subtype': None,
        'cartography': None,
        'source_country_iso': None,
        'has_land_cover_context': False
    }

    for col, default_value in land_cover_context_columns.items():
        result_gdf[col] = default_value

    return result_gdf


@tag(stage="overture_integration", data_type="table")
@cache(behavior="disable")
def pv_with_land_cover_context(
    pv_land_cover_spatial_join: gpd.GeoDataFrame,
    database_path: str = "db/eo_pv_data.duckdb",
    export_geoparquet: bool = False
) -> ir.Table:
    """
    Finalize PV locations with land cover context and convert to Ibis table.

    Cleans up spatial join results, standardizes land cover fields, and converts to
    Arrow/Ibis format for efficient downstream processing.

    Args:
        pv_land_cover_spatial_join: PV data with land cover spatial join
        database_path: Path to DuckDB database for storage
        export_geoparquet: Whether to export to GeoParquet format

    Returns:
        Ibis Table with PV locations enriched with land cover context
    """
    print(f"   🌲 Finalizing PV locations with land cover context...")

    if pv_land_cover_spatial_join.empty:
        print(f"   ⚠️  No data to process, returning empty table")
        return _create_empty_land_cover_table()

    # Clean up and standardize land cover fields
    result_gdf = _standardize_land_cover_fields(pv_land_cover_spatial_join)

    # Convert to Arrow table using geoarrow-rs
    arrow_table = _geoarrow_table(result_gdf, "pv_with_land_cover_context")

    # Store in DuckDB and create Ibis connection using connection API
    if database_path:
        conn = ibis.duckdb.connect(database_path)
        conn.raw_sql("INSTALL spatial; LOAD spatial;")
        conn.raw_sql("INSTALL h3 FROM community; LOAD h3;")

        table_name = "raw_data.pv_with_land_cover_context"
        row_count = _duckdb_table_from_geoarrow(conn._backend.con, arrow_table, table_name)

        print(f"   ✅ Stored {row_count} records in {table_name}")

        ibis_table = conn.register(arrow_table, "pv_with_land_cover_context")
    else:
        conn = ibis.duckdb.connect()
        conn.raw_sql("INSTALL spatial; LOAD spatial;")
        conn.raw_sql("INSTALL h3 FROM community; LOAD h3;")

        ibis_table = conn.register(arrow_table, "pv_with_land_cover_context")

    print(f"   ✅ Land cover context integration complete")
    print(f"      - Records with land cover data: {len(result_gdf)}")

    # Print land cover summary
    land_cover_counts = result_gdf['land_cover_subtype'].value_counts()
    print(f"      - Land cover subtypes: {land_cover_counts.to_dict()}")

    return ibis_table


def _standardize_land_cover_fields(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardize land cover field names and handle missing values.

    Ensures consistent field naming and handles cases where spatial join
    didn't find matching land cover features.
    """
    print(f"      - Standardizing land cover fields")

    result_gdf = gdf.copy()

    # Handle missing land cover data (only actual schema fields)
    land_cover_columns = [
        'land_cover_subtype', 'cartography'
    ]

    for col in land_cover_columns:
        if col in result_gdf.columns:
            # String columns - preserve nulls
            result_gdf[col] = result_gdf[col].fillna(None)

    # Ensure boolean columns are properly typed
    boolean_columns = ['has_land_cover_context']

    for col in boolean_columns:
        if col in result_gdf.columns:
            result_gdf[col] = result_gdf[col].fillna(False).astype(bool)



    # Remove duplicate index columns from spatial joins
    index_cols = [col for col in result_gdf.columns if col.startswith('index_')]
    if index_cols:
        result_gdf = result_gdf.drop(columns=index_cols)

    # Add land cover analysis summary
    context_count = result_gdf['has_land_cover_context'].sum()

    print(f"      - Standardization complete: {context_count} records with land cover context")

    return result_gdf





def _create_empty_land_cover_table() -> ir.Table:
    """
    Create empty Ibis table with expected land cover schema.

    Used when no data is available to maintain consistent return types.
    """
    import pyarrow as pa

    schema = pa.schema([
        pa.field("geometry", pa.binary()),
        pa.field("land_cover_subtype", pa.string()),
        pa.field("cartography", pa.string()),
        pa.field("source_country_iso", pa.string()),
        pa.field("has_land_cover_context", pa.bool_())
    ])

    empty_table = pa.table([], schema=schema)

    # Convert to Ibis using connection API
    conn = ibis.duckdb.connect()
    conn.raw_sql("INSTALL spatial; LOAD spatial;")

    return conn.register(empty_table, "empty_land_cover_table")
