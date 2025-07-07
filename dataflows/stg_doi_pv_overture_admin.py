"""
Hamilton dataflow for Overture Maps administrative boundaries integration.

Phase 3: Overture Maps Integration
Task 3.2: Administrative Boundaries Module

Objective: Integrate Overture Maps administrative boundaries with H3-indexed PV locations
Key Functions:
- overture_admin_boundaries() - Load Overture Maps division_area features using overturemaestro
- pv_admin_spatial_join() - Spatially join PV locations with admin boundaries
- pv_with_admin_context() - Enrich PV data with administrative context
Output: H3-indexed PV locations with administrative boundary assignments

References:
- Overture Maps Divisions Schema: https://docs.overturemaps.org/schema/reference/divisions/division_area/
- OvertureMaestro: https://kraina-ai.github.io/overturemaestro/latest/examples/basic_usage/
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
from ._doi_pv_helpers_storage import _geoarrow_table, _duckdb_table_from_geoarrow


@tag(stage="overture_integration", data_type="metadata")
@cache(behavior="disable")
def stac_manifest_overture(stac_manifest_path: str = "data_loaders/stac_manifest.json") -> Dict[str, Any]:
    """
    Load Overture Maps configuration from STAC manifest.
    
    Args:
        stac_manifest_path: Path to STAC manifest JSON file
        
    Returns:
        Overture Maps configuration dictionary
    """
    with open(stac_manifest_path, 'r') as f:
        manifest = json.load(f)
    
    overture_config = manifest.get("overture_maps", {})
    print(f"   📋 Loaded Overture Maps config: {overture_config.get('display_name', 'Unknown')}")
    print(f"      - Latest release: {overture_config.get('latest_release', 'Unknown')}")
    print(f"      - Total features: {overture_config.get('total_features', 'Unknown')}")
    
    return overture_config


@tag(stage="overture_integration", data_type="vector")
@cache(behavior="disable")
def overture_admin_boundaries(
    admin_levels: List[str] = ["dependency", "country", "region", "county", "localadmin"],
    overture_release: Optional[str] = None
) -> gpd.GeoDataFrame:
    """
    Load Overture Maps administrative boundaries (division_area features) globally.

    Loads all division_area features globally since we have a global PV dataset.
    This will be used for spatial join to assign countries to PV installations.

    Args:
        admin_levels: List of administrative levels to include
        overture_release: Specific Overture Maps release version (None = latest)

    Returns:
        GeoDataFrame with global administrative boundary polygons
    """
    print(f"   🌍 Loading global Overture Maps administrative boundaries...")
    print(f"      - Admin levels: {admin_levels}")
    print(f"      - Loading globally for spatial join with PV locations")

    # Use overturemaestro to load division_area features globally
    try:
        # Load data for each admin level globally
        admin_gdfs = []

        for admin_level in admin_levels:
            print(f"      - Loading global {admin_level} boundaries...")

            # Use overturemaestro functions to get global data
            gdf = om.functions.convert_bounding_box_to_geodataframe(
                bbox=(-180, -90, 180, 90),  # Global bbox
                theme="divisions",
                type="division_area",
                overture_maps_release=overture_release,
                pyarrow_filters=[
                    ("subtype", "=", admin_level),
                    ("is_land", "=", True)
                ]
            )

            if not gdf.empty:
                admin_gdfs.append(gdf)
                print(f"         - Loaded {len(gdf)} global {admin_level} boundaries")

        if not admin_gdfs:
            print(f"   ⚠️  No administrative boundaries found for levels: {admin_levels}")
            return gpd.GeoDataFrame()

        # Combine all admin levels
        combined_gdf = gpd.pd.concat(admin_gdfs, ignore_index=True)

        # Standardize column names to match schema
        combined_gdf = _standardize_admin_columns(combined_gdf)

        print(f"   ✅ Loaded {len(combined_gdf)} global administrative boundaries")
        print(f"      - Admin levels: {combined_gdf['admin_level'].value_counts().to_dict()}")
        print(f"      - Countries: {combined_gdf['country_iso'].nunique()} unique")

        return combined_gdf

    except Exception as e:
        print(f"   ⚠️  Error loading admin boundaries with overturemaestro: {e}")
        print(f"      - Falling back to empty GeoDataFrame")
        return gpd.GeoDataFrame()


def _standardize_admin_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardize administrative boundary column names to match expected schema.

    Maps Overture Maps division_area schema fields to consistent naming.
    """
    # Map Overture schema fields to our expected names
    column_mapping = {
        'country': 'country_iso',
        'region': 'region_iso',
        'subtype': 'admin_level',
        'class': 'admin_class'
    }
    # Note: division_id is already correctly named, global 'id' is ignored

    result_gdf = gdf.copy()

    # Rename columns if they exist
    for old_name, new_name in column_mapping.items():
        if old_name in result_gdf.columns:
            result_gdf = result_gdf.rename(columns={old_name: new_name})

    # Extract primary name from names object if it exists
    if 'names' in result_gdf.columns:
        result_gdf['admin_name'] = result_gdf['names'].apply(
            lambda x: x.get('primary', 'Unknown') if isinstance(x, dict) else 'Unknown'
        )
    else:
        result_gdf['admin_name'] = 'Unknown'

    return result_gdf


@tag(stage="overture_integration", data_type="spatial_join")
@cache(behavior="disable")
def pv_admin_spatial_join(
    pv_h3_grid_cells: ir.Table,
    overture_admin_boundaries: gpd.GeoDataFrame,
    spatial_method: str = "intersects"
) -> gpd.GeoDataFrame:
    """
    Spatially join H3-indexed PV locations with administrative boundaries.

    Uses efficient spatial joins to add administrative context to PV locations.
    Preserves original PV geometries while adding administrative data.

    Args:
        pv_h3_grid_cells: H3-indexed PV locations from previous stage
        overture_admin_boundaries: Administrative boundary polygons
        spatial_method: Method for spatial join ("intersects", "within")

    Returns:
        GeoDataFrame with PV locations enriched with administrative data
    """
    print(f"   🔗 Performing spatial join: PV locations ↔ Admin boundaries")
    print(f"      - Spatial method: {spatial_method}")

    if overture_admin_boundaries.empty:
        print(f"   ⚠️  No admin boundaries available, returning original PV data")
        pv_df = pv_h3_grid_cells.to_pandas()
        pv_gdf = _reconstruct_geometry_from_wkb(pv_df)
        return _add_empty_admin_context(pv_gdf)

    # Convert Ibis table to GeoDataFrame for spatial operations
    pv_df = pv_h3_grid_cells.to_pandas()
    pv_gdf = _reconstruct_geometry_from_wkb(pv_df)

    print(f"      - PV locations: {len(pv_gdf)} features")
    print(f"      - Admin boundaries: {len(overture_admin_boundaries)} features")

    # Perform spatial join
    if spatial_method == "intersects":
        joined = gpd.sjoin(pv_gdf, overture_admin_boundaries, how='left', predicate='intersects')
    elif spatial_method == "within":
        joined = gpd.sjoin(pv_gdf, overture_admin_boundaries, how='left', predicate='within')
    else:
        print(f"      - Unknown spatial method: {spatial_method}, using 'intersects'")
        joined = gpd.sjoin(pv_gdf, overture_admin_boundaries, how='left', predicate='intersects')

    # Handle multiple admin matches per PV installation
    if len(joined) > len(pv_gdf):
        print(f"      - Multiple admin matches found, selecting primary admin per PV installation")
        joined = _resolve_multiple_admin_matches(joined)

    # Add admin context flag
    joined['has_admin_context'] = joined['division_id'].notna()

    print(f"   ✅ Spatial join complete: {len(joined)} features with admin context")
    print(f"      - PV with admin data: {joined['has_admin_context'].sum()}")

    return joined


def _reconstruct_geometry_from_wkb(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Reconstruct geometry from WKB if needed."""
    result_df = df.copy()

    if 'geometry' in result_df.columns and result_df['geometry'].dtype == 'object':
        if not result_df.empty and not hasattr(result_df['geometry'].iloc[0], 'geom_type'):
            from shapely import wkb
            result_df['geometry'] = result_df['geometry'].apply(lambda x: wkb.loads(x) if x is not None else None)

    return gpd.GeoDataFrame(result_df, geometry='geometry', crs='EPSG:4326')


def _resolve_multiple_admin_matches(joined_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Resolve cases where PV installations intersect multiple admin boundaries.

    Selects the most specific administrative level (locality > county > region > country).
    """
    print(f"      - Resolving multiple admin matches")

    # Define admin level priority (higher = more specific)
    admin_priority = {
        'localadmin': 5,
        'county': 4,
        'region': 3,
        'country': 2,
        'dependency': 1,
        'unknown': 0
    }

    # Group by original PV installation index
    grouped = joined_gdf.groupby(joined_gdf.index)

    resolved_records = []

    for pv_idx, group in grouped:
        if len(group) == 1:
            # Single match - keep as is
            resolved_records.append(group.iloc[0])
        else:
            # Multiple matches - select most specific admin level
            group_copy = group.copy()
            group_copy['priority'] = group_copy['admin_level'].map(
                lambda x: admin_priority.get(x.lower() if x else 'unknown', 0)
            )

            best_match = group_copy.loc[group_copy['priority'].idxmax()]
            resolved_records.append(best_match)

    result_gdf = gpd.GeoDataFrame(resolved_records, crs=joined_gdf.crs)

    print(f"      - Resolved to {len(result_gdf)} unique PV installations")

    return result_gdf


def _add_empty_admin_context(pv_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add empty admin context columns when no admin data is available."""
    result_gdf = pv_gdf.copy()

    admin_context_columns = {
        'division_id': None,
        'country_iso': None,
        'region_iso': None,
        'admin_name': None,
        'admin_level': None,
        'admin_class': None,
        'has_admin_context': False
    }

    for col, default_value in admin_context_columns.items():
        result_gdf[col] = default_value

    return result_gdf


@tag(stage="overture_integration", data_type="table")
@cache(behavior="disable")
def pv_with_admin_context(
    pv_admin_spatial_join: gpd.GeoDataFrame,
    database_path: str = "db/eo_pv_data.duckdb",
    export_geoparquet: bool = False
) -> ir.Table:
    """
    Finalize PV locations with administrative context and convert to Ibis table.

    Cleans up spatial join results, standardizes admin fields, and converts to
    Arrow/Ibis format for efficient downstream processing using Ibis connection API.

    Args:
        pv_admin_spatial_join: Spatially joined PV and admin data
        database_path: Path to DuckDB database for storage
        export_geoparquet: Whether to export to GeoParquet format

    Returns:
        Ibis Table with PV locations enriched with administrative context
    """
    print(f"   🏛️ Finalizing PV locations with administrative context...")

    if pv_admin_spatial_join.empty:
        print(f"   ⚠️  No data to process, returning empty table")
        return _create_empty_admin_table()

    # Clean up and standardize administrative fields
    result_gdf = _standardize_admin_fields(pv_admin_spatial_join)

    # Convert to Arrow table using geoarrow-rs
    arrow_table = _geoarrow_table(result_gdf, "pv_with_admin_boundaries")

    # Store in DuckDB and create Ibis connection using connection API
    if database_path:
        # Create Ibis connection with DuckDB extensions
        conn = ibis.duckdb.connect(database_path)

        # Load required extensions using Ibis connection API
        conn.raw_sql("INSTALL spatial; LOAD spatial;")
        conn.raw_sql("INSTALL h3 FROM community; LOAD h3;")

        table_name = "raw_data.pv_with_admin_boundaries"
        row_count = _duckdb_table_from_geoarrow(conn._backend.con, arrow_table, table_name)

        print(f"   ✅ Stored {row_count} records in {table_name}")

        # Register Arrow table with Ibis for downstream processing
        ibis_table = conn.register(arrow_table, "pv_with_admin_context")

    else:
        # Create temporary Ibis connection
        conn = ibis.duckdb.connect()
        conn.raw_sql("INSTALL spatial; LOAD spatial;")
        conn.raw_sql("INSTALL h3 FROM community; LOAD h3;")

        ibis_table = conn.register(arrow_table, "pv_with_admin_context")

    print(f"   ✅ Administrative context integration complete")
    print(f"      - Records with admin data: {len(result_gdf)}")
    print(f"      - Countries represented: {result_gdf['country_iso'].nunique()}")

    return ibis_table


@tag(stage="overture_integration", data_type="list", execution_mode="parallel")
@config.when(execution_mode="parallel")
def country_iso_codes__parallel(collected_pv_country_aggregates: ir.Table) -> Parallelizable[str]:
    """Extract country ISO codes for parallel processing of other Overture themes."""
    country_df = collected_pv_country_aggregates.to_pandas()

    if country_df.empty:
        print("⚠️  No countries with PV installations found")
        return

    country_codes = country_df['country_iso'].dropna().unique().tolist()
    print(f"🌍 Processing {len(country_codes)} countries in parallel for Overture themes")

    for country_iso in country_codes:
        yield country_iso


@tag(stage="overture_integration", data_type="list", execution_mode="sequential")
@config.when(execution_mode="sequential")
def country_iso_codes__sequential(collected_pv_country_aggregates: ir.Table) -> List[str]:
    """Extract country ISO codes for sequential processing of other Overture themes."""
    country_df = collected_pv_country_aggregates.to_pandas()

    if country_df.empty:
        print("⚠️  No countries with PV installations found")
        return []

    country_codes = country_df['country_iso'].dropna().unique().tolist()
    print(f"🌍 Processing {len(country_codes)} countries sequentially for Overture themes")

    return country_codes


@tag(stage="overture_integration", data_type="aggregation")
@cache(behavior="disable")
def collected_pv_country_aggregates(
    pv_with_admin_context: ir.Table,
    database_path: str = "db/eo_pv_data.duckdb"
) -> ir.Table:
    """
    Create country-level aggregates for PV installations with division geometries and bbox.

    Aggregates PV installations by country to facilitate region-based searches for other
    Overture Maps themes using convert_bounding_box_to_geodataframe.

    Args:
        pv_with_admin_context: PV locations with administrative context
        database_path: Path to DuckDB database for storage

    Returns:
        Ibis Table with country-level aggregates including bbox and division geometries
    """
    print(f"   📊 Creating country-level PV aggregates...")

    # Convert to pandas for aggregation
    pv_df = pv_with_admin_context.to_pandas()

    if pv_df.empty:
        print(f"   ⚠️  No PV data available for aggregation")
        return _create_empty_country_aggregates_table()

    # Reconstruct geometry for spatial operations
    pv_gdf = _reconstruct_geometry_from_wkb(pv_df)

    # Group by country and calculate aggregates
    country_aggregates = []

    for country_iso, group in pv_gdf.groupby('country_iso'):
        if pd.isna(country_iso) or country_iso == 'Unknown':
            continue

        # Calculate spatial bounds for country
        bounds = group.total_bounds

        # Calculate aggregates
        pv_count = len(group)
        pv_area_sum = group['area_m2'].sum() if 'area_m2' in group.columns else 0

        # Get representative division geometry (largest by area if available)
        country_divisions = group[group['admin_level'] == 'country']
        if not country_divisions.empty:
            # Use the largest country division geometry
            largest_division = country_divisions.loc[country_divisions.geometry.area.idxmax()]
            division_geometry = largest_division.geometry
        else:
            # Create bbox polygon as fallback
            from shapely.geometry import box
            division_geometry = box(bounds[0], bounds[1], bounds[2], bounds[3])

        country_aggregates.append({
            'country_iso': country_iso,
            'pv_count': pv_count,
            'pv_area_sum_m2': pv_area_sum,
            'bbox_xmin': bounds[0],
            'bbox_ymin': bounds[1],
            'bbox_xmax': bounds[2],
            'bbox_ymax': bounds[3],
            'geometry': division_geometry
        })

    if not country_aggregates:
        print(f"   ⚠️  No valid country aggregates created")
        return _create_empty_country_aggregates_table()

    # Create GeoDataFrame
    country_gdf = gpd.GeoDataFrame(country_aggregates, crs='EPSG:4326')

    print(f"   ✅ Created country aggregates for {len(country_gdf)} countries")
    print(f"      - Total PV installations: {country_gdf['pv_count'].sum()}")
    print(f"      - Total PV area: {country_gdf['pv_area_sum_m2'].sum():.2f} m²")

    # Convert to Arrow table and store
    arrow_table = _geoarrow_table(country_gdf, "pv_country_aggregates")

    # Store in DuckDB and create Ibis connection
    if database_path:
        conn = ibis.duckdb.connect(database_path)
        conn.raw_sql("INSTALL spatial; LOAD spatial;")
        conn.raw_sql("INSTALL h3 FROM community; LOAD h3;")

        table_name = "prepared.pv_country_aggregates"
        row_count = _duckdb_table_from_geoarrow(conn._backend.con, arrow_table, table_name)

        print(f"   ✅ Stored {row_count} country aggregates in {table_name}")

        ibis_table = conn.register(arrow_table, "pv_country_aggregates")
    else:
        conn = ibis.duckdb.connect()
        conn.raw_sql("INSTALL spatial; LOAD spatial;")
        ibis_table = conn.register(arrow_table, "pv_country_aggregates")

    return ibis_table


def _create_empty_country_aggregates_table() -> ir.Table:
    """Create empty Ibis table with expected country aggregates schema."""
    import pyarrow as pa

    schema = pa.schema([
        pa.field("country_iso", pa.string()),
        pa.field("pv_count", pa.int64()),
        pa.field("pv_area_sum_m2", pa.float64()),
        pa.field("bbox_xmin", pa.float64()),
        pa.field("bbox_ymin", pa.float64()),
        pa.field("bbox_xmax", pa.float64()),
        pa.field("bbox_ymax", pa.float64()),
        pa.field("geometry", pa.binary())
    ])

    empty_table = pa.table([], schema=schema)

    conn = ibis.duckdb.connect()
    conn.raw_sql("INSTALL spatial; LOAD spatial;")

    return conn.register(empty_table, "empty_country_aggregates_table")


def _standardize_admin_fields(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardize administrative field names and handle missing values.

    Ensures consistent field naming and handles cases where spatial join
    didn't find matching administrative boundaries.
    """
    print(f"      - Standardizing administrative fields")

    result_gdf = gdf.copy()

    # Handle missing administrative data
    admin_columns = [
        'division_id', 'country_iso', 'region_iso', 'admin_name', 'admin_level', 'admin_class'
    ]

    for col in admin_columns:
        if col in result_gdf.columns:
            result_gdf[col] = result_gdf[col].fillna('Unknown')

    # Ensure boolean columns are properly typed
    if 'has_admin_context' in result_gdf.columns:
        result_gdf['has_admin_context'] = result_gdf['has_admin_context'].fillna(False).astype(bool)

    # Remove duplicate index columns from spatial join
    index_cols = [col for col in result_gdf.columns if col.startswith('index_')]
    if index_cols:
        result_gdf = result_gdf.drop(columns=index_cols)

    print(f"      - Standardization complete: {result_gdf['has_admin_context'].sum()} records with admin context")

    return result_gdf


def _create_empty_admin_table() -> ir.Table:
    """
    Create empty Ibis table with expected admin schema.

    Used when no data is available to maintain consistent return types.
    """
    import pyarrow as pa

    schema = pa.schema([
        pa.field("geometry", pa.binary()),
        pa.field("division_id", pa.string()),
        pa.field("country_iso", pa.string()),
        pa.field("region_iso", pa.string()),
        pa.field("admin_name", pa.string()),
        pa.field("admin_level", pa.string()),
        pa.field("admin_class", pa.string()),
        pa.field("has_admin_context", pa.bool_())
    ])

    empty_table = pa.table([], schema=schema)

    # Convert to Ibis using connection API
    conn = ibis.duckdb.connect()
    conn.raw_sql("INSTALL spatial; LOAD spatial;")

    return conn.register(empty_table, "empty_admin_table")
