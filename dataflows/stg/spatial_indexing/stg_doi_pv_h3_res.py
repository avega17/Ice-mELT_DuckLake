"""
Hamilton dataflow for H3 spatial indexing of PV locations.

Phase 2: H3 Spatial Indexing
Task 2.2: H3 Indexing Module

Objective: Apply H3 spatial indexing to PV locations
Key Functions:
- pv_h3_grid_cells() - Convert PV centroids to H3 cell IDs at calculated resolution
Output: H3-indexed PV locations with cell metadata

References:
- H3ronpy: https://github.com/nmandery/h3ronpy
- H3ronpy vector operations: https://h3ronpy.readthedocs.io/en/latest/usage/vector.html
- H3ronpy pandas API: https://h3ronpy.readthedocs.io/en/latest/api/pandas.html
- Ibis geospatial: https://ibis-project.org/reference/expression-geospatial
- Ibis + Hamilton: https://ibis-project.org/posts/hamilton-ibis/index.html#working-across-rows-with-user-defined-functions-udfs
- H3 DuckDB extension: https://github.com/isaacbrodsky/h3-duckdb
"""

from __future__ import annotations

from typing import Dict, Any, Optional

import geopandas as gpd
import pandas as pd
import ibis
import ibis.expr.types as ir

from hamilton.function_modifiers import cache, tag, config


# =============================================================================
# H3 INDEXING CONFIGURATION
# =============================================================================

# Default H3 resolution if sensor-specific calculation is not available
DEFAULT_H3_RESOLUTION = 10


# =============================================================================
# HAMILTON DATAFLOW NODES - H3 INDEXING
# =============================================================================

@tag(stage="h3_config", data_type="dict")
def h3_resolution_calculator(
    h3_resolution: int = DEFAULT_H3_RESOLUTION
) -> Dict[str, Dict[str, Any]]:
    """
    Create H3 resolution calculator configuration.

    Args:
        h3_resolution: H3 resolution to use (from config)

    Returns:
        Dictionary with H3 resolution configuration for different sensors/use cases
    """
    # Create a simple configuration that uses the provided H3 resolution
    config = {
        "sentinel-2-l2a": {
            "detection_model": h3_resolution,
            "classification_model": h3_resolution,
            "default": h3_resolution
        },
        "landsat-8": {
            "detection_model": h3_resolution,
            "classification_model": h3_resolution,
            "default": h3_resolution
        },
        "default": {
            "default": h3_resolution
        }
    }

    print(f"   🗺️  H3 resolution calculator configured with resolution {h3_resolution}")
    return config


@tag(stage="h3_indexing", data_type="table")
@cache(behavior="disable")  # Disable caching for large tables
def pv_h3_grid_cells(
    deduplicated_geodataframe: gpd.GeoDataFrame,
    h3_resolution_calculator: Dict[str, Dict[str, Any]],
    target_sensor: str = "sentinel-2-l2a",
    target_use_case: str = "detection_model"
) -> ir.Table:
    """
    Convert PV centroids to H3 cell IDs using h3ronpy and Ibis with DuckDB backend.
    
    Uses h3ronpy for vectorized H3 operations and Ibis for efficient columnar processing
    with DuckDB backend and H3 extension support.
    
    Args:
        deduplicated_geodataframe: Deduplicated PV locations from previous stage
        h3_resolution_calculator: H3 resolution calculations from h3_res module
        target_sensor: Sensor to use for H3 resolution (default: sentinel-2-l2a)
        target_use_case: Use case for H3 resolution (default: detection_model)
    
    Returns:
        Ibis Table with H3 index and resolution columns added
    """
    if deduplicated_geodataframe.empty:
        # Return empty Ibis table with expected schema
        return _create_empty_h3_table()
    
    gdf = deduplicated_geodataframe.copy()
    dataset_name = gdf['dataset'].iloc[0] if 'dataset' in gdf.columns else 'unknown'
    
    print(f"   🔷 Applying H3 indexing to {dataset_name}: {len(gdf)} records")
    
    # Get optimal H3 resolution for target sensor
    h3_resolution = _get_optimal_h3_resolution(
        h3_resolution_calculator, target_sensor, target_use_case
    )
    
    print(f"      - Using H3 resolution: {h3_resolution} (sensor: {target_sensor}, use case: {target_use_case})")
    
    # Apply vectorized H3 indexing using h3ronpy
    h3_indexed_gdf = _apply_h3_indexing_vectorized(gdf, h3_resolution)

    # Convert to Ibis table with DuckDB backend for efficient processing
    ibis_table = _convert_to_ibis_table(h3_indexed_gdf, dataset_name)
    
    print(f"   ✅ H3 indexing complete: {len(h3_indexed_gdf)} records indexed at resolution {h3_resolution}")
    
    return ibis_table


@tag(stage="h3_metadata", data_type="summary")
@cache(behavior="default", format="json")  # Cache H3 indexing summary as JSON
def h3_indexing_summary(
    pv_h3_grid_cells: ir.Table,
    h3_resolution_calculator: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Generate summary statistics for H3 indexing results using Ibis operations.
    
    Uses Ibis expressions for efficient aggregation and statistics computation
    on the H3-indexed table with DuckDB backend.
    
    Args:
        pv_locations_to_h3: Ibis Table with H3 indexing applied
        h3_resolution_calculator: H3 resolution calculations for context
    
    Returns:
        Dictionary with H3 indexing summary statistics
    """
    table = pv_h3_grid_cells
    
    # Use Ibis expressions for efficient aggregation
    try:
        # Basic statistics using Ibis aggregations
        basic_stats = table.aggregate([
            table.count().name('total_records'),
            table['h3_resolution'].max().name('h3_resolution'),
            table['h3_index'].nunique().name('unique_h3_cells'),
            table['dataset'].max().name('dataset_name')
        ]).execute()
        
        if basic_stats.empty or basic_stats['total_records'].iloc[0] == 0:
            return {'dataset_name': 'unknown', 'record_count': 0}
        
        stats_row = basic_stats.iloc[0]
        total_records = stats_row['total_records']
        h3_resolution = stats_row['h3_resolution']
        unique_h3_cells = stats_row['unique_h3_cells']
        dataset_name = stats_row['dataset_name']
        
        # H3 cell distribution using Ibis groupby
        cell_distribution = (
            table
            .group_by('h3_index')
            .aggregate(table.count().name('records_per_cell'))
            .aggregate([
                table.records_per_cell.mean().name('avg_records_per_cell'),
                table.records_per_cell.max().name('max_records_per_cell'),
                table.records_per_cell.min().name('min_records_per_cell')
            ])
            .execute()
        )
        
        # Coverage statistics using Ibis aggregations
        coverage_stats = table.aggregate([
            table['area_m2'].sum().name('total_area_m2'),
            table['area_m2'].mean().name('avg_installation_area_m2'),
            table['area_m2'].median().name('median_installation_area_m2')
        ]).execute()
        
        # Geographic extent using Ibis aggregations
        geo_stats = table.aggregate([
            table['centroid_lon'].min().name('lon_min'),
            table['centroid_lon'].max().name('lon_max'),
            table['centroid_lat'].min().name('lat_min'),
            table['centroid_lat'].max().name('lat_max')
        ]).execute()
        
        # Build summary dictionary
        summary = {
            'dataset_name': dataset_name,
            'h3_indexing_results': {
                'total_records': int(total_records),
                'h3_resolution': int(h3_resolution) if h3_resolution else None,
                'unique_h3_cells': int(unique_h3_cells)
            },
            'h3_cell_statistics': {
                'avg_records_per_cell': float(cell_distribution['avg_records_per_cell'].iloc[0]),
                'max_records_per_cell': int(cell_distribution['max_records_per_cell'].iloc[0]),
                'min_records_per_cell': int(cell_distribution['min_records_per_cell'].iloc[0])
            },
            'coverage_statistics': {
                'total_area_m2': float(coverage_stats['total_area_m2'].iloc[0]),
                'avg_installation_area_m2': float(coverage_stats['avg_installation_area_m2'].iloc[0]),
                'median_installation_area_m2': float(coverage_stats['median_installation_area_m2'].iloc[0])
            },
            'geographic_distribution': {
                'longitude_range': [float(geo_stats['lon_min'].iloc[0]), float(geo_stats['lon_max'].iloc[0])],
                'latitude_range': [float(geo_stats['lat_min'].iloc[0]), float(geo_stats['lat_max'].iloc[0])],
                'geographic_extent_degrees': {
                    'lon_span': float(geo_stats['lon_max'].iloc[0] - geo_stats['lon_min'].iloc[0]),
                    'lat_span': float(geo_stats['lat_max'].iloc[0] - geo_stats['lat_min'].iloc[0])
                }
            }
        }
        
        print(f"   📊 H3 Indexing Summary for {dataset_name}:")
        print(f"      - Total records: {total_records:,}")
        print(f"      - H3 resolution: {h3_resolution}")
        print(f"      - Unique H3 cells: {unique_h3_cells:,}")
        print(f"      - Avg records per cell: {summary['h3_cell_statistics']['avg_records_per_cell']:.1f}")
        print(f"      - Total area: {summary['coverage_statistics']['total_area_m2']:,.0f} m²")
        
        return summary
        
    except Exception as e:
        print(f"      - Error computing H3 summary with Ibis: {e}")
        return {'dataset_name': 'unknown', 'record_count': 0, 'error': str(e)}


# =============================================================================
# HELPER FUNCTIONS (prefixed with _ to exclude from DAG visualization)
# =============================================================================

def _get_optimal_h3_resolution(
    h3_resolution_calculator: Dict[str, Dict[str, Any]],
    target_sensor: str,
    target_use_case: str
) -> int:
    """
    Extract optimal H3 resolution for target sensor and use case.

    Args:
        h3_resolution_calculator: H3 resolution calculations
        target_sensor: Target sensor identifier
        target_use_case: Target use case identifier

    Returns:
        Optimal H3 resolution
    """
    try:
        sensor_config = h3_resolution_calculator[target_sensor]
        use_case_config = sensor_config['h3_configurations'][target_use_case]
        return use_case_config['optimal_h3_resolution']
    except KeyError as e:
        print(f"      - Could not find H3 resolution for {target_sensor}/{target_use_case}: {e}")
        print(f"      - Using default H3 resolution: {DEFAULT_H3_RESOLUTION}")
        return DEFAULT_H3_RESOLUTION


def _apply_h3_indexing_vectorized(gdf: gpd.GeoDataFrame, h3_resolution: int) -> gpd.GeoDataFrame:
    """
    Apply vectorized H3 indexing using h3ronpy.pandas.vector.geodataframe_to_cells().

    Uses h3ronpy's geodataframe_to_cells function which requires Point geometries.
    We create a temporary GeoDataFrame with Point geometries from centroids.

    Args:
        gdf: GeoDataFrame with centroid coordinates
        h3_resolution: H3 resolution to use

    Returns:
        GeoDataFrame with H3 index column added
    """
    try:
        from h3ronpy.pandas.vector import geodataframe_to_cells
        from shapely.geometry import Point

        print(f"      - Using h3ronpy.pandas.vector.geodataframe_to_cells()")

        # Create temporary GeoDataFrame with Point geometries for centroids
        # h3ronpy.geodataframe_to_cells requires geometry column to contain the shapes to index
        centroid_gdf = gdf.copy()
        centroid_gdf['geometry'] = centroid_gdf.apply(
            lambda row: Point(row['centroid_lon'], row['centroid_lat']),
            axis=1
        )

        # Use h3ronpy's vectorized geodataframe_to_cells operation
        # This explodes rows according to number of cells derived from geometry
        h3_cells_df = geodataframe_to_cells(
            centroid_gdf,
            resolution=h3_resolution,
            cell_column_name='h3_index'
        )

        # Since we're using Point geometries (centroids), each point should map to exactly one H3 cell
        # So the exploding shouldn't create additional rows
        result_gdf = gdf.copy()
        result_gdf['h3_index'] = h3_cells_df['h3_index']
        result_gdf['h3_resolution'] = h3_resolution

        print(f"      - Vectorized H3 indexing successful for {len(result_gdf)} records")

        return result_gdf

    except ImportError:
        print(f"      - h3ronpy not available, falling back to h3-py v4+")
        return _apply_h3_indexing_h3py(gdf, h3_resolution)


def _apply_h3_indexing_h3py(gdf: gpd.GeoDataFrame, h3_resolution: int) -> gpd.GeoDataFrame:
    """
    Fallback H3 indexing using h3-py v4+ API.

    Uses h3-py v4+ latlng_to_cell function for H3 indexing when h3ronpy is not available.

    Args:
        gdf: GeoDataFrame with centroid coordinates
        h3_resolution: H3 resolution to use

    Returns:
        GeoDataFrame with H3 index column added
    """
    try:
        import h3.api.memview_int as h3  # h3-py v4+

        print(f"      - Using h3-py v4+ API")

        # Use vectorized operations on pandas Series
        h3_indices = gdf.apply(
            lambda row: h3.latlng_to_cell(row['centroid_lat'], row['centroid_lon'], h3_resolution),
            axis=1
        )

        result_gdf = gdf.copy()
        result_gdf['h3_index'] = h3_indices
        result_gdf['h3_resolution'] = h3_resolution

        print(f"      - H3-py indexing successful for {len(result_gdf)} records")

        return result_gdf

    except ImportError:
        raise ImportError("Neither h3ronpy nor h3-py v4+ is available. Please install h3ronpy or h3-py>=4.0")


def _convert_to_ibis_table(gdf: gpd.GeoDataFrame, dataset_name: str) -> ir.Table:
    """
    Convert GeoDataFrame to Ibis table with DuckDB backend for efficient processing.

    Uses Ibis DuckDB backend with spatial extension for geospatial operations
    and H3 extension for H3-specific functions.

    Args:
        gdf: GeoDataFrame with H3 indexing applied
        dataset_name: Dataset name for table naming

    Returns:
        Ibis Table with DuckDB backend
    """
    # Create DuckDB connection with spatial and H3 extensions
    con = ibis.duckdb.connect()

    # Load spatial extension for geometry operations
    try:
        con.raw_sql("INSTALL spatial; LOAD spatial;")
        print(f"      - Spatial extension loaded successfully")
    except Exception as e:
        print(f"      - Spatial extension issue: {e}")
        try:
            con.raw_sql("INSTALL spatial; LOAD spatial;")
            print(f"      - Spatial extension loaded from community")
        except Exception as e2:
            print(f"      - Failed to load spatial extension: {e2}")
            raise

    # Load H3 extension for H3 operations (if available)
    try:
        con.raw_sql("INSTALL h3 FROM community; LOAD h3;")
        print(f"      - Loaded DuckDB H3 extension")
    except Exception:
        print(f"      - DuckDB H3 extension not available, using computed H3 indices")

    # Convert GeoDataFrame to Ibis table
    # Use table name based on dataset for better organization
    table_name = f"pv_h3_{dataset_name}"

    # Register the GeoDataFrame as a table in DuckDB
    ibis_table = con.register(gdf, table_name)

    print(f"      - Converted to Ibis table '{table_name}' with DuckDB backend")

    return ibis_table


def _create_empty_h3_table() -> ir.Table:
    """
    Create empty Ibis table with expected H3 schema.

    Returns:
        Empty Ibis Table with H3 indexing schema
    """
    con = ibis.duckdb.connect()

    # Load spatial extension for GEOMETRY type
    try:
        con.raw_sql("INSTALL spatial; LOAD spatial;")
    except Exception:
        try:
            con.raw_sql("INSTALL spatial; LOAD spatial;")
        except Exception as e:
            print(f"      - Warning: Could not load spatial extension: {e}")

    # Create empty table with expected schema
    empty_sql = """
    SELECT
        CAST(NULL AS VARCHAR) AS unified_id,
        CAST(NULL AS VARCHAR) AS dataset,
        CAST(NULL AS GEOMETRY) AS geometry,
        CAST(NULL AS DOUBLE) AS area_m2,
        CAST(NULL AS DOUBLE) AS spheroid_area,
        CAST(NULL AS DOUBLE) AS centroid_lon,
        CAST(NULL AS DOUBLE) AS centroid_lat,
        CAST(NULL AS VARCHAR) AS h3_index,
        CAST(NULL AS INTEGER) AS h3_resolution
    WHERE FALSE
    """

    return con.sql(empty_sql)
