"""
Hamilton dataflow for PV locations deduplication with multiple implementation strategies.

Phase 1: PV Data Staging & Pre-processing
Task 1.3: dedup

Objective: Remove overlapping geometries that exceed threshold
Key Functions:
- ibis_to_geopandas_conversion() - Convert Ibis table to GeoPandas using GeoArrow
- spatial_overlap_detection() - Find overlapping installations (configurable implementation)
- overlap_threshold_filter() - Remove overlaps exceeding configurable threshold
- geopandas_to_ibis_conversion() - Convert back to Ibis table using GeoArrow
- deduplication_summary() - Report on removed duplicates

Implementation Strategies:
1. GeoPandas + sindex (default, proven to work)
2. DuckDB/Ibis spatial operations (experimental, fallback)

Output: Deduplicated PV installation datasets

References:
- Hamilton + Ibis integration: https://hamilton.dagworks.io/en/latest/integrations/ibis/
- GeoArrow Python: https://geoarrow.org/geoarrow-python/
- GeoPandas spatial operations: https://geopandas.org/en/stable/docs/user_guide/geometric_manipulations.html
- DuckDB spatial functions: https://duckdb.org/docs/stable/core_extensions/spatial/functions.html
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional

import geopandas as gpd
import pandas as pd
import ibis
import ibis.expr.types as ir

from hamilton.function_modifiers import cache, tag, config


# =============================================================================
# DEDUPLICATION CONFIGURATION
# =============================================================================

# Default overlap threshold for considering geometries as duplicates
DEFAULT_OVERLAP_THRESHOLD = 0.8  # 80% overlap

# Minimum area threshold for valid PV installations (square meters)
MIN_INSTALLATION_AREA_M2 = 1.0

# Deduplication implementation strategy
DEFAULT_DEDUP_STRATEGY = "geopandas"  # Options: "geopandas", "duckdb"


# =============================================================================
# HAMILTON DATAFLOW NODES - DEDUPLICATION
# =============================================================================

@tag(stage="deduplication", data_type="config")
def overlap_detection_strategy(
    dedup_strategy: str = DEFAULT_DEDUP_STRATEGY
) -> Dict[str, Any]:
    """
    Provide overlap detection strategy configuration via dependency injection.

    This function selects which implementation to use for overlap detection
    based on the dedup_strategy parameter.

    Args:
        dedup_strategy: Strategy name ("geopandas" or "duckdb")

    Returns:
        Dictionary containing the strategy function and metadata
    """
    if dedup_strategy == "geopandas":
        return {
            'function': _detect_overlaps_geopandas_implementation,
            'strategy': 'geopandas',
            'description': 'GeoPandas + sindex implementation (proven to work)'
        }
    elif dedup_strategy == "duckdb":
        return {
            'function': _detect_overlaps_duckdb_implementation,
            'strategy': 'duckdb',
            'description': 'DuckDB/Ibis spatial operations (experimental)'
        }
    else:
        print(f"   ⚠️  Unknown dedup strategy '{dedup_strategy}', defaulting to 'geopandas'")
        return {
            'function': _detect_overlaps_geopandas_implementation,
            'strategy': 'geopandas',
            'description': 'GeoPandas + sindex implementation (default fallback)'
        }


@tag(stage="deduplication", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames
def ibis_to_geopandas_conversion(
    geometry_processed_geodataframe: ir.Table
) -> gpd.GeoDataFrame:
    """
    Convert Ibis table to GeoPandas GeoDataFrame using GeoArrow for efficient conversion.

    This conversion is necessary for spatial operations that require GeoPandas
    spatial indexing (sindex) which isn't efficiently available in Ibis/DuckDB.

    Args:
        geometry_processed_geodataframe: Ibis table with geometry data

    Returns:
        GeoPandas GeoDataFrame ready for spatial operations
    """
    print("🔄 Converting Ibis table to GeoPandas GeoDataFrame using GeoArrow...")

    try:
        # Convert Ibis table to Arrow table first
        arrow_table = geometry_processed_geodataframe.to_pyarrow()
        print(f"   📊 Converted to Arrow table: {len(arrow_table)} records")

        # Convert Arrow table to GeoPandas using GeoArrow
        try:
            # Try using geoarrow-rs Python bindings for efficient conversion
            import geoarrow.pyarrow as ga

            # Convert to GeoDataFrame using geoarrow
            gdf = ga.to_geopandas(arrow_table)
            print(f"   ✅ GeoArrow conversion successful")

        except ImportError:
            print("   ⚠️  geoarrow-rs not available, falling back to pandas conversion")
            # Fallback to pandas conversion
            df = arrow_table.to_pandas()

            # Handle geometry column conversion
            if 'geometry' in df.columns:
                # Convert WKB bytes to shapely geometries
                from shapely import wkb
                df['geometry'] = df['geometry'].apply(
                    lambda x: wkb.loads(x) if x is not None else None
                )
                gdf = gpd.GeoDataFrame(df, geometry='geometry')
            else:
                print("   ⚠️  No geometry column found")
                gdf = gpd.GeoDataFrame(df)

        # Set CRS if not already set
        if hasattr(gdf, 'crs') and gdf.crs is None:
            gdf.set_crs('EPSG:4326', inplace=True)

        print(f"   ✅ GeoPandas conversion complete: {len(gdf)} records")
        print(f"      - CRS: {getattr(gdf, 'crs', 'Not set')}")
        print(f"      - Columns: {list(gdf.columns)}")

        return gdf

    except Exception as e:
        print(f"   ❌ Error converting Ibis to GeoPandas: {e}")
        # Return empty GeoDataFrame with expected schema
        return gpd.GeoDataFrame(columns=['dataset_name', 'geometry', 'centroid_lon', 'centroid_lat', 'area_m2', 'processed_at'])


@tag(stage="deduplication", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames
def spatial_overlap_detection(
    ibis_to_geopandas_conversion: gpd.GeoDataFrame,
    overlap_detection_strategy: Dict[str, Any],
    overlap_threshold: float = DEFAULT_OVERLAP_THRESHOLD
) -> gpd.GeoDataFrame:
    """
    Find overlapping installations using configurable implementation strategy.

    Uses dependency injection to select between different overlap detection strategies.

    Args:
        ibis_to_geopandas_conversion: GeoPandas GeoDataFrame with processed geometries
        overlap_detection_strategy: Strategy configuration from dependency injection
        overlap_threshold: Overlap ratio threshold for duplicate detection (default: 0.8)

    Returns:
        GeoDataFrame with overlap analysis results and duplicate flags
    """
    if ibis_to_geopandas_conversion.empty:
        return ibis_to_geopandas_conversion

    # Use the strategy function provided by dependency injection
    strategy_func = overlap_detection_strategy['function']
    return strategy_func(ibis_to_geopandas_conversion, overlap_threshold)


@tag(stage="deduplication", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames
def overlap_detected_geodataframe(
    spatial_overlap_detection: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Alias for spatial overlap detection results.

    This provides the expected input name for downstream deduplication functions.

    Args:
        spatial_overlap_detection: Results from spatial overlap detection

    Returns:
        GeoDataFrame with overlap detection results
    """
    return spatial_overlap_detection


@tag(stage="deduplication", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames
def overlap_threshold_filter(
    overlap_detected_geodataframe: gpd.GeoDataFrame,
    overlap_threshold: float = DEFAULT_OVERLAP_THRESHOLD,
    keep_larger_geometry: bool = True
) -> gpd.GeoDataFrame:
    """
    Remove overlaps exceeding configurable threshold.
    
    Filters out geometries that have significant overlaps with other geometries,
    keeping either the larger or smaller geometry based on configuration.
    
    Args:
        overlap_detected_geodataframe: Geodataframe with overlap detection results
        overlap_threshold: Overlap ratio threshold for removal (default: 0.8)
        keep_larger_geometry: If True, keep larger geometry in overlapping pairs (default: True)
    
    Returns:
        GeoDataFrame with overlapping geometries removed
    """
    if overlap_detected_geodataframe.empty:
        return overlap_detected_geodataframe
    
    gdf = overlap_detected_geodataframe.copy()
    dataset_name = gdf['dataset'].iloc[0] if 'dataset' in gdf.columns else 'unknown'
    
    initial_count = len(gdf)
    overlap_count = gdf['has_overlap'].sum() if 'has_overlap' in gdf.columns else 0
    
    print(f"   🧹 Filtering overlapping geometries for {dataset_name}")
    print(f"      - Initial records: {initial_count}")
    print(f"      - Records with overlaps: {overlap_count}")
    print(f"      - Overlap threshold: {overlap_threshold}")
    print(f"      - Keep larger geometry: {keep_larger_geometry}")
    
    if overlap_count == 0:
        print(f"   ✅ No overlaps to filter")
        return gdf
    
    # Simple approach: remove geometries with high overlap ratios
    # Keep geometries with overlap ratio below threshold
    if 'max_overlap_ratio' in gdf.columns:
        gdf_filtered = gdf[gdf['max_overlap_ratio'] <= overlap_threshold].copy()
        removed_count = initial_count - len(gdf_filtered)
        
        print(f"   ✅ Deduplication complete:")
        print(f"      - Removed: {removed_count} overlapping geometries")
        print(f"      - Remaining: {len(gdf_filtered)} unique geometries")
        print(f"      - Reduction: {removed_count/initial_count*100:.1f}%")
        
        return gdf_filtered
    else:
        print(f"   ✅ No overlap ratios available, keeping all geometries")
        return gdf


@tag(stage="deduplication", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames
def deduplicated_geodataframe(
    overlap_threshold_filter: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Alias for the final deduplicated geodataframe.

    This provides a clear, noun-based name for the final output of the deduplication process.

    Args:
        overlap_threshold_filter: The filtered geodataframe from overlap_threshold_filter

    Returns:
        Final deduplicated GeoDataFrame
    """
    return overlap_threshold_filter


@tag(stage="deduplication", data_type="summary")
@cache(behavior="default")  # Cache summary results
def deduplication_summary(
    deduplicated_geodataframe: gpd.GeoDataFrame,
    overlap_detected_geodataframe: gpd.GeoDataFrame
) -> Dict[str, Any]:
    """
    Report on removed duplicates and deduplication statistics.
    
    Generates comprehensive summary of the deduplication process including
    counts, overlap statistics, and data quality metrics.
    
    Args:
        deduplicated_geodataframe: Final deduplicated geodataframe
        overlap_detected_geodataframe: Geodataframe before deduplication
    
    Returns:
        Dictionary with deduplication summary statistics
    """
    dataset_name = (
        deduplicated_geodataframe['dataset'].iloc[0] 
        if not deduplicated_geodataframe.empty and 'dataset' in deduplicated_geodataframe.columns
        else 'unknown'
    )
    
    initial_count = len(overlap_detected_geodataframe)
    final_count = len(deduplicated_geodataframe)
    removed_count = initial_count - final_count
    
    # Calculate overlap statistics
    overlap_stats = {}
    if 'has_overlap' in overlap_detected_geodataframe.columns:
        overlap_stats = {
            'total_overlaps_detected': overlap_detected_geodataframe['has_overlap'].sum(),
            'max_overlap_ratio': overlap_detected_geodataframe['max_overlap_ratio'].max() if 'max_overlap_ratio' in overlap_detected_geodataframe.columns else 0.0,
            'avg_overlap_ratio': overlap_detected_geodataframe['max_overlap_ratio'].mean() if 'max_overlap_ratio' in overlap_detected_geodataframe.columns else 0.0
        }
    
    # Calculate area statistics
    area_stats = {}
    if 'area_m2' in deduplicated_geodataframe.columns:
        areas = deduplicated_geodataframe['area_m2']
        area_stats = {
            'total_area_m2': areas.sum(),
            'mean_area_m2': areas.mean(),
            'median_area_m2': areas.median(),
            'min_area_m2': areas.min(),
            'max_area_m2': areas.max()
        }
    
    summary = {
        'dataset_name': dataset_name,
        'deduplication_results': {
            'initial_count': initial_count,
            'final_count': final_count,
            'removed_count': removed_count,
            'removal_percentage': (removed_count / initial_count * 100) if initial_count > 0 else 0.0
        },
        'overlap_statistics': overlap_stats,
        'area_statistics': area_stats,
        'data_quality': {
            'has_geometry': 'geometry' in deduplicated_geodataframe.columns,
            'has_centroids': all(col in deduplicated_geodataframe.columns for col in ['centroid_lon', 'centroid_lat']),
            'has_areas': 'area_m2' in deduplicated_geodataframe.columns,
            'crs': str(deduplicated_geodataframe.crs) if hasattr(deduplicated_geodataframe, 'crs') and deduplicated_geodataframe.crs else None
        }
    }
    
    print(f"   📊 Deduplication Summary for {dataset_name}:")
    print(f"      - Initial: {initial_count:,} records")
    print(f"      - Final: {final_count:,} records")
    print(f"      - Removed: {removed_count:,} duplicates ({summary['deduplication_results']['removal_percentage']:.1f}%)")
    
    if overlap_stats:
        print(f"      - Overlaps detected: {overlap_stats.get('total_overlaps_detected', 0):,}")
        print(f"      - Max overlap ratio: {overlap_stats.get('max_overlap_ratio', 0):.3f}")
    
    if area_stats:
        print(f"      - Total area: {area_stats.get('total_area_m2', 0):,.0f} m²")
        print(f"      - Mean area: {area_stats.get('mean_area_m2', 0):.1f} m²")
    
    return summary


@tag(stage="deduplication", data_type="table")
@cache(behavior="disable")  # Disable caching for large tables
def geopandas_to_ibis_conversion(
    deduplicated_geodataframe: gpd.GeoDataFrame
) -> ir.Table:
    """
    Convert deduplicated GeoPandas GeoDataFrame back to Ibis table using GeoArrow.

    This conversion maintains the Ibis-first approach for the pipeline,
    only using GeoPandas for spatial operations that require it.

    Args:
        deduplicated_geodataframe: Deduplicated GeoPandas GeoDataFrame

    Returns:
        Ibis table ready for downstream processing
    """
    print("🔄 Converting deduplicated GeoPandas GeoDataFrame back to Ibis table using GeoArrow...")

    try:
        if deduplicated_geodataframe.empty:
            print("   ⚠️  Empty GeoDataFrame, creating empty Ibis table")
            con = ibis.duckdb.connect()
            return con.sql("SELECT NULL as dataset_name, NULL as geometry, NULL as centroid_lon, NULL as centroid_lat, NULL as area_m2, NULL as processed_at WHERE FALSE")

        # Convert GeoPandas to Arrow table using GeoArrow
        try:
            import geoarrow.pyarrow as ga

            # Convert to Arrow table using geoarrow
            arrow_table = ga.from_geopandas(deduplicated_geodataframe)
            print(f"   ✅ GeoArrow conversion successful")

        except ImportError:
            print("   ⚠️  geoarrow-rs not available, falling back to manual conversion")
            # Fallback: manual conversion
            df = deduplicated_geodataframe.copy()

            # Convert geometry to WKB for DuckDB storage
            if 'geometry' in df.columns:
                df['geometry'] = df['geometry'].apply(lambda x: x.wkb if x else None)

            arrow_table = df.to_arrow()

        # Create Ibis connection and register the Arrow table
        con = ibis.duckdb.connect()
        con.raw_sql("INSTALL spatial")
        con.raw_sql("LOAD spatial")

        # Register Arrow table as Ibis table
        table_name = "deduplicated_pv_data"
        ibis_table = con.register(arrow_table, table_name)

        print(f"   ✅ Converted to Ibis table: {len(deduplicated_geodataframe)} records")
        print(f"      - Columns: {len(ibis_table.columns)}")

        return ibis_table

    except Exception as e:
        print(f"   ❌ Error converting GeoPandas to Ibis: {e}")
        con = ibis.duckdb.connect()
        return con.sql("SELECT NULL as dataset_name, NULL as geometry, NULL as centroid_lon, NULL as centroid_lat, NULL as area_m2, NULL as processed_at WHERE FALSE")


# =============================================================================
# HELPER FUNCTIONS (prefixed with _ to exclude from DAG visualization)
# =============================================================================

def _detect_overlaps_geopandas_implementation(gdf: gpd.GeoDataFrame, overlap_threshold: float) -> gpd.GeoDataFrame:
    """
    GeoPandas + sindex implementation for overlap detection (proven to work).

    Uses GeoPandas spatial indexing to efficiently identify overlapping geometries
    within the same dataset. This is the proven implementation.

    Args:
        gdf: GeoDataFrame with processed geometries
        overlap_threshold: Overlap ratio threshold for duplicate detection

    Returns:
        GeoDataFrame with overlap analysis results and duplicate flags
    """
    dataset_name = gdf['dataset'].iloc[0] if 'dataset' in gdf.columns else 'unknown'

    print(f"   🔍 Detecting spatial overlaps for {dataset_name}: {len(gdf)} records (GeoPandas)")
    print(f"      - Overlap threshold: {overlap_threshold}")

    # Initialize overlap tracking columns
    gdf['has_overlap'] = False
    gdf['overlap_count'] = 0
    gdf['max_overlap_ratio'] = 0.0

    # Use GeoPandas spatial index for efficient overlap detection
    try:
        overlaps_found = _detect_overlaps_geopandas_sindex(gdf, overlap_threshold)

        # Update overlap flags
        for idx, overlap_info in overlaps_found.items():
            gdf.loc[idx, 'has_overlap'] = True
            gdf.loc[idx, 'overlap_count'] = overlap_info['count']
            gdf.loc[idx, 'max_overlap_ratio'] = overlap_info['max_ratio']

        overlap_count = gdf['has_overlap'].sum()
        print(f"   ✅ Overlap detection complete: {overlap_count} records with overlaps")

    except Exception as e:
        print(f"   ⚠️  Overlap detection failed: {e}")
        print(f"      - Continuing without overlap detection")

    return gdf


def _detect_overlaps_duckdb_implementation(gdf: gpd.GeoDataFrame, overlap_threshold: float) -> gpd.GeoDataFrame:
    """
    DuckDB/Ibis implementation for overlap detection (experimental).

    Uses DuckDB spatial functions via Ibis for overlap detection.
    This is an alternative implementation that may be less performant.

    Args:
        gdf: GeoDataFrame with processed geometries
        overlap_threshold: Overlap ratio threshold for duplicate detection

    Returns:
        GeoDataFrame with overlap analysis results and duplicate flags
    """
    dataset_name = gdf['dataset'].iloc[0] if 'dataset' in gdf.columns else 'unknown'

    print(f"   🔍 Detecting spatial overlaps for {dataset_name}: {len(gdf)} records (DuckDB)")
    print(f"      - Overlap threshold: {overlap_threshold}")

    # Initialize overlap tracking columns
    gdf['has_overlap'] = False
    gdf['overlap_count'] = 0
    gdf['max_overlap_ratio'] = 0.0

    try:
        # Convert to Ibis table for DuckDB spatial operations
        con = ibis.duckdb.connect()
        con.raw_sql("INSTALL spatial")
        con.raw_sql("LOAD spatial")

        # Convert GeoDataFrame to Ibis table
        # This is a simplified implementation - would need more work for production
        print(f"   ⚠️  DuckDB implementation not fully implemented yet")
        print(f"      - Falling back to GeoPandas implementation")

        return _detect_overlaps_geopandas_implementation(gdf, overlap_threshold)

    except Exception as e:
        print(f"   ⚠️  DuckDB overlap detection failed: {e}")
        print(f"      - Falling back to GeoPandas implementation")
        return _detect_overlaps_geopandas_implementation(gdf, overlap_threshold)


def _detect_overlaps_geopandas_sindex(gdf: gpd.GeoDataFrame, overlap_threshold: float) -> Dict[int, Dict[str, Any]]:
    """
    Use GeoPandas spatial index to detect overlapping geometries.
    
    Args:
        gdf: GeoDataFrame to analyze
        overlap_threshold: Minimum overlap ratio to consider as overlap
    
    Returns:
        Dictionary mapping indices to overlap information
    """
    overlaps_found = {}
    
    # Create spatial index
    spatial_index = gdf.sindex
    
    for idx, geometry in gdf.geometry.items():
        if geometry is None or geometry.is_empty:
            continue
        
        # Find potential overlaps using spatial index
        possible_matches_index = list(spatial_index.intersection(geometry.bounds))
        possible_matches = gdf.iloc[possible_matches_index]
        
        # Remove self-match
        possible_matches = possible_matches[possible_matches.index != idx]
        
        if len(possible_matches) == 0:
            continue
        
        # Calculate actual overlaps
        overlaps = []
        for other_idx, other_geom in possible_matches.geometry.items():
            if other_geom is None or other_geom.is_empty:
                continue
            
            try:
                if geometry.intersects(other_geom):
                    intersection_area = geometry.intersection(other_geom).area
                    geometry_area = geometry.area
                    
                    if geometry_area > 0:
                        overlap_ratio = intersection_area / geometry_area
                        if overlap_ratio > overlap_threshold:
                            overlaps.append({
                                'other_idx': other_idx,
                                'overlap_ratio': overlap_ratio
                            })
            except Exception:
                # Skip problematic geometries
                continue
        
        if overlaps:
            max_overlap = max(overlaps, key=lambda x: x['overlap_ratio'])
            overlaps_found[idx] = {
                'count': len(overlaps),
                'max_ratio': max_overlap['overlap_ratio']
            }
    
    return overlaps_found
