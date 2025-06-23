"""
Geospatial operations utility module.

This module contains utility functions for processing geospatial files
into standardized GeoDataFrames with consistent CRS, metadata, and calculations.

These are utility functions (not Hamilton nodes) that implement the actual
geospatial processing logic called by Hamilton nodes.
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
from typing import List, Dict, Any


def process_geospatial_files(
    geospatial_files: List[str],
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]]
) -> gpd.GeoDataFrame:
    """
    Process geospatial files into standardized GeoDataFrame.

    Uses simplified processing with staging-specific settings.
    Standardizes CRS to WGS84 and adds metadata columns.
    
    Args:
        geospatial_files: List of geospatial file paths
        dataset_name: Name of the dataset
        dataset_metadata: Complete metadata dictionary
        
    Returns:
        gpd.GeoDataFrame: Processed geospatial data with standardized schema
    """
    metadata = dataset_metadata[dataset_name]

    print(f"🔧 Processing geospatial data for {dataset_name}")
    print(f"   Files to process: {len(geospatial_files)}")

    # Load geospatial files
    ds_dataframes = []
    for fname in geospatial_files:
        if fname.endswith(('.geojson', '.json', '.shp', '.gpkg')):
            try:
                file_gdf = gpd.read_file(fname)
                ds_dataframes.append(file_gdf)
                print(f"     📄 Loaded {Path(fname).name}: {len(file_gdf)} features, {len(file_gdf.columns)} columns")
            except Exception as e:
                print(f"     ❌ Error reading {Path(fname).name}: {e}")
                continue

    if len(ds_dataframes) == 0:
        raise ValueError(f"No valid geospatial files found for {dataset_name}")

    # Concatenate all dataframes (preserve all columns)
    gdf = gpd.GeoDataFrame(pd.concat(ds_dataframes, ignore_index=True))

    # Ensure geometry column is properly set
    if 'geometry' not in gdf.columns and hasattr(gdf, 'geometry'):
        gdf['geometry'] = gdf.geometry

    print(f"   📊 Combined dataset: {len(gdf)} features, {len(gdf.columns)} columns")

    # Apply basic cleanup
    gdf = _apply_basic_cleanup(gdf, metadata, dataset_name)
    
    # Standardize CRS
    gdf = _standardize_crs(gdf, dataset_name)
    
    # Add metadata columns
    gdf = _add_metadata_columns(gdf, dataset_name, metadata)
    
    # Calculate spatial metrics
    gdf = _calculate_spatial_metrics(gdf, dataset_name)
    
    # Add processing metadata
    gdf = _add_processing_metadata(gdf)

    print(f"Processed {len(gdf)} features for {dataset_name}")
    return gdf


def _apply_basic_cleanup(
    gdf: gpd.GeoDataFrame, 
    metadata: Dict[str, Any], 
    dataset_name: str
) -> gpd.GeoDataFrame:
    """Apply basic cleanup operations."""
    # Remove invalid geometries if specified
    if metadata.get('rm_invalid', True):
        initial_count = len(gdf)
        gdf = gdf[gdf.geometry.is_valid]
        if len(gdf) < initial_count:
            print(f"   🧹 Removed {initial_count - len(gdf)} invalid geometries")

    # Remove exact duplicates only
    initial_count = len(gdf)
    gdf = gdf.drop_duplicates()
    if len(gdf) < initial_count:
        print(f"   🧹 Removed {initial_count - len(gdf)} duplicate rows")

    if gdf is None or len(gdf) == 0:
        raise ValueError(f"No valid geometries found for {dataset_name}")
        
    return gdf


def _standardize_crs(gdf: gpd.GeoDataFrame, dataset_name: str) -> gpd.GeoDataFrame:
    """Standardize CRS to WGS84."""
    print(f"   🗺️  Original CRS: {gdf.crs}")
    
    target_crs = 'EPSG:4326'  # WGS84
    if gdf.crs is None:
        print(f"   ⚠️  No CRS detected, assuming WGS84")
        gdf = gdf.set_crs(target_crs)
    elif gdf.crs != target_crs:
        print(f"   🔄 Converting from {gdf.crs} to {target_crs}")
        try:
            gdf = gdf.to_crs(target_crs)
            print(f"   ✅ CRS conversion successful")
        except Exception as crs_error:
            print(f"   ❌ CRS conversion failed: {crs_error}")
            print(f"   🔧 Setting CRS to WGS84 directly")
            gdf = gdf.set_crs(target_crs)
    else:
        print(f"   ✅ Already in target CRS: {target_crs}")
        
    return gdf


def _add_metadata_columns(
    gdf: gpd.GeoDataFrame, 
    dataset_name: str, 
    metadata: Dict[str, Any]
) -> gpd.GeoDataFrame:
    """Add metadata columns to the GeoDataFrame."""
    print(f"   📝 Adding metadata columns to {len(gdf)} rows")

    # Ensure we're working with a copy
    gdf = gdf.copy()

    # Add metadata columns
    gdf.loc[:, 'dataset_name'] = dataset_name
    gdf.loc[:, 'doi'] = metadata['doi']
    gdf.loc[:, 'repository_type'] = metadata['repo']
    gdf.loc[:, 'label_format'] = metadata['label_fmt']

    # Handle complex metadata fields safely
    geom_type_str = str(metadata.get('geom_type', 'unknown'))
    if isinstance(metadata.get('geom_type'), (list, dict)):
        geom_type_str = str(metadata['geom_type'])
    gdf.loc[:, 'source_geometry_type'] = geom_type_str

    gdf.loc[:, 'source_crs'] = str(metadata.get('crs', 'unknown'))

    # Handle label_count which might be a list or integer
    label_count = metadata.get('label_count', 0)
    if isinstance(label_count, list):
        label_count = sum(label_count) if all(isinstance(x, (int, float)) for x in label_count) else str(label_count)
    gdf.loc[:, 'source_label_count'] = label_count
    
    return gdf


def _calculate_spatial_metrics(gdf: gpd.GeoDataFrame, dataset_name: str) -> gpd.GeoDataFrame:
    """Calculate spatial metrics (areas and centroids)."""
    print(f"   📐 Calculating areas and centroids")

    # Check geometry types
    geom_types = gdf.geometry.geom_type.value_counts()
    print(f"   📊 Geometry types: {dict(geom_types)}")

    has_polygons = any(geom_type in ['Polygon', 'MultiPolygon'] for geom_type in geom_types.index)

    # Calculate areas for polygons
    if has_polygons:
        gdf = _calculate_areas(gdf)
    else:
        gdf.loc[:, 'area_m2'] = 0.0

    # Calculate centroids
    gdf = _calculate_centroids(gdf, has_polygons)
    
    # Convert geometry to WKT for storage
    print(f"   📝 Converting geometries to WKT")
    gdf.loc[:, 'geometry_wkt'] = gdf.geometry.to_wkt()
    
    return gdf


def _calculate_areas(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Calculate areas using Web Mercator projection."""
    print(f"   🔄 Converting to Web Mercator (EPSG:3857) for area calculation")
    try:
        gdf_projected = gdf.to_crs('EPSG:3857')
        projected_areas = gdf_projected.geometry.area
        print(f"   ✅ Area calculation successful using Web Mercator")

        # Assign areas, handling mixed geometry types
        gdf.loc[:, 'area_m2'] = 0.0  # Initialize all to 0
        polygon_mask = gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])
        gdf.loc[polygon_mask, 'area_m2'] = projected_areas[polygon_mask]

    except Exception as crs_error:
        print(f"   ❌ Web Mercator conversion failed: {crs_error}")
        print(f"   🔄 Setting areas to 0 for all geometries")
        gdf.loc[:, 'area_m2'] = 0.0
        
    return gdf


def _calculate_centroids(gdf: gpd.GeoDataFrame, has_polygons: bool) -> gpd.GeoDataFrame:
    """Calculate centroids using projected coordinates."""
    print(f"   📍 Calculating centroids using projected coordinates")
    try:
        # Use projected coordinates for accuracy
        if has_polygons:
            gdf_projected_centroids = gdf.to_crs('EPSG:3857')
        else:
            gdf_projected_centroids = gdf.to_crs('EPSG:3857')
            
        projected_centroids = gdf_projected_centroids.geometry.centroid

        # Convert centroids back to WGS84
        centroids_gdf = gpd.GeoDataFrame(geometry=projected_centroids, crs='EPSG:3857')
        centroids_wgs84 = centroids_gdf.to_crs('EPSG:4326')

        gdf.loc[:, 'centroid_lon'] = centroids_wgs84.geometry.x
        gdf.loc[:, 'centroid_lat'] = centroids_wgs84.geometry.y
        print(f"   ✅ Centroid calculation successful")

    except Exception as centroid_error:
        print(f"   ❌ Projected centroid calculation failed: {centroid_error}")
        print(f"   🔄 Using simple WGS84 centroids")
        centroids = gdf.geometry.centroid
        gdf.loc[:, 'centroid_lon'] = centroids.x
        gdf.loc[:, 'centroid_lat'] = centroids.y
        
    return gdf


def _add_processing_metadata(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add processing metadata columns."""
    gdf.loc[:, 'processed_at'] = pd.Timestamp.now()
    gdf.loc[:, 'source_system'] = 'hamilton_doi_pipeline_refactored'
    return gdf
