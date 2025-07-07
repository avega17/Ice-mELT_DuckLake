"""
Hamilton dataflow for PV locations geometry processing.

Phase 1: PV Data Staging & Pre-processing
Task 1.2: geom_stats

Objective: Standardize geometries and calculate spatial metrics
Key Functions:
- reprojected_geometries() - Reproject to consistent CRS (EPSG:4326)
- geometry_centroids() - Calculate centroids for area-based PV installations
- rough_area_estimates() - Estimate installation areas in square meters
Output: Geometrically standardized datasets with spatial metrics

References:
- GeoPandas CRS handling: https://geopandas.org/en/stable/docs/user_guide/projections.html
- Geometry operations: https://geopandas.org/en/stable/docs/user_guide/geometric_manipulations.html
- Area calculations: https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.area.html
- PV installation size estimates: Barbose et al. (2021) "Tracking the Sun" LBNL report
    https://emp.lbl.gov/publications/tracking-sun-pricing-and-design-1
"""

from __future__ import annotations

import warnings
from typing import Dict, Any, Tuple, Optional
import numpy as np

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely import wkt
import pyproj

from hamilton.function_modifiers import cache, tag, config


# =============================================================================
# GEOMETRY PROCESSING CONFIGURATION
# =============================================================================

# Target CRS for consistent processing (WGS84)
TARGET_CRS = "EPSG:4326"

# CRS for area calculations (equal-area projection)
AREA_CALCULATION_CRS = "EPSG:3857"  # Web Mercator (approximate for global data)

# Geometry validation thresholds
MIN_AREA_M2 = 1.0  # Minimum area in square meters for valid PV installation
MAX_AREA_M2 = 1e9  # Maximum area in square meters (1000 km²)
MIN_COORDINATE = -180.0  # Minimum valid longitude/latitude
MAX_COORDINATE = 180.0   # Maximum valid longitude/latitude

# PV installation area estimates based on Barbose et al. (2021) "Tracking the Sun" LBNL
# Reference: https://emp.lbl.gov/sites/default/files/tracking_the_sun_2021_report.pdf
# Typical system sizes by installation type (square meters)
INSTALLATION_AREA_ESTIMATES = {
    'residential': 75.0,      # ~5-10 kW residential systems (Barbose 2021: median ~7.15 kW)
    'rooftop': 75.0,          # Alias for residential
    'commercial': 1500.0,     # ~100-500 kW commercial systems (Barbose 2021: median ~200 kW)
    'utility_scale': 50000.0, # >1 MW utility systems (Barbose 2021: median ~20 MW)
    'ground_mount': 1000.0,   # Mixed ground-mount systems
    'unknown': 200.0,         # Conservative estimate between residential and commercial
    'mixed': 500.0            # Mixed installation types
}


# =============================================================================
# HAMILTON DATAFLOW NODES - GEOMETRY PROCESSING
# =============================================================================

@tag(stage="geometry", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames
def reprojected_geometries(
    standardized_geodataframe: gpd.GeoDataFrame,
    target_crs: str = TARGET_CRS
) -> gpd.GeoDataFrame:
    """
    Reproject geometries to consistent CRS (EPSG:4326).
    
    Ensures all PV installation geometries are in a consistent coordinate reference system
    for downstream processing. Handles missing CRS by defaulting to WGS84.
    
    Args:
        standardized_geodataframe: Standardized geodataframe from schema standardization
        target_crs: Target CRS for reprojection (default: EPSG:4326)
    
    Returns:
        GeoDataFrame with geometries reprojected to target CRS
    """
    if standardized_geodataframe.empty:
        return standardized_geodataframe
    
    gdf = standardized_geodataframe.copy()
    dataset_name = gdf['dataset'].iloc[0] if 'dataset' in gdf.columns else 'unknown'
    
    print(f"   🌍 Reprojecting geometries for {dataset_name}: {len(gdf)} records")
    
    # Handle missing CRS
    if gdf.crs is None:
        print(f"      - No CRS found, assuming {target_crs}")
        gdf.set_crs(target_crs, inplace=True)
        return gdf
    
    # Check if already in target CRS
    current_crs = gdf.crs.to_string()
    if gdf.crs.to_epsg() == int(target_crs.split(':')[1]):
        print(f"      - Already in target CRS: {current_crs}")
        return gdf
    
    # Reproject to target CRS
    try:
        original_crs = current_crs
        gdf = gdf.to_crs(target_crs)
        print(f"      - Reprojected from {original_crs} to {target_crs}")
        
        # Validate coordinates are within reasonable bounds
        _validate_coordinate_bounds(gdf, dataset_name)
        
    except Exception as e:
        print(f"      - ⚠️  Reprojection failed: {e}")
        print(f"      - Keeping original CRS: {current_crs}")
    
    return gdf


@tag(stage="geometry", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames
def geometry_centroids(
    reprojected_geodataframe: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Calculate centroids for area-based PV installations.
    
    Computes geometric centroids for polygon geometries and updates point coordinates.
    Handles mixed geometry types and ensures centroid coordinates are properly calculated.
    
    Args:
        reprojected_geodataframe: Geodataframe with reprojected geometries
    
    Returns:
        GeoDataFrame with updated centroid coordinates
    """
    if reprojected_geodataframe.empty:
        return reprojected_geodataframe
    
    gdf = reprojected_geodataframe.copy()
    dataset_name = gdf['dataset'].iloc[0] if 'dataset' in gdf.columns else 'unknown'
    
    print(f"   📍 Computing centroids for {dataset_name}: {len(gdf)} records")
    
    # Analyze geometry types
    geom_types = gdf.geometry.geom_type.value_counts()
    print(f"      - Geometry types: {dict(geom_types)}")
    
    # Calculate centroids for all geometries
    try:
        centroids = gdf.geometry.centroid
        
        # Update centroid coordinates
        gdf['centroid_lon'] = centroids.x
        gdf['centroid_lat'] = centroids.y
        
        print(f"      - Updated centroid coordinates for {len(gdf)} records")
        
        # Validate centroid coordinates
        _validate_centroid_coordinates(gdf, dataset_name)
        
    except Exception as e:
        print(f"      - ⚠️  Centroid calculation failed: {e}")
        # Fallback: use existing coordinates if available
        if 'centroid_lon' not in gdf.columns or 'centroid_lat' not in gdf.columns:
            gdf['centroid_lon'] = None
            gdf['centroid_lat'] = None
    
    return gdf


@tag(stage="geometry", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames
def rough_area_estimates(
    centroid_geodataframe: gpd.GeoDataFrame,
    area_crs: str = AREA_CALCULATION_CRS
) -> gpd.GeoDataFrame:
    """
    Estimate installation areas in square meters.
    
    Calculates approximate area for polygon geometries using equal-area projection.
    For point geometries, estimates area based on typical installation sizes from
    Barbose et al. (2021) "Tracking the Sun" LBNL report.
    
    Args:
        centroid_geodataframe: Geodataframe with computed centroids
        area_crs: CRS for area calculations (default: EPSG:3857 Web Mercator)
    
    Returns:
        GeoDataFrame with area estimates in square meters
    """
    if centroid_geodataframe.empty:
        return centroid_geodataframe
    
    gdf = centroid_geodataframe.copy()
    dataset_name = gdf['dataset'].iloc[0] if 'dataset' in gdf.columns else 'unknown'
    
    print(f"   📐 Estimating areas for {dataset_name}: {len(gdf)} records")
    
    # Separate by geometry type for different area calculation approaches
    polygons_mask = gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])
    points_mask = gdf.geometry.geom_type == 'Point'
    
    polygon_count = polygons_mask.sum()
    point_count = points_mask.sum()
    
    print(f"      - Polygons: {polygon_count}, Points: {point_count}")
    
    # Initialize area column
    gdf['area_m2'] = 0.0
    
    # Calculate area for polygons using equal-area projection
    if polygon_count > 0:
        try:
            # Reproject to equal-area CRS for accurate area calculation
            polygon_gdf = gdf[polygons_mask].to_crs(area_crs)
            areas = polygon_gdf.geometry.area
            
            # Convert back to original indices
            gdf.loc[polygons_mask, 'area_m2'] = areas.values
            
            print(f"      - Calculated areas for {polygon_count} polygons")
            print(f"      - Area range: {areas.min():.1f} - {areas.max():.1f} m²")
            
        except Exception as e:
            print(f"      - ⚠️  Polygon area calculation failed: {e}")
            # Fallback: use rough WGS84 approximation
            gdf.loc[polygons_mask, 'area_m2'] = gdf.loc[polygons_mask, 'geometry'].area * 111320 * 111320
    
    # Estimate area for points using LBNL Tracking the Sun data
    if point_count > 0:
        default_point_area = INSTALLATION_AREA_ESTIMATES['unknown']
        
        if 'installation_type' in gdf.columns:
            # Use installation type to estimate area based on LBNL data
            area_estimates = gdf.loc[points_mask, 'installation_type'].map(INSTALLATION_AREA_ESTIMATES).fillna(default_point_area)
        else:
            area_estimates = default_point_area
        
        gdf.loc[points_mask, 'area_m2'] = area_estimates
        print(f"      - Estimated areas for {point_count} points using LBNL Tracking the Sun data")
        print(f"      - Default estimate: {default_point_area} m² for unknown types")
    
    # Validate area estimates
    _validate_area_estimates(gdf, dataset_name)
    
    return gdf


# =============================================================================
# HELPER FUNCTIONS (prefixed with _ to exclude from DAG visualization)
# =============================================================================

def _validate_coordinate_bounds(gdf: gpd.GeoDataFrame, dataset_name: str) -> None:
    """Validate that coordinates are within reasonable geographic bounds."""
    if 'centroid_lon' in gdf.columns and 'centroid_lat' in gdf.columns:
        invalid_lon = (gdf['centroid_lon'] < MIN_COORDINATE) | (gdf['centroid_lon'] > MAX_COORDINATE)
        invalid_lat = (gdf['centroid_lat'] < -90) | (gdf['centroid_lat'] > 90)
        
        invalid_count = (invalid_lon | invalid_lat).sum()
        if invalid_count > 0:
            print(f"      - ⚠️  {invalid_count} records with invalid coordinates")


def _validate_centroid_coordinates(gdf: gpd.GeoDataFrame, dataset_name: str) -> None:
    """Validate centroid coordinates are reasonable."""
    if 'centroid_lon' in gdf.columns and 'centroid_lat' in gdf.columns:
        null_coords = gdf[['centroid_lon', 'centroid_lat']].isnull().any(axis=1).sum()
        if null_coords > 0:
            print(f"      - ⚠️  {null_coords} records with null centroid coordinates")


def _validate_area_estimates(gdf: gpd.GeoDataFrame, dataset_name: str) -> None:
    """Validate area estimates are within reasonable bounds."""
    if 'area_m2' in gdf.columns:
        areas = gdf['area_m2']
        
        # Check for unreasonable areas
        too_small = (areas < MIN_AREA_M2).sum()
        too_large = (areas > MAX_AREA_M2).sum()
        null_areas = areas.isnull().sum()
        
        if too_small > 0:
            print(f"      - ⚠️  {too_small} records with area < {MIN_AREA_M2} m²")
        if too_large > 0:
            print(f"      - ⚠️  {too_large} records with area > {MAX_AREA_M2:,.0f} m²")
        if null_areas > 0:
            print(f"      - ⚠️  {null_areas} records with null areas")
        
        # Summary statistics
        valid_areas = areas[(areas >= MIN_AREA_M2) & (areas <= MAX_AREA_M2)]
        if len(valid_areas) > 0:
            print(f"      - Valid areas: mean={valid_areas.mean():.1f} m², median={valid_areas.median():.1f} m²")
