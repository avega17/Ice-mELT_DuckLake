"""
Validation operations utility module.

This module contains utility functions for validating geospatial data quality,
schema compliance, and performing comprehensive data quality checks.

These are utility functions (not Hamilton nodes) that implement the actual
validation logic called by Hamilton nodes.
"""

import pandas as pd
import geopandas as gpd
import pyarrow as pa
from typing import Dict, Any, List


def validate_geospatial_data(
    gdf: gpd.GeoDataFrame,
    dataset_name: str
) -> Dict[str, Any]:
    """
    Validate geospatial data quality and consistency.
    
    Args:
        gdf: GeoDataFrame to validate
        dataset_name: Name of the dataset for reporting
        
    Returns:
        Dict with validation results and status
    """
    print(f"🔍 Validating geospatial data for {dataset_name}")
    
    validation_result = {
        "dataset_name": dataset_name,
        "timestamp": pd.Timestamp.now().isoformat(),
        "status": "passed",
        "issues": [],
        "geometry_validity": {},
        "data_completeness": {},
        "spatial_checks": {}
    }
    
    # Check geometry validity
    geometry_validation = _validate_geometries(gdf)
    validation_result["geometry_validity"] = geometry_validation
    
    # Check data completeness
    completeness_validation = _validate_completeness(gdf)
    validation_result["data_completeness"] = completeness_validation
    
    # Check spatial characteristics
    spatial_validation = _validate_spatial_characteristics(gdf)
    validation_result["spatial_checks"] = spatial_validation
    
    # Determine overall status
    issues = []
    if geometry_validation.get("invalid_count", 0) > 0:
        issues.append(f"Found {geometry_validation['invalid_count']} invalid geometries")
    
    if completeness_validation.get("missing_geometry_count", 0) > 0:
        issues.append(f"Found {completeness_validation['missing_geometry_count']} missing geometries")
        
    if spatial_validation.get("suspicious_coordinates", False):
        issues.append("Found suspicious coordinate values")
    
    validation_result["issues"] = issues
    validation_result["status"] = "failed" if issues else "passed"
    
    print(f"   ✅ Validation complete: {validation_result['status']}")
    if issues:
        for issue in issues:
            print(f"   ⚠️  {issue}")
    
    return validation_result


def validate_arrow_schema(
    arrow_table: pa.Table,
    dataset_name: str
) -> Dict[str, Any]:
    """
    Validate Arrow table schema compliance.
    
    Args:
        arrow_table: Arrow table to validate
        dataset_name: Name of the dataset for reporting
        
    Returns:
        Dict with schema validation results
    """
    print(f"🔍 Validating Arrow schema for {dataset_name}")
    
    schema_validation = {
        "dataset_name": dataset_name,
        "timestamp": pd.Timestamp.now().isoformat(),
        "schema_valid": True,
        "issues": [],
        "column_count": len(arrow_table.columns),
        "row_count": arrow_table.num_rows,
        "schema_info": {}
    }
    
    # Check required columns
    required_columns = ['dataset_name', 'geometry_wkt']
    missing_columns = [col for col in required_columns if col not in arrow_table.column_names]
    
    if missing_columns:
        schema_validation["schema_valid"] = False
        schema_validation["issues"].append(f"Missing required columns: {missing_columns}")
    
    # Check geometry column
    geometry_columns = [col for col in arrow_table.column_names if 'geometry' in col.lower()]
    if not geometry_columns:
        schema_validation["schema_valid"] = False
        schema_validation["issues"].append("No geometry column found")
    else:
        schema_validation["schema_info"]["geometry_columns"] = geometry_columns
    
    # Check metadata
    if arrow_table.schema.metadata:
        metadata = arrow_table.schema.metadata
        if b'dataset_name' in metadata:
            schema_validation["schema_info"]["has_dataset_metadata"] = True
        else:
            schema_validation["issues"].append("Missing dataset metadata")
    
    print(f"   ✅ Schema validation: {'passed' if schema_validation['schema_valid'] else 'failed'}")
    return schema_validation


def _validate_geometries(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """Validate geometry column and individual geometries."""
    geometry_validation = {
        "total_count": len(gdf),
        "valid_count": 0,
        "invalid_count": 0,
        "null_count": 0,
        "geometry_types": {}
    }
    
    if 'geometry' in gdf.columns:
        # Count null geometries
        null_mask = gdf.geometry.isnull()
        geometry_validation["null_count"] = null_mask.sum()
        
        # Count valid geometries (excluding nulls)
        non_null_gdf = gdf[~null_mask]
        if len(non_null_gdf) > 0:
            valid_mask = non_null_gdf.geometry.is_valid
            geometry_validation["valid_count"] = valid_mask.sum()
            geometry_validation["invalid_count"] = len(non_null_gdf) - valid_mask.sum()
            
            # Get geometry types
            geom_types = non_null_gdf.geometry.geom_type.value_counts()
            geometry_validation["geometry_types"] = dict(geom_types)
    
    return geometry_validation


def _validate_completeness(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """Validate data completeness."""
    completeness_validation = {
        "total_rows": len(gdf),
        "missing_geometry_count": 0,
        "column_completeness": {}
    }
    
    # Check geometry completeness
    if 'geometry' in gdf.columns:
        completeness_validation["missing_geometry_count"] = gdf.geometry.isnull().sum()
    
    # Check completeness of key columns
    key_columns = ['dataset_name', 'doi', 'geometry_wkt']
    for col in key_columns:
        if col in gdf.columns:
            missing_count = gdf[col].isnull().sum()
            completeness_validation["column_completeness"][col] = {
                "missing_count": missing_count,
                "completeness_rate": 1.0 - (missing_count / len(gdf))
            }
    
    return completeness_validation


def _validate_spatial_characteristics(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """Validate spatial characteristics and coordinate ranges."""
    spatial_validation = {
        "suspicious_coordinates": False,
        "coordinate_ranges": {},
        "crs_info": str(gdf.crs) if gdf.crs else "unknown"
    }
    
    # Check coordinate ranges for WGS84
    if gdf.crs and 'EPSG:4326' in str(gdf.crs):
        if 'centroid_lon' in gdf.columns and 'centroid_lat' in gdf.columns:
            lon_range = (gdf['centroid_lon'].min(), gdf['centroid_lon'].max())
            lat_range = (gdf['centroid_lat'].min(), gdf['centroid_lat'].max())
            
            spatial_validation["coordinate_ranges"] = {
                "longitude": lon_range,
                "latitude": lat_range
            }
            
            # Check for suspicious coordinates
            if (lon_range[0] < -180 or lon_range[1] > 180 or 
                lat_range[0] < -90 or lat_range[1] > 90):
                spatial_validation["suspicious_coordinates"] = True
    
    return spatial_validation
