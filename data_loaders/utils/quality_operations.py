"""
Quality operations utility module.

This module contains utility functions for calculating data quality metrics,
generating quality scores, and providing quality recommendations.

These are utility functions (not Hamilton nodes) that implement the actual
quality assessment logic called by Hamilton nodes.
"""

import pandas as pd
import geopandas as gpd
from typing import Dict, Any, List


def calculate_quality_metrics(
    gdf: gpd.GeoDataFrame,
    validation_result: Dict[str, Any],
    dataset_name: str
) -> Dict[str, Any]:
    """
    Calculate comprehensive quality metrics for the dataset.
    
    Args:
        gdf: GeoDataFrame to analyze
        validation_result: Results from geospatial validation
        dataset_name: Name of the dataset for reporting
        
    Returns:
        Dict with quality metrics and overall score
    """
    print(f"📊 Calculating quality metrics for {dataset_name}")
    
    quality_metrics = {
        "dataset_name": dataset_name,
        "timestamp": pd.Timestamp.now().isoformat(),
        "overall_score": 0.0,
        "completeness": {},
        "spatial_metrics": {},
        "data_consistency": {},
        "recommendations": []
    }
    
    # Calculate completeness metrics
    completeness_metrics = _calculate_completeness_metrics(gdf)
    quality_metrics["completeness"] = completeness_metrics
    
    # Calculate spatial quality metrics
    spatial_metrics = _calculate_spatial_quality_metrics(gdf, validation_result)
    quality_metrics["spatial_metrics"] = spatial_metrics
    
    # Calculate data consistency metrics
    consistency_metrics = _calculate_consistency_metrics(gdf)
    quality_metrics["data_consistency"] = consistency_metrics
    
    # Calculate overall quality score
    overall_score = _calculate_overall_quality_score(
        completeness_metrics, spatial_metrics, consistency_metrics
    )
    quality_metrics["overall_score"] = overall_score
    
    # Generate recommendations
    recommendations = _generate_quality_recommendations(
        completeness_metrics, spatial_metrics, consistency_metrics, overall_score
    )
    quality_metrics["recommendations"] = recommendations
    
    print(f"   ✅ Quality score: {overall_score:.2f}")
    return quality_metrics


def _calculate_completeness_metrics(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """Calculate data completeness metrics."""
    total_rows = len(gdf)
    
    completeness_metrics = {
        "total_records": total_rows,
        "column_completeness": {},
        "overall_completeness": 0.0
    }
    
    # Check completeness for key columns
    key_columns = ['geometry', 'dataset_name', 'doi', 'geometry_wkt']
    completeness_scores = []
    
    for col in key_columns:
        if col in gdf.columns:
            non_null_count = gdf[col].notna().sum()
            completeness_rate = non_null_count / total_rows if total_rows > 0 else 0.0
            completeness_metrics["column_completeness"][col] = {
                "non_null_count": non_null_count,
                "completeness_rate": completeness_rate
            }
            completeness_scores.append(completeness_rate)
    
    # Calculate overall completeness
    if completeness_scores:
        completeness_metrics["overall_completeness"] = sum(completeness_scores) / len(completeness_scores)
    
    return completeness_metrics


def _calculate_spatial_quality_metrics(
    gdf: gpd.GeoDataFrame, 
    validation_result: Dict[str, Any]
) -> Dict[str, Any]:
    """Calculate spatial quality metrics."""
    spatial_metrics = {
        "geometry_validity_rate": 0.0,
        "coordinate_quality": {},
        "spatial_distribution": {},
        "area_statistics": {}
    }
    
    # Geometry validity rate
    geometry_validation = validation_result.get("geometry_validity", {})
    total_count = geometry_validation.get("total_count", 0)
    valid_count = geometry_validation.get("valid_count", 0)
    
    if total_count > 0:
        spatial_metrics["geometry_validity_rate"] = valid_count / total_count
    
    # Coordinate quality (for WGS84 data)
    if 'centroid_lon' in gdf.columns and 'centroid_lat' in gdf.columns:
        coord_quality = _assess_coordinate_quality(gdf)
        spatial_metrics["coordinate_quality"] = coord_quality
    
    # Spatial distribution metrics
    if 'area_m2' in gdf.columns:
        area_stats = _calculate_area_statistics(gdf)
        spatial_metrics["area_statistics"] = area_stats
    
    return spatial_metrics


def _calculate_consistency_metrics(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """Calculate data consistency metrics."""
    consistency_metrics = {
        "duplicate_rate": 0.0,
        "metadata_consistency": {},
        "schema_consistency": {}
    }
    
    # Calculate duplicate rate
    total_rows = len(gdf)
    unique_rows = len(gdf.drop_duplicates())
    if total_rows > 0:
        consistency_metrics["duplicate_rate"] = 1.0 - (unique_rows / total_rows)
    
    # Check metadata consistency
    metadata_columns = ['dataset_name', 'doi', 'repository_type']
    metadata_consistency = {}
    
    for col in metadata_columns:
        if col in gdf.columns:
            unique_values = gdf[col].nunique()
            # For metadata columns, we expect consistent values (usually 1 unique value)
            metadata_consistency[col] = {
                "unique_count": unique_values,
                "is_consistent": unique_values <= 1
            }
    
    consistency_metrics["metadata_consistency"] = metadata_consistency
    
    return consistency_metrics


def _calculate_overall_quality_score(
    completeness_metrics: Dict[str, Any],
    spatial_metrics: Dict[str, Any],
    consistency_metrics: Dict[str, Any]
) -> float:
    """Calculate overall quality score (0.0 to 1.0)."""
    scores = []
    
    # Completeness score (weight: 0.4)
    completeness_score = completeness_metrics.get("overall_completeness", 0.0)
    scores.append(completeness_score * 0.4)
    
    # Spatial quality score (weight: 0.4)
    geometry_validity = spatial_metrics.get("geometry_validity_rate", 0.0)
    coord_quality = spatial_metrics.get("coordinate_quality", {}).get("quality_score", 1.0)
    spatial_score = (geometry_validity + coord_quality) / 2
    scores.append(spatial_score * 0.4)
    
    # Consistency score (weight: 0.2)
    duplicate_penalty = consistency_metrics.get("duplicate_rate", 0.0)
    consistency_score = max(0.0, 1.0 - duplicate_penalty)
    scores.append(consistency_score * 0.2)
    
    return sum(scores)


def _assess_coordinate_quality(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """Assess coordinate quality for WGS84 data."""
    coord_quality = {
        "quality_score": 1.0,
        "issues": [],
        "coordinate_ranges": {}
    }
    
    lon_col = 'centroid_lon'
    lat_col = 'centroid_lat'
    
    if lon_col in gdf.columns and lat_col in gdf.columns:
        lon_values = gdf[lon_col].dropna()
        lat_values = gdf[lat_col].dropna()
        
        if len(lon_values) > 0 and len(lat_values) > 0:
            lon_range = (lon_values.min(), lon_values.max())
            lat_range = (lat_values.min(), lat_values.max())
            
            coord_quality["coordinate_ranges"] = {
                "longitude": lon_range,
                "latitude": lat_range
            }
            
            # Check for invalid coordinate ranges
            quality_score = 1.0
            issues = []
            
            if lon_range[0] < -180 or lon_range[1] > 180:
                issues.append("Longitude values outside valid range (-180, 180)")
                quality_score -= 0.3
                
            if lat_range[0] < -90 or lat_range[1] > 90:
                issues.append("Latitude values outside valid range (-90, 90)")
                quality_score -= 0.3
                
            # Check for suspicious clustering at (0,0)
            zero_coords = ((lon_values == 0) & (lat_values == 0)).sum()
            if zero_coords > len(lon_values) * 0.1:  # More than 10% at (0,0)
                issues.append(f"Suspicious clustering at (0,0): {zero_coords} records")
                quality_score -= 0.2
            
            coord_quality["quality_score"] = max(0.0, quality_score)
            coord_quality["issues"] = issues
    
    return coord_quality


def _calculate_area_statistics(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """Calculate area statistics for polygon geometries."""
    area_stats = {}
    
    if 'area_m2' in gdf.columns:
        areas = gdf['area_m2'].dropna()
        areas = areas[areas > 0]  # Exclude zero areas
        
        if len(areas) > 0:
            area_stats = {
                "count": len(areas),
                "mean_area_m2": areas.mean(),
                "median_area_m2": areas.median(),
                "min_area_m2": areas.min(),
                "max_area_m2": areas.max(),
                "std_area_m2": areas.std()
            }
    
    return area_stats


def _generate_quality_recommendations(
    completeness_metrics: Dict[str, Any],
    spatial_metrics: Dict[str, Any],
    consistency_metrics: Dict[str, Any],
    overall_score: float
) -> List[str]:
    """Generate quality improvement recommendations."""
    recommendations = []
    
    # Overall score recommendations
    if overall_score < 0.7:
        recommendations.append("Dataset quality is below recommended threshold (0.7)")
    
    # Completeness recommendations
    overall_completeness = completeness_metrics.get("overall_completeness", 1.0)
    if overall_completeness < 0.9:
        recommendations.append("Improve data completeness - some key fields have missing values")
    
    # Spatial quality recommendations
    geometry_validity = spatial_metrics.get("geometry_validity_rate", 1.0)
    if geometry_validity < 0.95:
        recommendations.append("Fix invalid geometries to improve spatial data quality")
    
    coord_issues = spatial_metrics.get("coordinate_quality", {}).get("issues", [])
    if coord_issues:
        recommendations.append("Review coordinate values - found suspicious coordinate patterns")
    
    # Consistency recommendations
    duplicate_rate = consistency_metrics.get("duplicate_rate", 0.0)
    if duplicate_rate > 0.05:
        recommendations.append("Remove duplicate records to improve data consistency")
    
    if not recommendations:
        recommendations.append("Data quality is good - no major issues detected")
    
    return recommendations
