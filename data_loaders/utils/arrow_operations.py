"""
Arrow operations utility module.

This module contains utility functions for converting GeoDataFrames to Arrow format
with simplified error handling and graceful failure patterns.

Replaces complex fallback chains with clear, maintainable conversion strategies.
"""

import pandas as pd
import geopandas as gpd
import pyarrow as pa
from typing import Optional


def convert_geodataframe_to_arrow(
    gdf: gpd.GeoDataFrame,
    dataset_name: str
) -> pa.Table:
    """
    Convert GeoDataFrame to Arrow table with simplified error handling.
    
    Uses a clear conversion strategy without complex fallback chains:
    1. Try GeoPandas built-in to_arrow() with WKB encoding
    2. If that fails, use WKT conversion approach
    3. If geometry conversion fails entirely, proceed without geometry
    
    Args:
        gdf: GeoDataFrame to convert
        dataset_name: Name of the dataset for metadata
        
    Returns:
        pa.Table: Arrow table with geometry data and dataset metadata
    """
    print(f"   📝 Converting {dataset_name} to Arrow format")
    
    # Strategy 1: Try GeoPandas built-in to_arrow() with WKB
    arrow_table = _try_geopandas_to_arrow(gdf, dataset_name)
    
    # Strategy 2: If that failed, try WKT conversion
    if arrow_table is None:
        arrow_table = _try_wkt_conversion(gdf, dataset_name)
    
    # Strategy 3: If geometry conversion failed, proceed without geometry
    if arrow_table is None:
        arrow_table = _convert_without_geometry(gdf, dataset_name)
    
    # Add dataset metadata
    metadata = {b'dataset_name': dataset_name.encode('utf-8')}
    arrow_table = arrow_table.replace_schema_metadata(metadata)
    print(f"   📝 Added dataset metadata: {dataset_name}")
    
    return arrow_table


def _try_geopandas_to_arrow(
    gdf: gpd.GeoDataFrame,
    dataset_name: str
) -> Optional[pa.Table]:
    """
    Try converting using GeoPandas built-in to_arrow() method.
    
    Returns None if conversion fails, allowing graceful fallback.
    """
    try:
        print(f"   🔄 Attempting GeoPandas to_arrow() with WKB encoding")
        
        # Use GeoPandas to_arrow() with WKB encoding
        geopandas_arrow = gdf.to_arrow(index=False, geometry_encoding='WKB')
        arrow_table = pa.table(geopandas_arrow)
        
        print(f"   ✅ GeoPandas conversion successful: {arrow_table.num_rows} rows, {len(arrow_table.columns)} columns")
        
        # Log geometry columns found
        geometry_columns = [col for col in arrow_table.column_names if 'geometry' in col.lower()]
        if geometry_columns:
            print(f"   🗺️  Geometry columns: {geometry_columns}")
        
        return arrow_table
        
    except Exception as geopandas_error:
        print(f"   ⚠️  GeoPandas to_arrow() failed: {geopandas_error}")
        return None


def _try_wkt_conversion(
    gdf: gpd.GeoDataFrame,
    dataset_name: str
) -> Optional[pa.Table]:
    """
    Try converting using WKT geometry representation.
    
    Returns None if conversion fails, allowing graceful fallback.
    """
    try:
        print(f"   🔄 Attempting WKT geometry conversion")
        
        # Create a copy for conversion
        arrow_df = gdf.copy()
        
        # Convert geometry to WKT if geometry column exists
        if 'geometry' in arrow_df.columns and hasattr(arrow_df, 'geometry'):
            # Only add WKT if not already present
            if 'geometry_wkt' not in arrow_df.columns:
                arrow_df['geometry_wkt'] = arrow_df['geometry'].to_wkt()
            
            # Remove the original geometry column
            arrow_df = arrow_df.drop(columns=['geometry'])
            
            print(f"   🗺️  Converted geometry to WKT format")
        
        # Convert to Arrow table
        arrow_table = pa.Table.from_pandas(arrow_df, preserve_index=False)
        
        print(f"   ✅ WKT conversion successful: {arrow_table.num_rows} rows, {len(arrow_table.columns)} columns")
        return arrow_table
        
    except Exception as wkt_error:
        print(f"   ⚠️  WKT conversion failed: {wkt_error}")
        return None


def _convert_without_geometry(
    gdf: gpd.GeoDataFrame,
    dataset_name: str
) -> pa.Table:
    """
    Convert DataFrame without geometry as final fallback.
    
    This should always succeed as it removes all geometry-related complexity.
    """
    print(f"   🔄 Converting without geometry (final fallback)")
    
    try:
        # Create a copy and remove all geometry-related columns
        df = gdf.copy()
        
        # Remove geometry columns
        geometry_columns = [col for col in df.columns if 'geometry' in col.lower()]
        if geometry_columns:
            df = df.drop(columns=geometry_columns)
            print(f"   ⚠️  Removed geometry columns: {geometry_columns}")
        
        # Convert to regular pandas DataFrame if it's still a GeoDataFrame
        if isinstance(df, gpd.GeoDataFrame):
            df = pd.DataFrame(df)
        
        # Convert to Arrow table
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        
        print(f"   ✅ Non-geometry conversion successful: {arrow_table.num_rows} rows, {len(arrow_table.columns)} columns")
        print(f"   ⚠️  Warning: Geometry data was lost during conversion for {dataset_name}")
        
        return arrow_table
        
    except Exception as final_error:
        # This should never happen, but if it does, we need to fail gracefully
        print(f"   ❌ Final fallback conversion failed: {final_error}")
        raise ValueError(f"Unable to convert {dataset_name} to Arrow format: {final_error}")


def validate_arrow_conversion(
    arrow_table: pa.Table,
    original_gdf: gpd.GeoDataFrame,
    dataset_name: str
) -> dict:
    """
    Validate Arrow conversion results and provide conversion summary.
    
    Args:
        arrow_table: Converted Arrow table
        original_gdf: Original GeoDataFrame
        dataset_name: Name of the dataset
        
    Returns:
        dict: Conversion validation results
    """
    validation_result = {
        "dataset_name": dataset_name,
        "conversion_successful": True,
        "row_count_match": False,
        "geometry_preserved": False,
        "issues": []
    }
    
    # Check row count preservation
    if arrow_table.num_rows == len(original_gdf):
        validation_result["row_count_match"] = True
    else:
        validation_result["issues"].append(
            f"Row count mismatch: {len(original_gdf)} -> {arrow_table.num_rows}"
        )
    
    # Check geometry preservation
    arrow_columns = arrow_table.column_names
    geometry_columns = [col for col in arrow_columns if 'geometry' in col.lower()]
    
    if geometry_columns:
        validation_result["geometry_preserved"] = True
        validation_result["geometry_columns"] = geometry_columns
    else:
        validation_result["issues"].append("Geometry data was not preserved in Arrow conversion")
    
    # Check for critical issues
    if validation_result["issues"]:
        print(f"   ⚠️  Conversion issues for {dataset_name}:")
        for issue in validation_result["issues"]:
            print(f"      - {issue}")
    
    return validation_result
