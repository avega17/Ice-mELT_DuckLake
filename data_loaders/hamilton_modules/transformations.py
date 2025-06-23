"""
Hamilton module for data transformation operations.

This module contains Hamilton nodes for:
- Processing geospatial files into standardized GeoDataFrames
- Converting GeoDataFrames to Arrow tables
- Applying geospatial transformations and calculations
- Standardizing coordinate reference systems

All functions follow Hamilton best practices:
- Noun-based naming (not verbs)
- Proper dependency injection
- Immutable outputs
- Clear type annotations
"""

from typing import List
import geopandas as gpd
import pyarrow as pa

from hamilton.function_modifiers import tag, cache


@cache(behavior="default", format="parquet")  # Cache expensive geospatial processing
@tag(
    data_source="doi",
    processing_stage="process",
    data_type="geodataframe",
    hamilton_node_type="transform",
    geospatial_operation="standardization",
    description="Processed and standardized GeoDataFrame with Hamilton caching"
)
def processed_geodataframe(
    geospatial_file_paths: List[str],
    target_datasets: str,
    dataset_metadata: dict
) -> gpd.GeoDataFrame:
    """
    Processed and standardized GeoDataFrame from geospatial files.

    This Hamilton node processes raw geospatial files into a standardized format
    with consistent CRS, metadata columns, and calculated fields.

    Args:
        geospatial_file_paths: File paths from geospatial_file_paths() function
        target_datasets: Dataset name from target_datasets() function
        dataset_metadata: Dataset metadata from dataset_metadata() function

    Returns:
        gpd.GeoDataFrame: Processed geospatial data with standardized schema
    """
    # Import processing utility function
    from ..utils.geospatial_operations import process_geospatial_files
    
    return process_geospatial_files(
        geospatial_files=geospatial_file_paths,
        dataset_name=target_datasets,
        dataset_metadata=dataset_metadata
    )


@cache(behavior="default", format="parquet")  # Cache Arrow table conversion
@tag(
    data_source="doi",
    processing_stage="convert",
    data_type="arrow_table",
    hamilton_node_type="transform",
    data_format="arrow",
    description="Arrow table with geospatial data and Hamilton caching"
)
def arrow_table_with_geometry(
    processed_geodataframe: gpd.GeoDataFrame,
    target_datasets: str
) -> pa.Table:
    """
    Arrow table with geospatial data using simplified conversion approach.

    Uses a simplified conversion strategy that gracefully handles different
    geometry formats without complex fallback chains.

    Args:
        processed_geodataframe: Processed GeoDataFrame from processed_geodataframe() function
        target_datasets: Dataset name from target_datasets() function

    Returns:
        pa.Table: Arrow table with geometry data and dataset metadata
    """
    # Import conversion utility function
    from ..utils.arrow_operations import convert_geodataframe_to_arrow

    return convert_geodataframe_to_arrow(
        gdf=processed_geodataframe,
        dataset_name=target_datasets
    )


@tag(
    data_source="doi",
    processing_stage="validate",
    data_type="validation_result",
    hamilton_node_type="transform",
    description="Geospatial data validation results"
)
def geospatial_validation_result(
    processed_geodataframe: gpd.GeoDataFrame,
    target_datasets: str
) -> dict:
    """
    Geospatial data validation results for quality assurance.

    Performs validation checks on the processed GeoDataFrame to ensure
    data quality and consistency before storage.

    Args:
        processed_geodataframe: Processed GeoDataFrame from processed_geodataframe() function
        target_datasets: Dataset name from target_datasets() function

    Returns:
        dict: Validation results with quality metrics and status
    """
    # Import validation utility function
    from ..utils.validation_operations import validate_geospatial_data
    
    return validate_geospatial_data(
        gdf=processed_geodataframe,
        dataset_name=target_datasets
    )


@tag(
    data_source="doi",
    processing_stage="quality",
    data_type="quality_metrics",
    hamilton_node_type="transform",
    description="Data quality metrics and statistics"
)
def data_quality_metrics(
    processed_geodataframe: gpd.GeoDataFrame,
    geospatial_validation_result: dict,
    target_datasets: str
) -> dict:
    """
    Data quality metrics and statistics for monitoring.

    Calculates comprehensive quality metrics for the processed dataset
    including geometry validity, completeness, and spatial characteristics.

    Args:
        processed_geodataframe: Processed GeoDataFrame from processed_geodataframe() function
        geospatial_validation_result: Validation results from geospatial_validation_result() function
        target_datasets: Dataset name from target_datasets() function

    Returns:
        dict: Quality metrics and statistics
    """
    # Import quality metrics utility function
    from ..utils.quality_operations import calculate_quality_metrics
    
    return calculate_quality_metrics(
        gdf=processed_geodataframe,
        validation_result=geospatial_validation_result,
        dataset_name=target_datasets
    )
