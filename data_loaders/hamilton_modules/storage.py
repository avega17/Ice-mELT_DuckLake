"""
Hamilton module for data storage operations.

This module contains Hamilton nodes for:
- Storing Arrow tables in DuckDB
- Managing database schemas and tables
- Collecting results from parallel processing
- Generating storage reports and summaries

All functions follow Hamilton best practices:
- Noun-based naming (not verbs)
- Proper dependency injection
- Immutable outputs
- Clear type annotations
"""

from typing import List, Dict, Any
import pyarrow as pa
from hamilton.function_modifiers import tag
from hamilton.htypes import Collect


@tag(
    data_source="doi",
    processing_stage="collect",
    data_type="arrow_tables",
    hamilton_node_type="collect",
    execution_mode="parallel",
    description="Collected Arrow tables from parallel processing"
)
def collected_arrow_tables(
    arrow_table_with_geometry: Collect[pa.Table]
) -> List[pa.Table]:
    """
    Collected Arrow tables from parallel processing execution.

    Hamilton automatically collects all Arrow tables produced by parallel
    processing of individual datasets using the Collect functionality.

    Args:
        arrow_table_with_geometry: Collection of Arrow tables from arrow_table_with_geometry() function

    Returns:
        List[pa.Table]: List of all processed Arrow tables ready for storage
    """
    # Hamilton automatically collects all Arrow tables from parallel processing
    return list(arrow_table_with_geometry)


@tag(
    data_source="doi",
    processing_stage="collect",
    data_type="processing_reports",
    hamilton_node_type="collect",
    execution_mode="parallel",
    description="Collected processing reports from parallel execution"
)
def collected_processing_reports(
    processing_report: Collect[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Collected processing reports from parallel processing execution.

    Collects all processing reports generated during parallel dataset processing
    for comprehensive monitoring and quality assurance.

    Args:
        processing_report: Collection of processing reports from processing_report() function

    Returns:
        List[Dict[str, Any]]: List of all processing reports
    """
    return list(processing_report)


@tag(
    data_source="doi",
    processing_stage="store",
    data_type="storage_result",
    hamilton_node_type="sink",
    io_operation="database_write",
    database_type="duckdb",
    description="DuckDB storage results for individual datasets"
)
def duckdb_storage_result(
    collected_arrow_tables: List[pa.Table],
    collected_processing_reports: List[Dict[str, Any]],
    database_path: str = "../db/eo_pv_data.duckdb"
) -> Dict[str, str]:
    """
    DuckDB storage results for individual datasets with validation.

    Stores each processed dataset individually in DuckDB while maintaining
    separate schemas. Only stores datasets that pass validation checks.

    Args:
        collected_arrow_tables: Arrow tables from collected_arrow_tables() function
        collected_processing_reports: Processing reports from collected_processing_reports() function
        database_path: Path to DuckDB database

    Returns:
        Dict[str, str]: Mapping of dataset names to storage status information
    """
    # Import storage utility function
    from ..utils.storage_operations import store_datasets_in_duckdb
    
    return store_datasets_in_duckdb(
        arrow_tables=collected_arrow_tables,
        processing_reports=collected_processing_reports,
        database_path=database_path
    )


@tag(
    data_source="doi",
    processing_stage="store",
    data_type="table_creation_result",
    hamilton_node_type="transform",
    io_operation="database_write",
    description="DuckDB table creation result"
)
def duckdb_table_creation_result(
    arrow_table_with_geometry: pa.Table,
    processing_report: Dict[str, Any],
    target_datasets: str,
    database_path: str = "../db/eo_pv_data.duckdb"
) -> Dict[str, Any]:
    """
    DuckDB table creation result for individual dataset.

    Creates a DuckDB table from an Arrow table following best practices
    for geospatial data storage. Only proceeds if validation passes.

    Args:
        arrow_table_with_geometry: Arrow table from arrow_table_with_geometry() function
        processing_report: Processing report from processing_report() function
        target_datasets: Dataset name from target_datasets() function
        database_path: Path to DuckDB database

    Returns:
        Dict[str, Any]: Table creation result with status and metadata
    """
    # Import table creation utility function
    from ..utils.storage_operations import create_duckdb_table_from_arrow_safe
    
    return create_duckdb_table_from_arrow_safe(
        arrow_table=arrow_table_with_geometry,
        processing_report=processing_report,
        dataset_name=target_datasets,
        database_path=database_path
    )


@tag(
    data_source="doi",
    processing_stage="report",
    data_type="pipeline_summary",
    hamilton_node_type="transform",
    description="Complete pipeline execution summary"
)
def pipeline_execution_summary(
    duckdb_storage_result: Dict[str, str],
    collected_processing_reports: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Complete pipeline execution summary with statistics and status.

    Generates a comprehensive summary of the entire pipeline execution
    including success rates, quality metrics, and storage results.

    Args:
        duckdb_storage_result: Storage results from duckdb_storage_result() function
        collected_processing_reports: Processing reports from collected_processing_reports() function

    Returns:
        Dict[str, Any]: Pipeline execution summary with comprehensive statistics
    """
    import pandas as pd
    
    total_datasets = len(collected_processing_reports)
    successful_storage = len([status for status in duckdb_storage_result.values() if "success" in status.lower()])
    
    # Calculate quality statistics
    quality_scores = [
        report.get("validation", {}).get("quality_score", 0.0)
        for report in collected_processing_reports
    ]
    
    validation_statuses = [
        report.get("validation", {}).get("validation_status", "unknown")
        for report in collected_processing_reports
    ]
    
    return {
        "pipeline_execution_timestamp": pd.Timestamp.now().isoformat(),
        "total_datasets_processed": total_datasets,
        "successful_storage_count": successful_storage,
        "storage_success_rate": successful_storage / total_datasets if total_datasets > 0 else 0.0,
        "average_quality_score": sum(quality_scores) / len(quality_scores) if quality_scores else 0.0,
        "validation_status_counts": {
            status: validation_statuses.count(status) 
            for status in set(validation_statuses)
        },
        "stored_tables": duckdb_storage_result,
        "pipeline_status": "success" if successful_storage == total_datasets else "partial_success",
        "recommendations": [
            "Review failed datasets for data quality issues" if successful_storage < total_datasets else "All datasets processed successfully",
            "Monitor quality scores for datasets below 0.8 threshold",
            "Consider implementing additional validation rules for improved quality"
        ]
    }
