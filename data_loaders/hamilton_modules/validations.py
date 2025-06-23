"""
Hamilton module for data validation operations.

This module contains Hamilton nodes for:
- Validating geospatial data quality
- Checking data completeness and consistency
- Performing spatial validation checks
- Generating validation reports

All functions follow Hamilton best practices:
- Noun-based naming (not verbs)
- Proper dependency injection
- Immutable outputs
- Clear type annotations
"""

from typing import Dict, Any, List
import geopandas as gpd
import pyarrow as pa

from hamilton.function_modifiers import tag


@tag(
    data_source="doi",
    processing_stage="validate",
    data_type="validation_summary",
    hamilton_node_type="transform",
    description="Comprehensive validation summary for dataset"
)
def validation_summary(
    geospatial_validation_result: dict,
    data_quality_metrics: dict,
    target_datasets: str
) -> Dict[str, Any]:
    """
    Comprehensive validation summary combining all validation checks.

    Aggregates validation results and quality metrics into a comprehensive
    summary for monitoring and quality assurance reporting.

    Args:
        geospatial_validation_result: Validation results from geospatial_validation_result() function
        data_quality_metrics: Quality metrics from data_quality_metrics() function
        target_datasets: Dataset name from target_datasets() function

    Returns:
        Dict[str, Any]: Comprehensive validation summary with status and metrics
    """
    return {
        "dataset_name": target_datasets,
        "validation_status": geospatial_validation_result.get("status", "unknown"),
        "quality_score": data_quality_metrics.get("overall_score", 0.0),
        "geometry_validity": geospatial_validation_result.get("geometry_validity", {}),
        "completeness": data_quality_metrics.get("completeness", {}),
        "spatial_metrics": data_quality_metrics.get("spatial_metrics", {}),
        "validation_timestamp": geospatial_validation_result.get("timestamp"),
        "issues_found": geospatial_validation_result.get("issues", []),
        "recommendations": data_quality_metrics.get("recommendations", [])
    }


@tag(
    data_source="doi",
    processing_stage="validate",
    data_type="schema_validation",
    hamilton_node_type="transform",
    description="Schema validation results for Arrow table"
)
def arrow_schema_validation(
    arrow_table_with_geometry: pa.Table,
    target_datasets: str
) -> Dict[str, Any]:
    """
    Schema validation results for Arrow table format compliance.

    Validates that the Arrow table conforms to expected schema requirements
    for downstream processing and storage operations.

    Args:
        arrow_table_with_geometry: Arrow table from arrow_table_with_geometry() function
        target_datasets: Dataset name from target_datasets() function

    Returns:
        Dict[str, Any]: Schema validation results and compliance status
    """
    # Import schema validation utility function
    from ..utils.validation_operations import validate_arrow_schema
    
    return validate_arrow_schema(
        arrow_table=arrow_table_with_geometry,
        dataset_name=target_datasets
    )


@tag(
    data_source="doi",
    processing_stage="validate",
    data_type="data_lineage",
    hamilton_node_type="transform",
    description="Data lineage tracking information"
)
def data_lineage_info(
    target_datasets: str,
    dataset_metadata: Dict[str, Dict[str, Any]],
    validation_summary: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Data lineage tracking information for provenance and audit trails.

    Captures comprehensive lineage information including source metadata,
    processing steps, validation results, and transformation history.

    Args:
        target_datasets: Dataset name from target_datasets() function
        dataset_metadata: Dataset metadata from dataset_metadata() function
        validation_summary: Validation summary from validation_summary() function

    Returns:
        Dict[str, Any]: Data lineage information with provenance details
    """
    import pandas as pd
    
    source_metadata = dataset_metadata.get(target_datasets, {})
    
    return {
        "dataset_name": target_datasets,
        "source_doi": source_metadata.get("doi"),
        "source_repository": source_metadata.get("repo"),
        "source_format": source_metadata.get("label_fmt"),
        "processing_timestamp": pd.Timestamp.now().isoformat(),
        "processing_system": "hamilton_doi_pipeline",
        "validation_status": validation_summary.get("validation_status"),
        "quality_score": validation_summary.get("quality_score"),
        "transformation_steps": [
            "download_and_cache",
            "extract_geospatial_files", 
            "process_and_standardize",
            "convert_to_arrow",
            "validate_quality"
        ],
        "schema_version": "1.0.0",
        "pipeline_version": "refactored_hamilton_v1"
    }


@tag(
    data_source="doi",
    processing_stage="validate",
    data_type="processing_report",
    hamilton_node_type="transform",
    description="Complete processing report for dataset"
)
def processing_report(
    validation_summary: Dict[str, Any],
    arrow_schema_validation: Dict[str, Any],
    data_lineage_info: Dict[str, Any],
    target_datasets: str
) -> Dict[str, Any]:
    """
    Complete processing report combining all validation and lineage information.

    Generates a comprehensive report that can be used for monitoring,
    auditing, and quality assurance of the data processing pipeline.

    Args:
        validation_summary: Validation summary from validation_summary() function
        arrow_schema_validation: Schema validation from arrow_schema_validation() function
        data_lineage_info: Lineage information from data_lineage_info() function
        target_datasets: Dataset name from target_datasets() function

    Returns:
        Dict[str, Any]: Complete processing report with all validation and lineage data
    """
    return {
        "dataset_name": target_datasets,
        "report_type": "hamilton_processing_report",
        "report_version": "1.0.0",
        "validation": validation_summary,
        "schema_compliance": arrow_schema_validation,
        "lineage": data_lineage_info,
        "overall_status": "success" if validation_summary.get("validation_status") == "passed" else "warning",
        "ready_for_storage": arrow_schema_validation.get("schema_valid", False) and 
                           validation_summary.get("validation_status") == "passed"
    }
