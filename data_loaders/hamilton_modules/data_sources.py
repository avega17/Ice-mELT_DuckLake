"""
Hamilton module for data source operations.

This module contains Hamilton nodes for:
- Loading dataset metadata from DOI manifest
- Generating target datasets for processing
- Managing dataset download operations
- Extracting geospatial files from downloads

All functions follow Hamilton best practices:
- Noun-based naming (not verbs)
- Proper dependency injection
- Immutable outputs
- Clear type annotations
"""

import json
from pathlib import Path
from typing import Dict, Any, List

from hamilton.function_modifiers import tag, cache
from hamilton.htypes import Parallelizable


@cache(behavior="recompute")  # Always reload manifest to catch updates
@tag(
    data_source="doi",
    processing_stage="raw",
    data_type="metadata",
    hamilton_node_type="source",
    description="DOI dataset metadata configuration with Hamilton caching"
)
def dataset_metadata(manifest_path: str = "doi_manifest.json") -> Dict[str, Dict[str, Any]]:
    """
    DOI dataset metadata configuration from doi_manifest.json.

    Args:
        manifest_path: Path to doi_manifest.json file. Can be absolute or relative.
                      Default: "doi_manifest.json" (looks in data_loaders directory)

    Returns:
        Dict mapping dataset names to their metadata configurations.
        This function has no dependencies and serves as a root node in the DAG.
    """
    # Convert to Path object
    manifest_file_path = Path(manifest_path)

    # If it's a relative path, make it relative to the data_loaders directory
    if not manifest_file_path.is_absolute():
        # Get data_loaders directory (parent of hamilton_modules)
        data_loaders_dir = Path(__file__).parent.parent
        manifest_file_path = data_loaders_dir / manifest_path

    if not manifest_file_path.exists():
        raise FileNotFoundError(f"DOI manifest not found: {manifest_file_path}")

    print(f"   📍 Loading DOI manifest from: {manifest_file_path}")
    with open(manifest_file_path, 'r') as f:
        manifest = json.load(f)

    # Filter for vector datasets suitable for our pipeline
    vector_datasets = {}
    for dataset_name, metadata in manifest.items():
        # Filter out datasets from "sciencebase" repository
        if metadata.get("repo") == "sciencebase":
            print(f"   ℹ️  Skipping ScienceBase dataset: {dataset_name}")
            continue
        # Include datasets with geospatial vector formats
        if metadata.get("label_fmt") in ["geojson", "shp", "gpkg", "json"] and not metadata.get("has_imgs", False):
            vector_datasets[dataset_name] = metadata

    print(f"Loaded {len(vector_datasets)} vector datasets from manifest")
    return vector_datasets


@tag(
    data_source="doi",
    processing_stage="config",
    data_type="dataset_names",
    hamilton_node_type="generator",
    execution_mode="parallel",
    description="Dataset names for parallel processing"
)
def target_datasets(dataset_metadata: Dict[str, Dict[str, Any]]) -> Parallelizable[str]:
    """
    Dataset names for parallel processing using Hamilton's Parallelizable.

    Args:
        dataset_metadata: Dataset metadata from dataset_metadata() function

    Yields:
        str: Dataset names for parallel processing
    """
    available_datasets = list(dataset_metadata.keys())
    print(f"Processing {len(available_datasets)} datasets in parallel")
    for dataset_name in available_datasets:
        yield dataset_name


@tag(
    data_source="doi",
    processing_stage="config", 
    data_type="dataset_names",
    hamilton_node_type="generator",
    execution_mode="sequential",
    description="Dataset names for sequential processing"
)
def target_datasets_list(dataset_metadata: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Dataset names for sequential processing as a simple list.

    Args:
        dataset_metadata: Dataset metadata from dataset_metadata() function

    Returns:
        List[str]: Dataset names for sequential processing
    """
    available_datasets = list(dataset_metadata.keys())
    print(f"Processing {len(available_datasets)} datasets sequentially")
    return available_datasets


@cache(behavior="default", format="json")  # Cache download paths as JSON
@tag(
    data_source="doi",
    processing_stage="download",
    data_type="file_path",
    hamilton_node_type="transform",
    io_operation="download",
    description="Downloaded dataset file path with Hamilton caching"
)
def dataset_download_path(
    target_datasets: str,
    dataset_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250,
    force_download: bool = False
) -> str:
    """
    Downloaded dataset file path with Hamilton built-in caching.

    This Hamilton node uses Hamilton's built-in caching instead of custom cache logic.
    Hamilton automatically handles caching based on code version and input data versions.

    Args:
        target_datasets: Dataset name from target_datasets() function
        dataset_metadata: Dataset metadata from dataset_metadata() function
        max_mb: Maximum file size in MB
        force_download: Force re-download even if cached

    Returns:
        str: Path to downloaded/extracted files
    """
    # Import download utility function
    from ..utils.download_operations import download_doi_dataset_files_hamilton

    return download_doi_dataset_files_hamilton(
        dataset_name=target_datasets,
        dataset_metadata=dataset_metadata,
        max_mb=max_mb,
        force_download=force_download
    )


@tag(
    data_source="doi",
    processing_stage="extract",
    data_type="file_paths",
    hamilton_node_type="transform",
    io_operation="file_discovery",
    description="Geospatial file paths from downloaded dataset"
)
def geospatial_file_paths(
    dataset_download_path: str,
    target_datasets: str,
    dataset_metadata: Dict[str, Dict[str, Any]]
) -> List[str]:
    """
    Geospatial file paths extracted from downloaded dataset.

    Args:
        dataset_download_path: Path to downloaded files from dataset_download_path() function
        target_datasets: Dataset name from target_datasets() function
        dataset_metadata: Dataset metadata from dataset_metadata() function

    Returns:
        List[str]: Paths to geospatial files
    """
    # Import file extraction utility function
    from ..utils.file_operations import extract_geospatial_file_paths
    
    return extract_geospatial_file_paths(
        download_path=dataset_download_path,
        dataset_name=target_datasets,
        dataset_metadata=dataset_metadata
    )
