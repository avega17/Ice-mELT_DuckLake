"""
Hamilton dataflow for PV locations schema standardization.

Phase 1: PV Data Staging & Pre-processing
Task 1.1: std_schema

Objective: Create Hamilton functions to standardize field schemas across PV datasets
Key Functions:
- dataset_field_mapping() - Define field mappings per dataset from DOI manifest
- standardized_schema() - Apply consistent field names and types
- curated_fields() - Select only relevant fields for analysis
Output: Standardized GeoDataFrames with consistent schema

References:
- DOI manifest structure in data_loaders/doi_manifest.json
- Existing Hamilton patterns from dataflows/doi_pv_locations.py
- geoarrow-rs integration from _doi_pv_helpers_storage.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import warnings

import geopandas as gpd
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv

# Import geoarrow-rs for efficient spatial operations
import geoarrow.rust.core as geoarrow

# Load environment variables
load_dotenv()

# Get repo root and manifest path from environment or use defaults
# Use relative paths as they work better than absolute paths in this environment
REPO_ROOT = str(Path(__file__).parent.parent)
INGEST_METADATA = str(Path(__file__).parent.parent / "data_loaders" / "doi_manifest.json")

from hamilton.function_modifiers import cache, tag, config
from hamilton.htypes import Parallelizable, Collect

# Import storage helper functions
from ._doi_pv_helpers_storage import _geoarrow_table


# =============================================================================
# SCHEMA STANDARDIZATION CONFIGURATION
# =============================================================================

# Unified PV schema based on consolidation approach from utils/fetch_and_preprocess.py
UNIFIED_PV_SCHEMA = {
    # Core identification (following unified_id pattern)
    'unified_id': 'string',        # SHA256 hash of coalesced ID fields + dataset name
    'dataset': 'string',           # Dataset source name
    'source_file': 'string',       # Original file name
    
    # Geometry and spatial metrics (following DuckDB spatial pattern)
    'geometry': 'geometry',        # Geometry column (WKB for DuckDB compatibility)
    'area_m2': 'float64',         # Planar area calculation
    'spheroid_area': 'float64',   # Spheroid area using ST_Area_Spheroid
    'centroid_lon': 'float64',    # Longitude of centroid
    'centroid_lat': 'float64',    # Latitude of centroid
    'bbox': 'object',             # Bounding box struct (xmin, ymin, xmax, ymax)
    
    # Spatial indexing (to be added in Phase 2)
    'h3_index': 'string',
    'h3_resolution': 'int64',
    
    # Optional fields that may be present in some datasets
    'capacity_mw': 'float64',
    'installation_type': 'string',
    'confidence_score': 'float64',
    'installation_year': 'int64',
    'data_quality': 'string',
    'validation_status': 'string',
    'last_updated': 'datetime64[ns]'
}

# Common ID field candidates for unified_id generation (following COALESCE pattern)
ID_FIELD_CANDIDATES = [
    'fid', 'sol_id', 'GID_0', 'polygon_id', 'unique_id', 'id', 'objectid',
    'feature_id', 'installation_id', 'pv_id', 'uid', 'gid'
]

# Required columns for consolidated schema (must be present or computed)
REQUIRED_CONSOLIDATED_COLUMNS = {
    'area_m2', 'centroid_lon', 'centroid_lat', 'bbox', 'geometry'
}

# Default values for common PV installation metadata
DEFAULT_PV_METADATA = {
    'installation_type': 'unknown',
    'data_quality': 'medium',
    'validation_status': 'unvalidated',
    'technology_type': 'unknown'
}


# =============================================================================
# HAMILTON DATAFLOW NODES - SCHEMA STANDARDIZATION
# =============================================================================

@cache(behavior="recompute")  # Always reload to catch manifest updates
@tag(stage="metadata", data_type="config", execution_mode="parallel")
@config.when(execution_mode="parallel")
def dataset_field_mapping__parallel(
    dataset_names: str,  # Individual dataset name from parallel processing
    doi_metadata: Dict[str, Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Define field mappings per dataset using cols_to_keep from DOI manifest.
    
    Uses the "cols_to_keep" field in the DOI manifest to determine which columns
    to retain from each dataset, along with any field renaming specified in the manifest.
    
    Args:
        dataset_name: Name of the dataset to get mapping for
        doi_metadata: DOI metadata dictionary (optional, will load if not provided)
    
    Returns:
        Dictionary containing columns to keep, field mappings, and default values
    """
    # Load DOI metadata if not provided
    if doi_metadata is None:
        manifest_file = Path(INGEST_METADATA)
        if not manifest_file.exists():
            raise FileNotFoundError(f"DOI manifest not found: {manifest_file}")
        
        with open(manifest_file, 'r') as f:
            doi_metadata = json.load(f)
    
    # Get dataset metadata
    if dataset_names not in doi_metadata:
        raise ValueError(f"Dataset '{dataset_names}' not found in DOI manifest")

    dataset_metadata = doi_metadata[dataset_names]
    
    # Extract cols_to_keep from manifest (default to keeping geometry only)
    cols_to_keep = dataset_metadata.get('cols_to_keep', ['geometry'])
    
    # Extract field mappings if specified (for renaming columns)
    field_mappings = dataset_metadata.get('field_mappings', {})
    
    # Create field mapping configuration
    field_mapping = {
        'dataset_name': dataset_names,
        'cols_to_keep': cols_to_keep,
        'field_mappings': field_mappings,
        'doi_metadata': dataset_metadata,
        'default_values': {
            'dataset_source': dataset_names,
            **DEFAULT_PV_METADATA
        }
    }

    # Add dataset-specific default values if specified in manifest
    if 'default_values' in dataset_metadata:
        field_mapping['default_values'].update(dataset_metadata['default_values'])

    # Add label count for validation
    if 'label_count' in dataset_metadata:
        field_mapping['expected_record_count'] = dataset_metadata['label_count']

    print(f"   📋 Field mapping configured for {dataset_names}")
    print(f"      - Columns to keep: {len(cols_to_keep)} fields: {cols_to_keep}")
    if field_mappings:
        print(f"      - Field mappings: {len(field_mappings)} renames: {field_mappings}")
    print(f"      - Default values: {len(field_mapping['default_values'])} fields")
    
    return field_mapping


@cache(behavior="recompute")  # Always reload to catch manifest updates
@tag(stage="metadata", data_type="config", execution_mode="sequential")
@config.when(execution_mode="sequential")
def dataset_field_mapping__sequential(
    dataset_names: List[str],  # List of dataset names for sequential processing
    doi_metadata: Dict[str, Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Placeholder for sequential mode - field mapping handled in processing functions.

    In sequential mode, field mapping is handled directly in the processing functions
    that iterate over the dataset list. This follows the working pattern from doi_pv_locations.py.
    """
    # Return empty mapping as this won't be used in sequential mode
    return {
        'dataset_name': '',
        'cols_to_keep': ['geometry'],
        'field_mappings': {},
        'doi_metadata': {},
        'default_values': {}
    }


@tag(stage="standardization", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames due to potential serialization issues
def standardized_schema(
    raw_geodataframe: gpd.GeoDataFrame,
    dataset_field_mapping: Dict[str, Any]
) -> gpd.GeoDataFrame:
    """
    Apply consistent field selection and standardization using cols_to_keep from manifest.
    
    Filters raw geodataframe to keep only specified columns, applies any field renaming,
    adds default values, and computes essential derived fields like centroids.
    
    Args:
        raw_geodataframe: Raw geodataframe from data loading
        dataset_field_mapping: Field mapping configuration from dataset_field_mapping()
    
    Returns:
        GeoDataFrame with standardized schema
    """
    if raw_geodataframe.empty:
        print(f"   ⚠️  Empty geodataframe for {dataset_field_mapping['dataset_name']}")
        return raw_geodataframe
    
    dataset_name = dataset_field_mapping['dataset_name']
    cols_to_keep = dataset_field_mapping['cols_to_keep']
    field_mappings = dataset_field_mapping['field_mappings']
    default_values = dataset_field_mapping['default_values']
    
    print(f"   🔄 Standardizing schema for {dataset_name}: {len(raw_geodataframe)} records")
    
    # Start with a copy of the raw data
    gdf = raw_geodataframe.copy()
    
    # Ensure geometry column exists
    if 'geometry' not in gdf.columns:
        raise ValueError(f"No geometry column found in {dataset_name}")
    
    # Ensure CRS is set (default to WGS84 if missing)
    if gdf.crs is None:
        gdf.set_crs("EPSG:4326", inplace=True)
        print(f"      - Set CRS to EPSG:4326 (was None)")
    
    # Convert to WGS84 for consistent processing
    if gdf.crs.to_epsg() != 4326:
        original_crs = gdf.crs.to_string()
        gdf = gdf.to_crs("EPSG:4326")
        print(f"      - Reprojected from {original_crs} to EPSG:4326")
    
    # Filter to keep only specified columns
    available_cols = [col for col in cols_to_keep if col in gdf.columns]
    missing_cols = [col for col in cols_to_keep if col not in gdf.columns]
    
    if missing_cols:
        print(f"      - Missing columns: {missing_cols}")
    
    gdf = gdf[available_cols].copy()
    print(f"      - Kept {len(available_cols)} columns: {available_cols}")
    
    # Apply field mappings (rename columns)
    for original_field, standard_field in field_mappings.items():
        if original_field in gdf.columns and original_field != standard_field:
            gdf.rename(columns={original_field: standard_field}, inplace=True)
            print(f"      - Renamed '{original_field}' -> '{standard_field}'")
    
    # Add default values for missing standard fields
    for field, value in default_values.items():
        if field not in gdf.columns:
            gdf[field] = value
            print(f"      - Added default '{field}': {value}")
    
    # Generate unified_id using COALESCE pattern from utils/fetch_and_preprocess.py
    gdf = _generate_unified_id(gdf, dataset_name)
    
    # Compute essential derived fields following consolidated schema
    _add_computed_fields(gdf, dataset_name)
    
    print(f"   ✅ Schema standardization complete: {len(gdf)} records, {len(gdf.columns)} columns")
    
    return gdf


@tag(stage="curation", data_type="geodataframe")
@cache(behavior="disable")  # Disable caching for GeoDataFrames
def curated_fields(
    standardized_geodataframe: gpd.GeoDataFrame,
    include_optional_fields: bool = True
) -> gpd.GeoDataFrame:
    """
    Select final curated fields for analysis.
    
    Since cols_to_keep already filters the main fields, this function primarily
    ensures we have the essential computed fields and removes any truly unnecessary columns.
    
    Args:
        standardized_geodataframe: Standardized geodataframe from standardized_schema()
        include_optional_fields: Whether to include optional fields
    
    Returns:
        GeoDataFrame with final curated field selection
    """
    if standardized_geodataframe.empty:
        return standardized_geodataframe
    
    # Essential fields that should always be present (following unified schema)
    essential_fields = [
        'unified_id',
        'dataset', 
        'geometry',
        'area_m2',
        'spheroid_area',
        'centroid_lon',
        'centroid_lat',
        'bbox'
    ]
    
    # Get all available fields
    available_fields = list(standardized_geodataframe.columns)
    
    # Ensure essential fields are present
    missing_essential = [f for f in essential_fields if f not in available_fields]
    if missing_essential:
        print(f"   ⚠️  Missing essential fields: {missing_essential}")
    
    # For now, keep all fields since cols_to_keep already did the main filtering
    curated_gdf = standardized_geodataframe.copy()
    
    print(f"   📋 Field curation complete:")
    print(f"      - Keeping all {len(available_fields)} standardized fields")
    print(f"      - Records: {len(curated_gdf)}")
    
    return curated_gdf


# =============================================================================
# HELPER FUNCTIONS (prefixed with _ to exclude from DAG visualization)
# =============================================================================

def _generate_unified_id(gdf: gpd.GeoDataFrame, dataset_name: str) -> gpd.GeoDataFrame:
    """
    Generate unified_id using COALESCE pattern from utils/fetch_and_preprocess.py.

    Creates SHA256 hash of coalesced ID fields + dataset name, following the pattern:
    sha256(concat_ws('_', COALESCE(id_fields...), dataset_name))

    Args:
        gdf: GeoDataFrame to modify
        dataset_name: Name of the dataset

    Returns:
        GeoDataFrame with unified_id column added
    """
    import hashlib

    # Find available ID field candidates
    available_id_fields = [field for field in ID_FIELD_CANDIDATES if field in gdf.columns]

    if available_id_fields:
        print(f"      - Found ID fields: {available_id_fields}")

        # Create coalesced ID using first non-null value from available fields
        def coalesce_ids(row):
            for field in available_id_fields:
                value = row[field]
                if pd.notna(value) and str(value).strip():
                    return str(value)
            # Fallback to row index if no valid ID found
            return f"NO_ID_{row.name}"

        coalesced_ids = gdf.apply(coalesce_ids, axis=1)
    else:
        print(f"      - No ID fields found, using row numbers")
        coalesced_ids = [f"NO_ID_{i}" for i in range(len(gdf))]

    # Generate unified_id using SHA256 hash
    unified_ids = []
    for coalesced_id in coalesced_ids:
        combined_id = f"{coalesced_id}_{dataset_name}"
        unified_id = hashlib.sha256(combined_id.encode()).hexdigest()
        unified_ids.append(unified_id)

    gdf['unified_id'] = unified_ids
    print(f"      - Generated unified_id for {len(gdf)} records")

    return gdf


def _add_computed_fields(gdf: gpd.GeoDataFrame, dataset_name: str) -> None:
    """
    Add essential computed fields following consolidated schema pattern.

    Args:
        gdf: GeoDataFrame to modify
        dataset_name: Name of the dataset
    """
    # Add dataset field (renamed from dataset_source)
    if 'dataset' not in gdf.columns:
        gdf['dataset'] = dataset_name
        print(f"      - Added dataset field: {dataset_name}")

    # Add centroid coordinates
    if 'centroid_lon' not in gdf.columns:
        gdf['centroid_lon'] = gdf.geometry.centroid.x
        print(f"      - Computed centroid_lon")

    if 'centroid_lat' not in gdf.columns:
        gdf['centroid_lat'] = gdf.geometry.centroid.y
        print(f"      - Computed centroid_lat")

    # Add planar area in square meters (approximate for WGS84)
    if 'area_m2' not in gdf.columns:
        try:
            gdf['area_m2'] = gdf.geometry.area * 111320 * 111320  # Rough conversion to m²
            print(f"      - Computed area_m2 (planar approximation)")
        except Exception as e:
            print(f"      - Failed to compute area_m2: {e}")
            gdf['area_m2'] = -1.0  # Use -1 to indicate invalid (following consolidation pattern)

    # Add spheroid area (will be computed properly in DuckDB with ST_Area_Spheroid)
    if 'spheroid_area' not in gdf.columns:
        gdf['spheroid_area'] = -1.0  # Placeholder, will be computed in DuckDB
        print(f"      - Added spheroid_area placeholder (to be computed in DuckDB)")

    # Add bounding box
    if 'bbox' not in gdf.columns:
        try:
            bounds = gdf.bounds
            bbox_structs = []
            for _, row in bounds.iterrows():
                bbox_structs.append({
                    'xmin': row['minx'],
                    'ymin': row['miny'],
                    'xmax': row['maxx'],
                    'ymax': row['maxy']
                })
            gdf['bbox'] = bbox_structs
            print(f"      - Computed bbox structures")
        except Exception as e:
            print(f"      - Failed to compute bbox: {e}")
            gdf['bbox'] = None

    # Add timestamp
    if 'last_updated' not in gdf.columns:
        gdf['last_updated'] = pd.Timestamp.now()
        print(f"      - Added last_updated timestamp")


def _validate_schema_compliance(gdf: gpd.GeoDataFrame, dataset_name: str) -> Dict[str, Any]:
    """
    Validate that geodataframe has essential fields and valid data.

    Args:
        gdf: GeoDataFrame to validate
        dataset_name: Name of the dataset for reporting

    Returns:
        Dictionary with validation results
    """
    validation_results = {
        'dataset_name': dataset_name,
        'record_count': len(gdf),
        'field_count': len(gdf.columns),
        'has_geometry': 'geometry' in gdf.columns,
        'has_crs': gdf.crs is not None if hasattr(gdf, 'crs') else False,
        'missing_essential_fields': [],
        'data_quality_issues': []
    }

    # Check for essential fields (following unified schema)
    essential_fields = ['unified_id', 'dataset', 'geometry', 'area_m2', 'centroid_lon', 'centroid_lat']
    for field in essential_fields:
        if field not in gdf.columns:
            validation_results['missing_essential_fields'].append(field)

    # Check for data quality issues
    if 'geometry' in gdf.columns:
        invalid_geom_count = gdf.geometry.isna().sum()
        if invalid_geom_count > 0:
            validation_results['data_quality_issues'].append(f"{invalid_geom_count} invalid geometries")

    return validation_results
