"""
Hamilton dataflow for hive partitioning and country-based data export.

Phase 4: Spatial Optimization & Partitioning
Task 4.2: Hive Partitioning Module

Objective: Implement country-based directory hive partitioning of exported geoparquet files for performance and parallelization

Key Functions:
- exported_hive_partitions() - Export GeoParquet with country-based Hive partitioning
- optimized_partitioned_table() - Optimize file sizes and partition distribution

Implementation Notes:
- Move upstream to improve country-level parallel processing
- Export country-partitioned GeoParquet files for efficient querying (after Overture nodes)
- Use established country ISO codes from admin aggregates
"""

from __future__ import annotations

import json
import tempfile
import shutil
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import warnings

import geopandas as gpd
import pandas as pd
import ibis as ir
from hamilton.function_modifiers import tag, cache, config
from hamilton import driver
from hamilton.htypes import Parallelizable, Collect

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


@tag(stage="hive_partitioning", data_type="metadata")
@cache(behavior="disable")
def hive_partitioning_config(
    partitioning_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Configuration for hive partitioning operations.
    
    Args:
        partitioning_config: Optional partitioning configuration overrides
        
    Returns:
        Hive partitioning configuration dictionary
    """
    default_config = {
        "export_format": "geoparquet",  # geoparquet, parquet, both
        "partition_column": "country_iso",
        "output_base_path": "exports/partitioned",
        "file_size_optimization": {
            "target_file_size_mb": 100,
            "max_files_per_partition": 10,
            "compression": "snappy"
        },
        "metadata": {
            "include_partition_metadata": True,
            "include_spatial_metadata": True,
            "include_schema_metadata": True
        },
        "validation": {
            "validate_partitions": True,
            "check_file_sizes": True,
            "verify_country_coverage": True
        }
    }
    
    if partitioning_config:
        # Deep merge configuration
        for key, value in partitioning_config.items():
            if key in default_config and isinstance(value, dict):
                default_config[key].update(value)
            else:
                default_config[key] = value
    
    print(f"   📁 Hive partitioning config loaded:")
    print(f"      - Export format: {default_config['export_format']}")
    print(f"      - Partition column: {default_config['partition_column']}")
    print(f"      - Target file size: {default_config['file_size_optimization']['target_file_size_mb']}MB")
    print(f"      - Compression: {default_config['file_size_optimization']['compression']}")
    
    return default_config


@tag(stage="hive_partitioning", data_type="table", execution_mode="parallel")
@cache(behavior="disable")
@config.when(execution_mode="parallel")
def country_partition_exports__parallel(
    country_iso_codes: str,  # Individual country from parallel processing
    collected_pv_country_aggregates: ir.Table,
    spatial_optimization_summary: Dict[str, Any],
    hive_partitioning_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Export GeoParquet with country-based Hive partitioning for a single country (parallel processing).
    
    Creates country-specific partitioned exports for efficient querying and distributed processing.
    
    Args:
        country_iso_codes: Individual country ISO code from parallel processing
        collected_pv_country_aggregates: Country aggregates table
        spatial_optimization_summary: Summary of spatial optimizations applied
        hive_partitioning_config: Hive partitioning configuration
        
    Returns:
        Export metadata for the country partition
    """
    print(f"   📦 Exporting hive partition for country: {country_iso_codes}")
    
    # Get country-specific data from aggregates table
    country_data = collected_pv_country_aggregates.filter(
        collected_pv_country_aggregates.country_iso == country_iso_codes
    ).execute()
    
    if len(country_data) == 0:
        print(f"      ⚠️  No data found for country: {country_iso_codes}")
        return {
            "country_iso": country_iso_codes,
            "status": "no_data",
            "records": 0,
            "files_created": []
        }
    
    # Create country-specific export directory
    base_path = Path(hive_partitioning_config["output_base_path"])
    partition_column = hive_partitioning_config["partition_column"]
    country_path = base_path / f"{partition_column}={country_iso_codes}"
    country_path.mkdir(parents=True, exist_ok=True)
    
    print(f"      - Export path: {country_path}")
    print(f"      - Records to export: {len(country_data):,}")
    
    # Convert to GeoDataFrame for export
    gdf = _prepare_geodataframe_for_export(country_data, country_iso_codes)
    
    # Apply file size optimization
    file_chunks = _optimize_file_sizes(gdf, hive_partitioning_config)
    
    # Export file chunks
    files_created = []
    export_format = hive_partitioning_config["export_format"]
    compression = hive_partitioning_config["file_size_optimization"]["compression"]
    
    for i, chunk_gdf in enumerate(file_chunks):
        if export_format in ["geoparquet", "both"]:
            geoparquet_file = country_path / f"part_{i:04d}.geoparquet"
            chunk_gdf.to_parquet(
                geoparquet_file,
                compression=compression,
                index=False
            )
            files_created.append(str(geoparquet_file))
            print(f"        • Created: {geoparquet_file.name} ({len(chunk_gdf):,} records)")
        
        if export_format in ["parquet", "both"]:
            # Convert geometry to WKT for regular parquet
            chunk_df = chunk_gdf.copy()
            chunk_df['geometry_wkt'] = chunk_gdf.geometry.apply(lambda x: x.wkt if x else None)
            chunk_df = chunk_df.drop(columns=['geometry'])
            
            parquet_file = country_path / f"part_{i:04d}.parquet"
            chunk_df.to_parquet(
                parquet_file,
                compression=compression,
                index=False
            )
            files_created.append(str(parquet_file))
            print(f"        • Created: {parquet_file.name} ({len(chunk_df):,} records)")
    
    # Create partition metadata
    if hive_partitioning_config["metadata"]["include_partition_metadata"]:
        metadata = _create_partition_metadata(
            country_iso_codes, gdf, files_created, 
            spatial_optimization_summary, hive_partitioning_config
        )
        metadata_file = country_path / "_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        files_created.append(str(metadata_file))
    
    print(f"   ✅ Hive partition export complete for {country_iso_codes}")
    print(f"      - Files created: {len(files_created)}")
    print(f"      - Total records: {len(gdf):,}")
    
    return {
        "country_iso": country_iso_codes,
        "status": "success",
        "records": len(gdf),
        "files_created": files_created,
        "export_path": str(country_path)
    }


@tag(stage="hive_partitioning", data_type="table", execution_mode="sequential")
@cache(behavior="disable")
@config.when(execution_mode="sequential")
def country_partition_exports__sequential(
    collected_pv_country_aggregates: ir.Table,
    spatial_optimization_summary: Dict[str, Any],
    hive_partitioning_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Export GeoParquet with country-based Hive partitioning for all countries (sequential processing).
    
    Creates country-specific partitioned exports for efficient querying and distributed processing.
    
    Args:
        collected_pv_country_aggregates: Country aggregates table
        spatial_optimization_summary: Summary of spatial optimizations applied
        hive_partitioning_config: Hive partitioning configuration
        
    Returns:
        List of export metadata for all country partitions
    """
    print(f"   📦 Exporting hive partitions for all countries (sequential)...")
    
    # Get unique country ISO codes
    country_codes = collected_pv_country_aggregates.select("country_iso").distinct().execute()
    unique_countries = country_codes["country_iso"].tolist()
    
    print(f"      - Countries to process: {len(unique_countries)}")
    
    results = []
    for country_iso in unique_countries:
        result = country_partition_exports__parallel(
            country_iso, collected_pv_country_aggregates,
            spatial_optimization_summary, hive_partitioning_config
        )
        results.append(result)
    
    print(f"   ✅ Sequential hive partition export complete")
    print(f"      - Countries processed: {len(results)}")
    
    return results


def _prepare_geodataframe_for_export(country_data: pd.DataFrame, country_iso: str) -> gpd.GeoDataFrame:
    """
    Prepare country data as GeoDataFrame for export.
    
    Args:
        country_data: Country-specific data
        country_iso: Country ISO code
        
    Returns:
        GeoDataFrame ready for export
    """
    from shapely.geometry import Point
    from shapely import wkb, wkt
    
    # Handle different geometry formats
    if 'geometry' in country_data.columns:
        # Check if geometry is WKB, WKT, or already Shapely
        first_geom = country_data['geometry'].iloc[0] if len(country_data) > 0 else None
        
        if isinstance(first_geom, bytes):
            # WKB format
            country_data['geometry'] = country_data['geometry'].apply(lambda x: wkb.loads(x) if x else None)
        elif isinstance(first_geom, str):
            # WKT format
            country_data['geometry'] = country_data['geometry'].apply(lambda x: wkt.loads(x) if x else None)
        
        gdf = gpd.GeoDataFrame(country_data, geometry='geometry', crs='EPSG:4326')
    
    elif 'centroid_lon' in country_data.columns and 'centroid_lat' in country_data.columns:
        # Create Point geometries from centroids
        geometry = [Point(lon, lat) for lon, lat in zip(country_data['centroid_lon'], country_data['centroid_lat'])]
        gdf = gpd.GeoDataFrame(country_data, geometry=geometry, crs='EPSG:4326')
    
    else:
        raise ValueError(f"No suitable geometry column found for country: {country_iso}")
    
    return gdf


def _optimize_file_sizes(gdf: gpd.GeoDataFrame, config: Dict[str, Any]) -> List[gpd.GeoDataFrame]:
    """
    Optimize file sizes by chunking data appropriately.
    
    Args:
        gdf: GeoDataFrame to chunk
        config: Hive partitioning configuration
        
    Returns:
        List of GeoDataFrame chunks
    """
    target_size_mb = config["file_size_optimization"]["target_file_size_mb"]
    max_files = config["file_size_optimization"]["max_files_per_partition"]
    
    # Estimate memory usage per record (rough approximation)
    if len(gdf) > 0:
        sample_size = min(100, len(gdf))
        sample_memory = gdf.head(sample_size).memory_usage(deep=True).sum()
        avg_memory_per_record = sample_memory / sample_size
        
        # Estimate records per target file size
        target_bytes = target_size_mb * 1024 * 1024
        records_per_chunk = max(1, int(target_bytes / avg_memory_per_record))
        
        # Ensure we don't exceed max files per partition
        min_records_per_chunk = max(1, len(gdf) // max_files)
        records_per_chunk = max(records_per_chunk, min_records_per_chunk)
    else:
        records_per_chunk = 1000  # Default chunk size
    
    # Create chunks
    chunks = []
    for i in range(0, len(gdf), records_per_chunk):
        chunk = gdf.iloc[i:i + records_per_chunk].copy()
        chunks.append(chunk)
    
    return chunks


def _create_partition_metadata(
    country_iso: str,
    gdf: gpd.GeoDataFrame,
    files_created: List[str],
    spatial_summary: Dict[str, Any],
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create metadata for the partition.
    
    Args:
        country_iso: Country ISO code
        gdf: GeoDataFrame that was exported
        files_created: List of files created
        spatial_summary: Spatial optimization summary
        config: Hive partitioning configuration
        
    Returns:
        Partition metadata dictionary
    """
    metadata = {
        "partition_info": {
            "country_iso": country_iso,
            "partition_column": config["partition_column"],
            "total_records": len(gdf),
            "files_count": len(files_created),
            "export_format": config["export_format"]
        },
        "spatial_info": {
            "crs": str(gdf.crs) if hasattr(gdf, 'crs') else None,
            "bounds": list(gdf.total_bounds) if len(gdf) > 0 else None,
            "geometry_types": gdf.geometry.geom_type.value_counts().to_dict() if len(gdf) > 0 else {}
        },
        "files": [{"path": f, "size_bytes": os.path.getsize(f) if os.path.exists(f) else 0} for f in files_created],
        "spatial_optimizations": spatial_summary.get("optimizations_applied", []),
        "created_at": pd.Timestamp.now().isoformat()
    }
    
    return metadata


@tag(stage="hive_partitioning", data_type="table")
@cache(behavior="disable")
def partition_optimization_summary(
    country_partition_exports: List[Dict[str, Any]],
    hive_partitioning_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Optimize file sizes and partition distribution across all country partitions.

    Analyzes partition distribution and provides optimization recommendations
    for improved query performance and storage efficiency.

    Args:
        exported_hive_partitions: List of partition export results
        hive_partitioning_config: Hive partitioning configuration

    Returns:
        Optimization summary and recommendations
    """
    print(f"   🔧 Optimizing partitioned table distribution...")

    # Analyze partition statistics
    total_records = sum(p.get("records", 0) for p in country_partition_exports)
    total_files = sum(len(p.get("files_created", [])) for p in country_partition_exports)
    successful_partitions = [p for p in country_partition_exports if p.get("status") == "success"]

    print(f"      - Total partitions: {len(country_partition_exports)}")
    print(f"      - Successful partitions: {len(successful_partitions)}")
    print(f"      - Total records: {total_records:,}")
    print(f"      - Total files: {total_files}")

    # Calculate partition size distribution
    partition_sizes = [p.get("records", 0) for p in successful_partitions]
    if partition_sizes:
        avg_partition_size = sum(partition_sizes) / len(partition_sizes)
        min_partition_size = min(partition_sizes)
        max_partition_size = max(partition_sizes)

        print(f"      - Avg partition size: {avg_partition_size:.0f} records")
        print(f"      - Min/Max partition size: {min_partition_size}/{max_partition_size} records")

    # Calculate file size statistics
    all_files = []
    for partition in successful_partitions:
        for file_path in partition.get("files_created", []):
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                all_files.append({
                    "path": file_path,
                    "size_mb": file_size / (1024 * 1024),
                    "country": partition.get("country_iso")
                })

    if all_files:
        file_sizes_mb = [f["size_mb"] for f in all_files]
        avg_file_size = sum(file_sizes_mb) / len(file_sizes_mb)
        target_size = hive_partitioning_config["file_size_optimization"]["target_file_size_mb"]

        print(f"      - Avg file size: {avg_file_size:.1f}MB (target: {target_size}MB)")
        print(f"      - File size range: {min(file_sizes_mb):.1f}MB - {max(file_sizes_mb):.1f}MB")

    # Generate optimization recommendations
    recommendations = []

    if partition_sizes:
        # Check for partition skew
        size_ratio = max_partition_size / avg_partition_size if avg_partition_size > 0 else 1
        if size_ratio > 10:
            recommendations.append({
                "type": "partition_skew",
                "severity": "high",
                "message": f"High partition skew detected (ratio: {size_ratio:.1f}x). Consider sub-partitioning large countries."
            })
        elif size_ratio > 5:
            recommendations.append({
                "type": "partition_skew",
                "severity": "medium",
                "message": f"Moderate partition skew detected (ratio: {size_ratio:.1f}x). Monitor query performance."
            })

    if all_files:
        # Check file size optimization
        oversized_files = [f for f in all_files if f["size_mb"] > target_size * 1.5]
        undersized_files = [f for f in all_files if f["size_mb"] < target_size * 0.1]

        if oversized_files:
            recommendations.append({
                "type": "file_size",
                "severity": "medium",
                "message": f"{len(oversized_files)} files exceed target size. Consider increasing chunk size."
            })

        if len(undersized_files) > len(all_files) * 0.3:
            recommendations.append({
                "type": "file_size",
                "severity": "low",
                "message": f"{len(undersized_files)} files are undersized. Consider decreasing chunk size."
            })

    # Validate partitions if enabled
    validation_results = {}
    if hive_partitioning_config["validation"]["validate_partitions"]:
        validation_results = _validate_partitions(country_partition_exports, hive_partitioning_config)

    # Create optimization summary
    optimization_summary = {
        "partition_statistics": {
            "total_partitions": len(country_partition_exports),
            "successful_partitions": len(successful_partitions),
            "total_records": total_records,
            "total_files": total_files,
            "avg_partition_size": avg_partition_size if partition_sizes else 0,
            "partition_size_range": [min_partition_size, max_partition_size] if partition_sizes else [0, 0]
        },
        "file_statistics": {
            "total_files": len(all_files),
            "avg_file_size_mb": avg_file_size if all_files else 0,
            "file_size_range_mb": [min(file_sizes_mb), max(file_sizes_mb)] if file_sizes_mb else [0, 0],
            "target_file_size_mb": target_size
        },
        "recommendations": recommendations,
        "validation_results": validation_results,
        "export_paths": [p.get("export_path") for p in successful_partitions if p.get("export_path")],
        "configuration": hive_partitioning_config
    }

    print(f"   ✅ Partitioned table optimization complete")
    print(f"      - Recommendations generated: {len(recommendations)}")
    print(f"      - Validation checks: {len(validation_results)}")

    return optimization_summary


def _validate_partitions(partitions: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate partition integrity and coverage.

    Args:
        partitions: List of partition export results
        config: Hive partitioning configuration

    Returns:
        Validation results
    """
    validation = {
        "file_integrity": {"passed": 0, "failed": 0, "errors": []},
        "country_coverage": {"total_countries": 0, "missing_countries": []},
        "size_validation": {"within_limits": 0, "oversized": 0, "undersized": 0}
    }

    # Validate file integrity
    for partition in partitions:
        for file_path in partition.get("files_created", []):
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                validation["file_integrity"]["passed"] += 1
            else:
                validation["file_integrity"]["failed"] += 1
                validation["file_integrity"]["errors"].append(f"Missing or empty file: {file_path}")

    # Validate country coverage
    expected_countries = set(p.get("country_iso") for p in partitions if p.get("country_iso"))
    successful_countries = set(p.get("country_iso") for p in partitions if p.get("status") == "success")
    missing_countries = expected_countries - successful_countries

    validation["country_coverage"]["total_countries"] = len(expected_countries)
    validation["country_coverage"]["missing_countries"] = list(missing_countries)

    # Validate file sizes
    target_size = config["file_size_optimization"]["target_file_size_mb"]
    for partition in partitions:
        for file_path in partition.get("files_created", []):
            if os.path.exists(file_path):
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                if size_mb < target_size * 0.1:
                    validation["size_validation"]["undersized"] += 1
                elif size_mb > target_size * 2:
                    validation["size_validation"]["oversized"] += 1
                else:
                    validation["size_validation"]["within_limits"] += 1

    return validation
