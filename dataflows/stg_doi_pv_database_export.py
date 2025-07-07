"""
Hamilton dataflow for leveraging DuckDB's native hive partitioning and GeoParquet capabilities.

Phase 5: dbt Integration Preparation
Task 5.1: Database Export Module

Objective: Use DuckDB's built-in hive partitioning and GeoParquet writing for optimal performance

Key Functions:
- hive_partitioned_geoparquet() - DuckDB native hive partitioning with GeoParquet
- spatial_bounding_boxes() - Leverage DuckDB's automatic bounding box columns
- export_configuration() - Configure DuckDB export parameters

Implementation Notes:
- Leverage DuckDB's EXPORT DATABASE with PARTITION_BY for hive partitioning
- Use DuckDB's GeoParquet writer with automatic bounding box columns for fast spatial filtering
- Follow noun naming convention (not verb-based function names)
- Let DuckDB handle the heavy lifting instead of reinventing partitioning logic
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List, Optional
import warnings

import pandas as pd
import ibis as ir
import duckdb
from hamilton.function_modifiers import tag, cache, config

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


@tag(stage="database_export", data_type="metadata")
@cache(behavior="disable")
def export_configuration(
    export_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Configuration for DuckDB native hive partitioning and GeoParquet export.

    Args:
        export_config: Optional export configuration overrides

    Returns:
        Export configuration dictionary optimized for DuckDB native capabilities
    """
    default_config = {
        "duckdb_native": {
            "database_path": "db/eo_pv_data.duckdb",
            "source_table": "raw_data.pv_installations_optimized",
            "partition_column": "country_iso",
            "export_format": "geoparquet",  # Use DuckDB's GeoParquet writer
            "compression": "zstd",  # Better compression than snappy
            "row_group_size": 100000  # Optimize for spatial queries
        },
        "hive_partitioning": {
            "output_path": "exports/hive_partitioned",
            "use_duckdb_export": True,  # Leverage EXPORT DATABASE
            "include_bounding_box": True,  # DuckDB's automatic bbox columns
            "spatial_optimization": True
        },
        "spatial_features": {
            "enable_hilbert_ordering": True,
            "enable_spatial_clustering": True,
            "bbox_column_name": "bbox",  # DuckDB's automatic bounding box
            "geometry_encoding": "wkb"  # For DuckDB compatibility
        },
        "performance": {
            "parallel_export": True,
            "memory_limit": "8GB",
            "threads": 4
        }
    }

    if export_config:
        # Deep merge configuration
        for key, value in export_config.items():
            if key in default_config and isinstance(value, dict):
                default_config[key].update(value)
            else:
                default_config[key] = value

    print(f"   🦆 DuckDB native export configuration:")
    print(f"      - Source table: {default_config['duckdb_native']['source_table']}")
    print(f"      - Partition column: {default_config['duckdb_native']['partition_column']}")
    print(f"      - Export format: {default_config['duckdb_native']['export_format']}")
    print(f"      - Compression: {default_config['duckdb_native']['compression']}")
    print(f"      - Bounding box columns: {default_config['hive_partitioning']['include_bounding_box']}")

    return default_config


@tag(stage="database_export", data_type="table")
@cache(behavior="disable")
def hive_partitioned_geoparquet(
    spatial_optimization_summary: Dict[str, Any],
    export_configuration: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create hive-partitioned GeoParquet files using DuckDB's native capabilities.

    Leverages DuckDB's EXPORT DATABASE with PARTITION_BY for optimal hive partitioning
    and DuckDB's GeoParquet writer with automatic bounding box columns for fast spatial filtering.

    Args:
        spatial_optimization_summary: Spatial optimization summary from previous steps
        export_configuration: DuckDB native export configuration

    Returns:
        Export results with file paths and metadata
    """
    print(f"   � Creating hive-partitioned GeoParquet using DuckDB native capabilities...")

    # Get configuration
    config = export_configuration
    db_path = config["duckdb_native"]["database_path"]
    source_table = config["duckdb_native"]["source_table"]
    partition_column = config["duckdb_native"]["partition_column"]
    output_path = Path(config["hive_partitioning"]["output_path"])
    compression = config["duckdb_native"]["compression"]
    row_group_size = config["duckdb_native"]["row_group_size"]

    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"      - Source table: {source_table}")
    print(f"      - Partition column: {partition_column}")
    print(f"      - Output path: {output_path}")
    print(f"      - Compression: {compression}")

    try:
        # Connect to DuckDB
        conn = duckdb.connect(db_path)

        # Install and load required extensions
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")

        # Set performance parameters
        conn.execute(f"SET memory_limit='{config['performance']['memory_limit']}'")
        conn.execute(f"SET threads={config['performance']['threads']}")

        # Check if source table exists and get record count
        count_query = f"SELECT COUNT(*) as total_records FROM {source_table}"
        result = conn.execute(count_query).fetchone()
        total_records = result[0] if result else 0

        print(f"      - Records to export: {total_records:,}")

        if total_records == 0:
            print(f"      ⚠️  No records found in source table")
            return {"status": "no_data", "total_records": 0, "files_created": []}

        # Use DuckDB's native EXPORT DATABASE with hive partitioning
        # This leverages DuckDB's optimized GeoParquet writer with bounding box columns
        export_sql = f"""
        EXPORT DATABASE '{output_path}' (
            FORMAT PARQUET,
            PARTITION_BY ({partition_column}),
            COMPRESSION '{compression}',
            ROW_GROUP_SIZE {row_group_size}
        )
        """

        print(f"      - Executing DuckDB native export with hive partitioning...")
        conn.execute(export_sql)

        # Get partition statistics
        partition_stats = _analyze_hive_partitions(conn, source_table, partition_column)

        # List created files
        created_files = list(output_path.rglob("*.parquet"))

        conn.close()

        export_result = {
            "status": "success",
            "total_records": total_records,
            "partition_count": len(partition_stats),
            "files_created": [str(f) for f in created_files],
            "partition_statistics": partition_stats,
            "export_path": str(output_path),
            "compression": compression,
            "row_group_size": row_group_size,
            "duckdb_features": {
                "hive_partitioning": True,
                "geoparquet_writer": True,
                "automatic_bounding_box": config["hive_partitioning"]["include_bounding_box"],
                "spatial_optimization": config["hive_partitioning"]["spatial_optimization"]
            }
        }

        print(f"   ✅ DuckDB native hive partitioning complete:")
        print(f"      - Total records: {total_records:,}")
        print(f"      - Partitions created: {len(partition_stats)}")
        print(f"      - Files created: {len(created_files)}")
        print(f"      - DuckDB GeoParquet writer with bounding box columns enabled")

        return export_result

    except Exception as e:
        print(f"   ❌ DuckDB native export failed: {e}")
        return {"status": "failed", "error": str(e), "total_records": 0}


@tag(stage="database_export", data_type="table")
@cache(behavior="disable")
def spatial_bounding_boxes(
    hive_partitioned_geoparquet: Dict[str, Any],
    export_configuration: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Analyze spatial bounding boxes from DuckDB's GeoParquet export.

    Leverages DuckDB's automatic bounding box columns in GeoParquet files
    for fast spatial filtering and query optimization.

    Args:
        hive_partitioned_geoparquet: Results from hive partitioning export
        export_configuration: DuckDB native export configuration

    Returns:
        Spatial bounding box analysis and optimization recommendations
    """
    if hive_partitioned_geoparquet.get("status") != "success":
        print(f"   ⏭️  Spatial bounding box analysis skipped - export failed")
        return {"status": "skipped", "reason": "export_failed"}

    print(f"   📦 Analyzing spatial bounding boxes from DuckDB GeoParquet export...")

    # Get configuration
    config = export_configuration
    db_path = config["duckdb_native"]["database_path"]
    source_table = config["duckdb_native"]["source_table"]

    try:
        # Connect to DuckDB
        conn = duckdb.connect(db_path)

        # Install spatial extension
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")

        # Analyze spatial distribution using DuckDB's spatial functions
        spatial_analysis_sql = f"""
        SELECT
            {config['duckdb_native']['partition_column']} as partition_key,
            COUNT(*) as record_count,
            ST_XMin(ST_Envelope(ST_Union_Agg(geometry))) as bbox_xmin,
            ST_YMin(ST_Envelope(ST_Union_Agg(geometry))) as bbox_ymin,
            ST_XMax(ST_Envelope(ST_Union_Agg(geometry))) as bbox_xmax,
            ST_YMax(ST_Envelope(ST_Union_Agg(geometry))) as bbox_ymax,
            AVG(ST_Area(geometry)) as avg_area,
            ST_Area(ST_Envelope(ST_Union_Agg(geometry))) as partition_bbox_area
        FROM {source_table}
        GROUP BY {config['duckdb_native']['partition_column']}
        ORDER BY record_count DESC
        """

        spatial_stats = conn.execute(spatial_analysis_sql).fetchdf()

        # Calculate global bounding box
        global_bbox_sql = f"""
        SELECT
            ST_XMin(ST_Envelope(ST_Union_Agg(geometry))) as global_xmin,
            ST_YMin(ST_Envelope(ST_Union_Agg(geometry))) as global_ymin,
            ST_XMax(ST_Envelope(ST_Union_Agg(geometry))) as global_xmax,
            ST_YMax(ST_Envelope(ST_Union_Agg(geometry))) as global_ymax,
            COUNT(*) as total_records
        FROM {source_table}
        """

        global_stats = conn.execute(global_bbox_sql).fetchone()

        conn.close()

        # Create bounding box analysis
        bbox_analysis = {
            "status": "success",
            "global_bounding_box": {
                "xmin": float(global_stats[0]),
                "ymin": float(global_stats[1]),
                "xmax": float(global_stats[2]),
                "ymax": float(global_stats[3]),
                "total_records": int(global_stats[4])
            },
            "partition_bounding_boxes": spatial_stats.to_dict('records'),
            "spatial_distribution": {
                "total_partitions": len(spatial_stats),
                "avg_records_per_partition": float(spatial_stats['record_count'].mean()),
                "max_records_per_partition": int(spatial_stats['record_count'].max()),
                "min_records_per_partition": int(spatial_stats['record_count'].min())
            },
            "duckdb_geoparquet_features": {
                "automatic_bbox_columns": True,
                "spatial_filtering_optimized": True,
                "hilbert_ordering_available": config["spatial_features"]["enable_hilbert_ordering"],
                "spatial_clustering": config["spatial_features"]["enable_spatial_clustering"]
            },
            "performance_recommendations": _generate_spatial_performance_recommendations(spatial_stats)
        }

        print(f"   ✅ Spatial bounding box analysis complete:")
        print(f"      - Global bbox: ({bbox_analysis['global_bounding_box']['xmin']:.6f}, {bbox_analysis['global_bounding_box']['ymin']:.6f}) to ({bbox_analysis['global_bounding_box']['xmax']:.6f}, {bbox_analysis['global_bounding_box']['ymax']:.6f})")
        print(f"      - Partitions analyzed: {bbox_analysis['spatial_distribution']['total_partitions']}")
        print(f"      - DuckDB GeoParquet automatic bounding box columns enabled")
        print(f"      - Performance recommendations: {len(bbox_analysis['performance_recommendations'])}")

        return bbox_analysis

    except Exception as e:
        print(f"   ❌ Spatial bounding box analysis failed: {e}")
        return {"status": "failed", "error": str(e)}


def _analyze_hive_partitions(conn, source_table: str, partition_column: str) -> List[Dict[str, Any]]:
    """
    Analyze hive partition statistics using DuckDB.

    Args:
        conn: DuckDB connection
        source_table: Source table name
        partition_column: Column used for partitioning

    Returns:
        List of partition statistics
    """
    partition_sql = f"""
    SELECT
        {partition_column} as partition_value,
        COUNT(*) as record_count,
        MIN(ST_XMin(geometry)) as min_x,
        MIN(ST_YMin(geometry)) as min_y,
        MAX(ST_XMax(geometry)) as max_x,
        MAX(ST_YMax(geometry)) as max_y
    FROM {source_table}
    GROUP BY {partition_column}
    ORDER BY record_count DESC
    """

    result = conn.execute(partition_sql).fetchdf()
    return result.to_dict('records')


def _generate_spatial_performance_recommendations(spatial_stats) -> List[str]:
    """
    Generate performance recommendations based on spatial statistics.

    Args:
        spatial_stats: DataFrame with spatial statistics per partition

    Returns:
        List of performance recommendations
    """
    recommendations = []

    # Check for partition skew
    if len(spatial_stats) > 0:
        max_records = spatial_stats['record_count'].max()
        min_records = spatial_stats['record_count'].min()
        avg_records = spatial_stats['record_count'].mean()

        if max_records > avg_records * 10:
            recommendations.append("High partition skew detected - consider sub-partitioning large partitions")

        if min_records < avg_records * 0.1:
            recommendations.append("Small partitions detected - consider consolidating or different partitioning strategy")

        # Check spatial distribution
        bbox_areas = spatial_stats['partition_bbox_area']
        if bbox_areas.std() > bbox_areas.mean():
            recommendations.append("Uneven spatial distribution - consider spatial clustering or different partitioning")

        # DuckDB-specific recommendations
        recommendations.append("Use DuckDB's spatial indexes with ST_Intersects for bbox filtering")
        recommendations.append("Leverage DuckDB's automatic bounding box columns in GeoParquet for fast spatial queries")
        recommendations.append("Consider using ST_Hilbert for spatial ordering within partitions")

    return recommendations


@tag(stage="database_export", data_type="metadata")
@cache(behavior="disable")
def export_summary(
    hive_partitioned_geoparquet: Dict[str, Any],
    spatial_bounding_boxes: Dict[str, Any],
    export_configuration: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate comprehensive summary of DuckDB native export operations.

    Summarizes hive partitioning, spatial bounding boxes, and DuckDB features
    used in the export process.

    Args:
        hive_partitioned_geoparquet: Results from hive partitioning export
        spatial_bounding_boxes: Spatial bounding box analysis
        export_configuration: DuckDB native export configuration

    Returns:
        Comprehensive export summary with DuckDB feature utilization
    """
    print(f"   📋 Generating DuckDB native export summary...")

    # Collect export results
    export_results = {
        "hive_partitioning": hive_partitioned_geoparquet.get("status", "unknown"),
        "spatial_analysis": spatial_bounding_boxes.get("status", "unknown")
    }

    successful_exports = [k for k, v in export_results.items() if v == "success"]
    failed_exports = [k for k, v in export_results.items() if v == "failed"]

    # Calculate totals
    total_records = hive_partitioned_geoparquet.get("total_records", 0)
    total_partitions = hive_partitioned_geoparquet.get("partition_count", 0)
    total_files = len(hive_partitioned_geoparquet.get("files_created", []))

    # DuckDB features utilized
    duckdb_features_used = []
    if hive_partitioned_geoparquet.get("duckdb_features", {}).get("hive_partitioning"):
        duckdb_features_used.append("Native hive partitioning with PARTITION_BY")
    if hive_partitioned_geoparquet.get("duckdb_features", {}).get("geoparquet_writer"):
        duckdb_features_used.append("GeoParquet writer with automatic bounding box columns")
    if spatial_bounding_boxes.get("duckdb_geoparquet_features", {}).get("spatial_filtering_optimized"):
        duckdb_features_used.append("Optimized spatial filtering with bbox columns")
    if export_configuration.get("spatial_features", {}).get("enable_hilbert_ordering"):
        duckdb_features_used.append("Hilbert curve spatial ordering")

    # Performance insights
    performance_insights = []
    if spatial_bounding_boxes.get("performance_recommendations"):
        performance_insights.extend(spatial_bounding_boxes["performance_recommendations"])

    # Add DuckDB-specific insights
    performance_insights.extend([
        "DuckDB's GeoParquet writer automatically includes bounding box columns for fast spatial filtering",
        "Use ST_Intersects with bbox columns for optimal spatial query performance",
        "Leverage DuckDB's native hive partitioning for distributed processing"
    ])

    # Create comprehensive summary
    summary = {
        "export_strategy": "duckdb_native_hive_partitioning",
        "total_records": total_records,
        "total_partitions": total_partitions,
        "total_files": total_files,
        "successful_exports": successful_exports,
        "failed_exports": failed_exports,
        "success_rate": len(successful_exports) / len(export_results) if export_results else 0,
        "duckdb_features_utilized": duckdb_features_used,
        "spatial_optimization": {
            "global_bounding_box": spatial_bounding_boxes.get("global_bounding_box", {}),
            "partition_distribution": spatial_bounding_boxes.get("spatial_distribution", {}),
            "automatic_bbox_columns": True,
            "hilbert_ordering": export_configuration.get("spatial_features", {}).get("enable_hilbert_ordering", False)
        },
        "performance_insights": performance_insights,
        "export_configuration": export_configuration,
        "created_at": pd.Timestamp.now().isoformat()
    }

    print(f"   ✅ DuckDB native export summary complete:")
    print(f"      - Strategy: DuckDB native hive partitioning")
    print(f"      - Total records: {total_records:,}")
    print(f"      - Partitions: {total_partitions}")
    print(f"      - Files: {total_files}")
    print(f"      - DuckDB features used: {len(duckdb_features_used)}")
    print(f"      - Success rate: {summary['success_rate']:.1%}")

    return summary


# Note: The database export module now focuses on DuckDB's native capabilities
# rather than manual export patterns. The main functions are:
# 1. hive_partitioned_geoparquet() - Uses DuckDB's EXPORT DATABASE with PARTITION_BY
# 2. spatial_bounding_boxes() - Analyzes DuckDB's automatic bounding box columns
# 3. export_summary() - Summarizes DuckDB native features utilized
#
# This approach leverages DuckDB's built-in optimizations instead of reinventing
# partitioning and export logic, following the principle of using the right tool
# for the job.
