"""
Hamilton dataflow for spatial optimization and indexing.

Phase 4: Spatial Optimization & Partitioning
Task 4.1: Advanced Spatial Indexing Module

Objective: Implement advanced spatial indexing and partitioning strategies for efficient queries

Key Functions:
- hilbert_curve_ordering() - Apply ST_Hilbert for spatial ordering in DuckDB
- geopandas_sindex_optimization() - Create spatial indexes for GeoPandas operations
- spatial_partitioning() - Partition data using adaptive S2 cells or KD-tree
- rtree_index_creation() - Create R-tree indexes for spatial queries

Implementation Notes:
- These nodes run after the stg consolidated PV dataset is built but BEFORE Overture (phase 3) nodes
- Use DuckDB spatial extensions for Hilbert curves with proper bounds
- Leverage GeoPandas sindex for optimization
- Consider adaptive S2 cell partitioning for large datasets
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


@tag(stage="spatial_optimization", data_type="metadata")
@cache(behavior="disable")
def spatial_optimization_config(
    spatial_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Configuration for spatial optimization and indexing operations.
    
    Args:
        spatial_config: Optional spatial configuration overrides
        
    Returns:
        Spatial optimization configuration dictionary
    """
    default_config = {
        "hilbert_curve": {
            "enable": True,
            "bbox_buffer": 0.01,  # Buffer around data bounds for Hilbert curve
            "precision": 1000000  # Precision for Hilbert encoding
        },
        "spatial_index": {
            "enable_geopandas_sindex": True,
            "enable_rtree": True,
            "rtree_leaf_capacity": 10,
            "rtree_near_minimum_overlap_factor": 32
        },
        "partitioning": {
            "enable_adaptive_s2": False,  # Disabled by default - experimental
            "s2_min_level": 8,
            "s2_max_level": 12,
            "target_partition_size": 10000  # Target records per partition
        }
    }
    
    if spatial_config:
        # Deep merge configuration
        for key, value in spatial_config.items():
            if key in default_config and isinstance(value, dict):
                default_config[key].update(value)
            else:
                default_config[key] = value
    
    print(f"   📊 Spatial optimization config loaded:")
    print(f"      - Hilbert curve: {default_config['hilbert_curve']['enable']}")
    print(f"      - GeoPandas sindex: {default_config['spatial_index']['enable_geopandas_sindex']}")
    print(f"      - R-tree indexes: {default_config['spatial_index']['enable_rtree']}")
    print(f"      - Adaptive S2 partitioning: {default_config['partitioning']['enable_adaptive_s2']}")
    
    return default_config


@tag(stage="spatial_optimization", data_type="table")
@cache(behavior="disable")
def hilbert_curve_ordering(
    pv_h3_grid_cells: ir.Table,
    spatial_optimization_config: Dict[str, Any]
) -> ir.Table:
    """
    Apply Hilbert curve spatial ordering using DuckDB's ST_Hilbert function.
    
    Uses DuckDB spatial extension to encode X,Y coordinates as Hilbert curve indices
    for improved spatial locality in queries and storage.
    
    Args:
        pv_h3_grid_cells: Ibis table with H3-indexed PV locations
        spatial_optimization_config: Spatial optimization configuration
        
    Returns:
        Ibis table with Hilbert curve ordering added
    """
    if not spatial_optimization_config["hilbert_curve"]["enable"]:
        print(f"   ⏭️  Hilbert curve ordering disabled, returning original table")
        return pv_h3_grid_cells
    
    print(f"   🌀 Applying Hilbert curve spatial ordering...")
    
    # Get configuration
    bbox_buffer = spatial_optimization_config["hilbert_curve"]["bbox_buffer"]
    precision = spatial_optimization_config["hilbert_curve"]["precision"]
    
    # Calculate bounding box for Hilbert curve
    # Use DuckDB aggregation functions to get bounds
    bounds_query = pv_h3_grid_cells.aggregate([
        pv_h3_grid_cells.centroid_lon.min().name("min_x"),
        pv_h3_grid_cells.centroid_lat.min().name("min_y"),
        pv_h3_grid_cells.centroid_lon.max().name("max_x"),
        pv_h3_grid_cells.centroid_lat.max().name("max_y")
    ])
    
    bounds = bounds_query.execute().iloc[0]
    
    # Apply buffer to bounds
    min_x = bounds["min_x"] - bbox_buffer
    min_y = bounds["min_y"] - bbox_buffer
    max_x = bounds["max_x"] + bbox_buffer
    max_y = bounds["max_y"] + bbox_buffer
    
    print(f"      - Hilbert curve bounds: ({min_x:.6f}, {min_y:.6f}) to ({max_x:.6f}, {max_y:.6f})")
    
    # Add Hilbert curve index using DuckDB's ST_Hilbert function
    # ST_Hilbert(x, y, min_x, min_y, max_x, max_y) encodes coordinates as Hilbert index
    hilbert_table = pv_h3_grid_cells.mutate(
        hilbert_index=ir.literal(f"""
            ST_Hilbert(
                centroid_lon, 
                centroid_lat, 
                {min_x}, 
                {min_y}, 
                {max_x}, 
                {max_y}
            )
        """, dtype="int64")
    )
    
    # Order by Hilbert index for spatial locality
    hilbert_ordered = hilbert_table.order_by("hilbert_index")
    
    print(f"   ✅ Hilbert curve ordering applied successfully")
    print(f"      - Bounds: ({min_x:.6f}, {min_y:.6f}) to ({max_x:.6f}, {max_y:.6f})")
    print(f"      - Precision: {precision}")
    
    return hilbert_ordered


@tag(stage="spatial_optimization", data_type="table")
@cache(behavior="disable")
def geopandas_sindex_optimization(
    hilbert_curve_ordering: ir.Table,
    spatial_optimization_config: Dict[str, Any]
) -> gpd.GeoDataFrame:
    """
    Create GeoPandas spatial index (sindex) for optimized spatial operations.
    
    Converts Ibis table to GeoDataFrame and builds spatial index using GeoPandas
    sindex functionality for efficient spatial queries.
    
    Args:
        hilbert_curve_ordering: Ibis table with Hilbert ordering
        spatial_optimization_config: Spatial optimization configuration
        
    Returns:
        GeoDataFrame with spatial index built
    """
    if not spatial_optimization_config["spatial_index"]["enable_geopandas_sindex"]:
        print(f"   ⏭️  GeoPandas sindex optimization disabled")
        # Convert to GeoDataFrame without sindex
        df = hilbert_curve_ordering.execute()
        return _convert_to_geodataframe(df)
    
    print(f"   🗂️  Building GeoPandas spatial index (sindex)...")
    
    # Convert Ibis table to pandas DataFrame
    df = hilbert_curve_ordering.execute()
    
    # Convert to GeoDataFrame
    gdf = _convert_to_geodataframe(df)
    
    # Build spatial index - this happens automatically when accessing sindex
    # but we'll access it explicitly to ensure it's built
    try:
        sindex_size = len(gdf.sindex)
        print(f"      - Spatial index built successfully: {sindex_size} entries")
        print(f"      - Index type: {type(gdf.sindex).__name__}")
        
        # Test spatial index functionality with a simple query
        if len(gdf) > 0:
            # Get bounds of first geometry for testing
            first_geom = gdf.geometry.iloc[0]
            if hasattr(first_geom, 'bounds'):
                test_bounds = first_geom.bounds
                possible_matches = list(gdf.sindex.intersection(test_bounds))
                print(f"      - Index test: {len(possible_matches)} potential matches for first geometry")
        
    except Exception as e:
        print(f"      ⚠️  Warning: Could not build spatial index: {e}")
        print(f"      - Continuing without spatial index optimization")
    
    print(f"   ✅ GeoPandas sindex optimization complete")
    
    return gdf


def _convert_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert pandas DataFrame to GeoDataFrame with proper geometry handling.
    
    Args:
        df: pandas DataFrame with geometry data
        
    Returns:
        GeoDataFrame with geometry column
    """
    from shapely.geometry import Point
    from shapely import wkb
    
    # Handle different geometry column formats
    if 'geometry' in df.columns:
        # Check if geometry is WKB or WKT
        if df['geometry'].dtype == 'object':
            first_geom = df['geometry'].iloc[0] if len(df) > 0 else None
            if isinstance(first_geom, bytes):
                # WKB format
                df['geometry'] = df['geometry'].apply(lambda x: wkb.loads(x) if x else None)
            elif isinstance(first_geom, str):
                # WKT format
                from shapely import wkt
                df['geometry'] = df['geometry'].apply(lambda x: wkt.loads(x) if x else None)
        
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    
    elif 'centroid_lon' in df.columns and 'centroid_lat' in df.columns:
        # Create Point geometries from centroid coordinates
        geometry = [Point(lon, lat) for lon, lat in zip(df['centroid_lon'], df['centroid_lat'])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    
    else:
        raise ValueError("No suitable geometry column found in DataFrame")
    
    return gdf


@tag(stage="spatial_optimization", data_type="table")
@cache(behavior="disable")
def rtree_index_creation(
    geopandas_sindex_optimization: gpd.GeoDataFrame,
    spatial_optimization_config: Dict[str, Any]
) -> gpd.GeoDataFrame:
    """
    Create R-tree spatial indexes for efficient spatial queries.

    Builds R-tree indexes using rtree library for optimized spatial operations
    beyond what GeoPandas sindex provides.

    Args:
        geopandas_sindex_optimization: GeoDataFrame with sindex
        spatial_optimization_config: Spatial optimization configuration

    Returns:
        GeoDataFrame with R-tree index metadata added
    """
    if not spatial_optimization_config["spatial_index"]["enable_rtree"]:
        print(f"   ⏭️  R-tree index creation disabled")
        return geopandas_sindex_optimization

    print(f"   🌳 Creating R-tree spatial indexes...")

    gdf = geopandas_sindex_optimization.copy()

    try:
        import rtree
        from rtree import index

        # Get R-tree configuration
        leaf_capacity = spatial_optimization_config["spatial_index"]["rtree_leaf_capacity"]
        near_min_overlap = spatial_optimization_config["spatial_index"]["rtree_near_minimum_overlap_factor"]

        # Create R-tree index properties
        props = index.Property()
        props.leaf_capacity = leaf_capacity
        props.near_minimum_overlap_factor = near_min_overlap

        print(f"      - R-tree configuration:")
        print(f"        • Leaf capacity: {leaf_capacity}")
        print(f"        • Near minimum overlap factor: {near_min_overlap}")

        # Create R-tree index
        rtree_idx = index.Index(properties=props)

        # Insert geometries into R-tree
        for idx, geom in enumerate(gdf.geometry):
            if geom and hasattr(geom, 'bounds'):
                rtree_idx.insert(idx, geom.bounds)

        # Add R-tree metadata to GeoDataFrame
        gdf.attrs['rtree_index'] = rtree_idx
        gdf.attrs['rtree_config'] = {
            'leaf_capacity': leaf_capacity,
            'near_minimum_overlap_factor': near_min_overlap,
            'total_entries': len(gdf)
        }

        print(f"   ✅ R-tree index created successfully")
        print(f"      - Total entries: {len(gdf)}")
        print(f"      - Index bounds: {rtree_idx.bounds if hasattr(rtree_idx, 'bounds') else 'N/A'}")

    except ImportError:
        print(f"      ⚠️  Warning: rtree library not available")
        print(f"      - Install with: pip install rtree")
        print(f"      - Continuing without R-tree optimization")
    except Exception as e:
        print(f"      ⚠️  Warning: Could not create R-tree index: {e}")
        print(f"      - Continuing without R-tree optimization")

    return gdf


@tag(stage="spatial_optimization", data_type="table")
@cache(behavior="disable")
def spatial_partitioning(
    rtree_index_creation: gpd.GeoDataFrame,
    spatial_optimization_config: Dict[str, Any]
) -> gpd.GeoDataFrame:
    """
    Apply adaptive spatial partitioning using S2 cells or KD-tree.

    Experimental feature for partitioning large datasets using adaptive
    spatial partitioning strategies for improved query performance.

    Args:
        rtree_index_creation: GeoDataFrame with R-tree indexes
        spatial_optimization_config: Spatial optimization configuration

    Returns:
        GeoDataFrame with spatial partitioning metadata added
    """
    if not spatial_optimization_config["partitioning"]["enable_adaptive_s2"]:
        print(f"   ⏭️  Adaptive S2 partitioning disabled (experimental feature)")
        return rtree_index_creation

    print(f"   🗺️  Applying adaptive S2 cell partitioning (experimental)...")

    gdf = rtree_index_creation.copy()

    try:
        # Try to use s2sphere library for S2 cell partitioning
        import s2sphere

        s2_min_level = spatial_optimization_config["partitioning"]["s2_min_level"]
        s2_max_level = spatial_optimization_config["partitioning"]["s2_max_level"]
        target_size = spatial_optimization_config["partitioning"]["target_partition_size"]

        print(f"      - S2 configuration:")
        print(f"        • Min level: {s2_min_level}")
        print(f"        • Max level: {s2_max_level}")
        print(f"        • Target partition size: {target_size}")

        # Convert geometries to S2 cells
        s2_cells = []
        for geom in gdf.geometry:
            if geom and hasattr(geom, 'centroid'):
                centroid = geom.centroid
                lat_lng = s2sphere.LatLng.from_degrees(centroid.y, centroid.x)
                cell_id = s2sphere.CellId.from_lat_lng(lat_lng)

                # Determine appropriate level based on density
                level = min(s2_max_level, max(s2_min_level,
                           _calculate_adaptive_s2_level(geom, target_size)))

                parent_cell = cell_id.parent(level)
                s2_cells.append(str(parent_cell))
            else:
                s2_cells.append(None)

        # Add S2 cell partitioning column
        gdf['s2_partition'] = s2_cells

        # Calculate partition statistics
        partition_counts = gdf['s2_partition'].value_counts()

        print(f"   ✅ S2 partitioning applied successfully")
        print(f"      - Total partitions: {len(partition_counts)}")
        print(f"      - Avg records per partition: {partition_counts.mean():.1f}")
        print(f"      - Min/Max records per partition: {partition_counts.min()}/{partition_counts.max()}")

    except ImportError:
        print(f"      ⚠️  Warning: s2sphere library not available")
        print(f"      - Install with: pip install s2sphere")
        print(f"      - Skipping S2 partitioning")
    except Exception as e:
        print(f"      ⚠️  Warning: Could not apply S2 partitioning: {e}")
        print(f"      - Skipping S2 partitioning")

    return gdf


def _calculate_adaptive_s2_level(geom, target_size: int) -> int:
    """
    Calculate adaptive S2 level based on geometry and target partition size.

    Args:
        geom: Shapely geometry
        target_size: Target number of records per partition

    Returns:
        Appropriate S2 level
    """
    # Simple heuristic: larger geometries get lower levels (bigger cells)
    # This is a placeholder - in practice you'd want more sophisticated logic
    if hasattr(geom, 'area'):
        area = geom.area
        if area > 0.01:  # Large area
            return 8
        elif area > 0.001:  # Medium area
            return 10
        else:  # Small area
            return 12

    return 10  # Default level


@tag(stage="spatial_optimization", data_type="metadata")
@cache(behavior="disable")
def spatial_optimization_summary(
    spatial_partitioning: gpd.GeoDataFrame,
    spatial_optimization_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate summary of spatial optimization operations applied.

    Args:
        spatial_partitioning: Final GeoDataFrame with all optimizations
        spatial_optimization_config: Spatial optimization configuration

    Returns:
        Summary dictionary of optimization results
    """
    print(f"   📊 Generating spatial optimization summary...")

    gdf = spatial_partitioning

    summary = {
        "total_records": len(gdf),
        "optimizations_applied": [],
        "spatial_indexes": {},
        "partitioning": {},
        "performance_hints": []
    }

    # Check which optimizations were applied
    if 'hilbert_index' in gdf.columns:
        summary["optimizations_applied"].append("hilbert_curve_ordering")
        summary["performance_hints"].append("Data ordered by Hilbert curve for spatial locality")

    if hasattr(gdf, 'sindex'):
        summary["optimizations_applied"].append("geopandas_sindex")
        summary["spatial_indexes"]["geopandas_sindex"] = {
            "type": type(gdf.sindex).__name__,
            "entries": len(gdf)
        }
        summary["performance_hints"].append("GeoPandas spatial index available for fast spatial queries")

    if 'rtree_index' in gdf.attrs:
        summary["optimizations_applied"].append("rtree_index")
        summary["spatial_indexes"]["rtree"] = gdf.attrs.get('rtree_config', {})
        summary["performance_hints"].append("R-tree index available for advanced spatial operations")

    if 's2_partition' in gdf.columns:
        summary["optimizations_applied"].append("s2_partitioning")
        partition_counts = gdf['s2_partition'].value_counts()
        summary["partitioning"]["s2_cells"] = {
            "total_partitions": len(partition_counts),
            "avg_records_per_partition": float(partition_counts.mean()),
            "min_records": int(partition_counts.min()),
            "max_records": int(partition_counts.max())
        }
        summary["performance_hints"].append("S2 cell partitioning available for distributed processing")

    # Add configuration summary
    summary["configuration"] = spatial_optimization_config

    print(f"   ✅ Spatial optimization summary complete:")
    print(f"      - Total records: {summary['total_records']:,}")
    print(f"      - Optimizations applied: {len(summary['optimizations_applied'])}")
    print(f"      - Spatial indexes: {len(summary['spatial_indexes'])}")
    print(f"      - Performance hints: {len(summary['performance_hints'])}")

    return summary
