"""
Helper functions for DOI PV location dataflow storage operations.

Contains storage-related helper functions to keep the main dataflow compact.
All functions prefixed with _ to exclude from Hamilton DAG visualization.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List
import re

import geopandas as gpd
import pyarrow as pa
import duckdb


def _apply_file_filters(files: List[Path], file_filters: Dict[str, Any]) -> List[Path]:
    """
    Apply file filters from DOI manifest to filter files.

    Args:
        files: List of file paths to filter
        file_filters: Filter configuration from DOI manifest

    Returns:
        Filtered list of file paths
    """
    if not file_filters:
        return files

    include_patterns = file_filters.get("include_patterns", [])
    exclude_patterns = file_filters.get("exclude_patterns", [])
    use_regex = file_filters.get("use_regex", False)

    filtered_files = []

    for file_path in files:
        file_name = file_path.name
        include_match = False
        exclude_match = False

        # Check include patterns
        if include_patterns:
            for pattern in include_patterns:
                if use_regex:
                    if re.search(pattern, file_name):
                        include_match = True
                        break
                else:
                    if file_path.match(pattern):
                        include_match = True
                        break
        else:
            # No include patterns means include all
            include_match = True

        # Check exclude patterns
        if exclude_patterns:
            for pattern in exclude_patterns:
                if use_regex:
                    if re.search(pattern, file_name):
                        exclude_match = True
                        break
                else:
                    if file_path.match(pattern):
                        exclude_match = True
                        break

        # Include file if it matches include patterns and doesn't match exclude patterns
        if include_match and not exclude_match:
            filtered_files.append(file_path)

    return filtered_files


def _geoarrow_table(gdf: gpd.GeoDataFrame, dataset_name: str):
    """
    Convert GeoDataFrame to Arrow table with proper GeoArrow encoding using geoarrow-rs.

    Uses geoarrow-rs (Rust implementation) for efficient spatial operations and
    better GeoArrow support with direct from_geopandas() conversion.

    Adds dataset_name metadata using native arro3 methods for single geometry types
    or PyArrow methods for mixed geometry types (WKB fallback).

    References:
    - https://geoarrow.org/geoarrow-rs/python/v0.4.0-beta.3/ecosystem/geopandas/
    - https://kylebarron.dev/arro3/v0.5.1/api/core/table/
    - https://kylebarron.dev/arro3/v0.5.1/api/core/schema/

    Args:
        gdf: GeoDataFrame to convert
        dataset_name: Name of the dataset (added to table metadata)

    Returns:
        Arrow table with dataset_name metadata (arro3.core.Table or PyArrow table)
    """
    if gdf.empty:
        return pa.table({
            'geometry': pa.array([], type=pa.binary()),
            'dataset_name': pa.array([dataset_name], type=pa.string())
        })

    gdf_copy = gdf.copy()

    # Import geoarrow-rs for efficient conversion
    from geoarrow.rust.core import from_geopandas

    # Check for mixed geometry types which may need special handling
    geom_types = set(gdf_copy.geometry.geom_type.unique())
    has_mixed_geom_types = len(geom_types) > 1

    try:
        if has_mixed_geom_types:
            print(f"   ⚠️  Mixed geometry types detected {geom_types}, using WKB fallback")
            # For mixed geometries, use WKB encoding as fallback
            table = pa.table(gdf_copy.to_arrow(index=False, geometry_encoding='WKB'))
            print(f"   ✅ Converted {len(gdf_copy)} features using WKB for mixed geometries")

            # Add dataset metadata (PyArrow table)
            metadata = {"dataset_name": dataset_name}
            table = table.replace_schema_metadata(metadata)

        else:
            # Use geoarrow-rs for efficient conversion with native GeoArrow types
            arro3_table = from_geopandas(gdf_copy)
            print(f"   ✅ Converted {len(gdf_copy)} features using geoarrow-rs native types")

            # Add dataset metadata using arro3 native methods
            # Reference: https://kylebarron.dev/arro3/v0.5.1/api/core/schema/
            metadata = {"dataset_name": dataset_name}
            schema_with_metadata = arro3_table.schema.with_metadata(metadata)
            table = arro3_table.with_schema(schema_with_metadata)

    except Exception as e:
        print(f"   ⚠️  geoarrow-rs conversion failed: {e}, falling back to WKB")
        # Fallback to WKB encoding if geoarrow-rs fails
        table = pa.table(gdf_copy.to_arrow(index=False, geometry_encoding='WKB'))
        print(f"   ✅ Converted {len(gdf_copy)} features using WKB fallback")

        # Add dataset metadata (PyArrow table)
        metadata = {"dataset_name": dataset_name}
        table = table.replace_schema_metadata(metadata)

    return table


def _duckdb_table_from_geoarrow(
    conn: 'duckdb.DuckDBPyConnection',
    arrow_table: pa.Table,
    table_name: str
) -> int:
    """
    Create DuckDB table from GeoArrow table using geoarrow-rs conversion.

    Uses geoarrow-rs to_geopandas() to properly convert Arrow tables with geometry data
    back to GeoPandas, preserving CRS and geometry information.

    Reference: https://geoarrow.org/geoarrow-rs/python/v0.4.0-beta.3/ecosystem/geopandas/

    Args:
        conn: DuckDB connection
        arrow_table: Arrow table to import
        table_name: Target table name in DuckDB

    Returns:
        Number of rows inserted
    """
    # Drop existing table
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    print(f"   🔄 Converting Arrow table back to GeoPandas for DuckDB import")

    try:
        # Convert using geoarrow-rs which preserves geometry information
        from geoarrow.rust.core import to_geopandas
        gdf = to_geopandas(arrow_table)
        # Use short CRS representation instead of verbose PROJ JSON
        crs_name = gdf.crs.to_string() if gdf.crs else "None"
        print(f"   ✅ geoarrow-rs conversion successful: {len(gdf)} rows, CRS: {crs_name}")

        # Handle DuckDB table creation - DuckDB doesn't support geoarrow extension types
        if isinstance(gdf, gpd.GeoDataFrame) and hasattr(gdf, 'geometry') and gdf.geometry is not None:
            # Set CRS if missing
            if gdf.crs is None:
                gdf = gdf.set_crs('EPSG:4326', allow_override=True)
            print(f"   ✅ GeoDataFrame with {len(gdf)} rows, CRS: {gdf.crs.to_string()}")

            # Convert geometry to WKB for DuckDB compatibility
            print(f"   🔄 Converting geometry to WKB for DuckDB")
            gdf_copy = gdf.copy()
            gdf_copy['geometry_wkb'] = gdf_copy.geometry.to_wkb()
            gdf_copy = gdf_copy.drop(columns=['geometry'])

            # Create table with WKB geometry converted to DuckDB geometry
            conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT
                    * EXCLUDE (geometry_wkb),
                    CASE
                        WHEN geometry_wkb IS NOT NULL
                        THEN ST_GeomFromWKB(geometry_wkb)
                        ELSE NULL
                    END as geometry
                FROM gdf_copy
            """)
        else:
            print(f"   ✅ DataFrame with {len(gdf)} rows (no geometry)")
            # No geometry column - simple table creation
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM gdf")

        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"   ✅ Created table {table_name} with {row_count} rows")
        return row_count

    except Exception as e:
        print(f"   ❌ Failed to create table {table_name}: {e}")
        raise


def _geoparquet_export(
    arrow_table: pa.Table,
    dataset_name: str,
    output_dir: Path
) -> str:
    """
    Export Arrow table to GeoParquet format using geoarrow-rs I/O.

    Uses geoarrow-rs for optimal GeoParquet export with native GeoArrow support.
    Falls back to GeoPandas conversion if needed.

    Reference: https://geoarrow.org/geoarrow-rs/python/v0.4.0-beta.3/api/io/geoparquet/

    Args:
        arrow_table: Arrow table to export
        dataset_name: Name of the dataset
        output_dir: Directory to save the parquet file

    Returns:
        Path to the exported parquet file
    """
    parquet_file = output_dir / f"doi_{dataset_name}.parquet"

    try:
        # Try geoarrow-rs direct GeoParquet export (most efficient)
        from geoarrow.rust.io import write_parquet

        # Use geoarrow-rs to write GeoParquet directly
        # Note: write_parquet should handle both PyArrow and arro3 tables
        write_parquet(arrow_table, str(parquet_file))
        print(f"   ✅ Exported {dataset_name} to GeoParquet using geoarrow-rs I/O")

    except Exception as geoarrow_error:
        print(f"   ⚠️  geoarrow-rs export failed: {geoarrow_error}, trying GeoPandas fallback")

        try:
            # Fallback: convert to GeoPandas and export
            from geoarrow.rust.core import to_geopandas
            gdf = to_geopandas(arrow_table)
            gdf.to_parquet(parquet_file)
            print(f"   ✅ Exported {dataset_name} to GeoParquet using GeoPandas fallback")

        except Exception as fallback_error:
            print(f"   ⚠️  GeoPandas fallback also failed: {fallback_error}")

            # Final fallback: manual WKB conversion
            df = arrow_table.to_pandas()
            geometry_columns = [col for col in df.columns if 'geometry' in col.lower()]
            if geometry_columns:
                geom_col = geometry_columns[0]
                df[geom_col] = gpd.GeoSeries.from_wkb(df[geom_col])
                gdf = gpd.GeoDataFrame(df, geometry=geom_col)
                gdf.to_parquet(parquet_file)
                print(f"   ✅ Exported {dataset_name} to GeoParquet using WKB conversion")
            else:
                raise ValueError(f"No geometry column found for {dataset_name}")

    return str(parquet_file)


def _storage_result(
    collected_arrow_tables: List[pa.Table],
    database_path: str,
    export_geoparquet: bool
) -> Dict[str, Any]:
    """Save arrow tables to DuckDB (WKB) and optionally export to GeoParquet (GeoArrow)."""

    # Connect to DuckDB
    conn = duckdb.connect(database_path)

    # Install spatial extension
    conn.execute("INSTALL spatial; LOAD spatial;")

    results = {
        "database_path": database_path,
        "tables_created": [],
        "geoparquet_files": [],
        "total_records": 0,
        "datasets_processed": 0,
        "datasets_total": len(collected_arrow_tables)
    }

    for table in collected_arrow_tables:
        # Get dataset name from metadata
        dataset_name = table.schema.metadata.get(b"dataset_name", b"unknown").decode()
        table_name = f"doi_{dataset_name}"

        # Use proven approach: convert to GeoPandas and use DuckDB's native support
        row_count = _duckdb_table_from_geoarrow(
            conn=conn,
            arrow_table=table,
            table_name=table_name
        )

        results["tables_created"].append({
            "table_name": table_name,
            "dataset_name": dataset_name,
            "record_count": row_count
        })
        results["total_records"] += row_count
        results["datasets_processed"] += 1

        # Export to GeoParquet if requested (use original GeoArrow table for best format)
        if export_geoparquet:
            output_dir = Path(database_path).parent / "geoparquet"
            output_dir.mkdir(exist_ok=True)

            try:
                parquet_file = _geoparquet_export(table, dataset_name, output_dir)
                results["geoparquet_files"].append(parquet_file)
            except Exception as e:
                print(f"   ⚠️  GeoParquet export failed for {dataset_name}: {e}")

    # Generate detailed summary
    print(f"\n🎯 Pipeline Results Summary:")
    print(f"   📊 Total datasets processed: {results['datasets_processed']}/{results['datasets_total']}")
    print(f"   📈 Total records imported: {results['total_records']:,}")
    print(f"   🗄️  Database: {database_path}")

    # Show table details
    print(f"\n📋 Tables created in DuckDB:")
    for table_info in results['tables_created']:
        table_name = table_info['table_name']
        record_count = table_info['record_count']
        print(f"   • {table_name}: {record_count:,} records")

    # Show database summary (CRS is standardized to WGS84)
    try:
        table_count = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'").fetchone()[0]
        print(f"\n📚 Database Summary:")
        print(f"   • Schema: main")
        print(f"   • Tables: {table_count}")
        print(f"   • CRS: EPSG:4326 (WGS84) - standardized")
    except Exception as e:
        print(f"   ⚠️  Could not retrieve database summary: {e}")

    if results['geoparquet_files']:
        print(f"\n📦 GeoParquet exports:")
        for parquet_file in results['geoparquet_files']:
            print(f"   • {parquet_file}")

    conn.close()
    return results
