#!/usr/bin/env python3
"""
Hamilton-based DOI dataset pipeline for EO PV data ingestion.

This replaces the previous dlt-based doi_dataset_pipeline.py with a simpler,
more maintainable Hamilton approach that provides fine-grained lineage and self-documenting pipelines.

Key improvements:
- Uses doi_manifest.json for dataset configuration
- Implements Hamilton's Parallelizable for default concurrent processing
- Proper parameterization with source() dependencies
- Zero-copy Arrow integration for efficient data exchange 
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterator
import pandas as pd
import duckdb
import geopandas as gpd
import pyarrow as pa

# Import utility functions with robust import handling
try:
    # Try relative import first (when running as script)
    from utils.ingestion_utils import (
        load_download_cache, save_download_cache, get_cached_tempdir,
        cache_tempdir, cleanup_cache_entry, clear_all_cache, print_raw_data_summary
    )
except ImportError:
    try:
        # Try absolute import (when imported as module by Hamilton CLI)
        from data_loaders.utils.ingestion_utils import (
            load_download_cache, save_download_cache, get_cached_tempdir,
            cache_tempdir, cleanup_cache_entry, clear_all_cache, print_raw_data_summary
        )
    except ImportError:
        # Fallback: add current directory to path and try again
        import sys
        from pathlib import Path
        current_dir = Path(os.getcwd()).parent
        if str(current_dir) not in sys.path:
            sys.path.insert(0, str(current_dir))
        try:
            from utils.ingestion_utils import (
                load_download_cache, save_download_cache, get_cached_tempdir,
                cache_tempdir, cleanup_cache_entry, clear_all_cache, print_raw_data_summary
            )
        except ImportError as e:
            # If all imports fail, provide helpful error message
            raise ImportError(
                f"Cannot import ingestion utilities. Tried multiple import paths. "
                f"Ensure utils/ingestion_utils.py exists in the correct location. "
                f"Original error: {e}"
            )
from hamilton import driver
from hamilton.function_modifiers import tag
from hamilton.htypes import Parallelizable, Collect

# GeoArrow imports for proper geometry handling in Arrow
try:
    import geoarrow.pandas as _  # This registers pyarrow extension types and geoarrow accessor
    import geoarrow.pyarrow as ga
    GEOARROW_AVAILABLE = True
    print("✅ GeoArrow available for proper geometry handling")
except ImportError:
    GEOARROW_AVAILABLE = False
    print("❌ GeoArrow not available - please install: pip install geoarrow-pandas geoarrow-pyarrow")
    raise ImportError("GeoArrow is required for geometry handling. Install with: pip install geoarrow-pandas geoarrow-pyarrow")

# Ensure project root is in Python path for Hamilton CLI compatibility
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(os.getcwd()).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Also add parent directory in case we're in a subdirectory
parent_dir = project_root.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))


def create_duckdb_table_from_arrow(
    conn: 'duckdb.DuckDBPyConnection',
    arrow_table: pa.Table,
    table_name: str,
    add_geometry: bool = True
) -> int:
    """
    Create DuckDB table from Arrow table using zero-copy conversion to GeoPandas.
    
    Avoids Arrow metadata serialization issues by converting back to GeoPandas
    and using DuckDB's09 native GeoPandas support.

    Args:
        conn: DuckDB connection
        arrow_table: Arrow table to import
        table_name: Target table name in DuckDB
        add_geometry: Whether to convert geometry column to PostGIS format

    Returns:
        Number of rows inserted
    """
    # Drop existing table
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    print(f"   🔄 Converting Arrow table back to GeoPandas for DuckDB import")
    
    try:
        # Use GeoArrow to convert to GeoPandas
        gdf = ga.to_geopandas(arrow_table)

        # Set CRS to WGS84 since all our data is standardized to this
        if hasattr(gdf, 'geometry') and gdf.geometry is not None:
            gdf = gdf.set_crs('EPSG:4326', allow_override=True)
            print(f"   ✅ Created GeoDataFrame with {len(gdf)} rows and geometry column")

            # Convert geometry to WKB for DuckDB compatibility
            # GeoPandas geometry objects need to be converted to WKB bytes for DuckDB
            print(f"   🔄 Converting geometry to WKB for DuckDB")
            gdf_copy = gdf.copy()
            gdf_copy['geometry_wkb'] = gdf_copy.geometry.to_wkb()
            gdf_copy = gdf_copy.drop(columns=['geometry'])

            # Use DuckDB's native support with WKB conversion
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
            print(f"   ✅ Created DataFrame with {len(gdf)} rows (no geometry)")
            # No geometry column - simple table creation
            conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM gdf
            """)

        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"   ✅ Successfully created table {table_name} with {row_count} rows")
        return row_count

    except Exception as conversion_error:
        print(f"   ❌ GeoArrow conversion failed: {conversion_error}")
        print(f"   🔄 Falling back to pandas conversion")

        # Fallback to pandas if GeoArrow conversion fails
        df = arrow_table.to_pandas()

        # Handle different geometry column types
        if 'geometry' in df.columns:
            # Check if it's WKB (binary) or WKT (string)
            if df['geometry'].dtype == 'object' and len(df) > 0:
                sample_val = df['geometry'].iloc[0]
                if isinstance(sample_val, bytes):
                    print(f"   🗺️  Using WKB geometry column directly")
                    gdf = df  # Keep as DataFrame with WKB bytes

                    conn.execute(f"""
                        CREATE TABLE {table_name} AS
                        SELECT
                            * EXCLUDE (geometry),
                            CASE
                                WHEN geometry IS NOT NULL
                                THEN ST_GeomFromWKB(geometry)
                                ELSE NULL
                            END as geometry
                        FROM gdf
                    """)
                else:
                    # Assume it's already proper geometry or handle as regular column
                    gdf = df
                    conn.execute(f"""
                        CREATE TABLE {table_name} AS
                        SELECT * FROM gdf
                    """)
            else:
                gdf = df
                conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM gdf
                """)
        elif 'geometry_wkt' in df.columns:
            # Convert WKT to geometry and use ST_GeomFromText
            print(f"   🗺️  Converting geometry_wkt column")
            df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry_wkt'])
            df = df.drop(columns=['geometry_wkt'])
            gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')

            conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT
                    * EXCLUDE (geometry),
                    CASE
                        WHEN geometry IS NOT NULL AND geometry != ''
                        THEN ST_GeomFromText(geometry)
                        ELSE NULL
                    END as geometry
                FROM gdf
            """)
        else:
            # No geometry column
            gdf = df
            conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM gdf
            """)

        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"   ✅ Fallback successful: created table {table_name} with {row_count} rows")
        return row_count


# Cache functions moved to ingestion_utils.py


@tag(
    data_source="doi",
    processing_stage="raw",
    data_type="metadata",
    hamilton_node_type="source",
    description="Load DOI dataset metadata configuration"
)
def dataset_metadata() -> Dict[str, Dict[str, Any]]:
    """
    Load dataset metadata configuration from doi_manifest.json.

    Returns:
        Dict mapping dataset names to their metadata configurations.
        This function has no dependencies and serves as a root node in the DAG.
    """
    from pathlib import Path
    import json

    manifest_path = Path(os.getcwd()).parent / "doi_manifest.json"

    if not manifest_path.exists():
        raise FileNotFoundError(f"DOI manifest not found: {manifest_path}")

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    # Filter for vector datasets suitable for our pipeline
    # Exclude datasets that are primarily for computer vision (has_imgs=True)
    vector_datasets = {}
    for dataset_name, metadata in manifest.items():
        # Filter out datasets from "sciencebase" repository
        if metadata.get("repo") == "sciencebase":
            print(f"   ℹ️  Skipping ScienceBase dataset (fetching not implemented): {dataset_name}")
            continue
        # Include datasets with geospatial vector formats and not primarily for CV
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
    description="Generate dataset names for parallel processing"
)
def target_datasets(dataset_metadata: Dict[str, Dict[str, Any]]) -> Parallelizable[str]:
    """
    Generate dataset names for parallel processing.

    Uses Hamilton's Parallelizable to enable concurrent dataset processing.
    Processes all available vector datasets by default.

    Args:
        dataset_metadata: Dataset metadata from dataset_metadata() function

    Yields:
        str: Dataset names for parallel processing
    """
    # Process all available vector datasets
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
    description="Generate dataset names for sequential processing"
)
def target_datasets_list(dataset_metadata: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Generate dataset names for sequential processing.

    Returns a simple list instead of Parallelizable for standard executor.
    Processes all available vector datasets by default.

    Args:
        dataset_metadata: Dataset metadata from dataset_metadata() function

    Returns:
        List[str]: Dataset names for sequential processing
    """
    # Process all available vector datasets
    available_datasets = list(dataset_metadata.keys())

    print(f"Processing {len(available_datasets)} datasets sequentially")
    return available_datasets


@tag(
    data_source="doi",
    processing_stage="download",
    io_operation="download",
    caching="enabled",
    description="Download DOI dataset with intelligent caching"
)
def download_doi_dataset(
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250,  # Increased for large datasets like Stowell
    use_cache: bool = True,  # Enable caching by default
    force_download: bool = False  # Force re-download even if cached
) -> str:
    """
    Download DOI dataset to directory with intelligent caching support.

    This function handles downloading datasets from various DOI repositories
    (Zenodo, GitHub, USGS) with smart caching to avoid redundant downloads
    during development and testing.

    Args:
        dataset_name: Name of the dataset to download
        dataset_metadata: Complete metadata dictionary from doi_manifest.json
        max_mb: Maximum file size in MB (default: 250MB for large datasets)
        use_cache: Whether to use cached downloads (default: True)
        force_download: Force re-download even if cached (default: False)

    Returns:
        str: Path to downloaded/extracted files directory

    Raises:
        ValueError: If dataset_name not found in metadata
        Exception: If download fails after retries

    Note:
        - Supports GitHub repositories and DOI-based downloads
        - Implements intelligent caching with JSON persistence
        - Handles large datasets up to specified max_mb limit
    """
    from pathlib import Path

    if dataset_name not in dataset_metadata:
        raise ValueError(f"Unknown dataset: {dataset_name}")

    metadata = dataset_metadata[dataset_name]

    # Check cache first (unless force_download is True)
    if use_cache and not force_download:
        cached_dir = get_cached_tempdir(dataset_name)
        if cached_dir:
            cached_files = list(Path(cached_dir).rglob("*"))
            cached_files = [f for f in cached_files if f.is_file()]
            print(f"📦 Using cached download for {dataset_name}")
            print(f"   Cache directory: {cached_dir}")
            print(f"   Cached files: {len(cached_files)}")
            return cached_dir

    # Use datahugger to download the dataset
    import datahugger
    import shutil
    from pathlib import Path
    import tempfile
    import os


    # Always create a new temporary directory for downloads
    download_dir = tempfile.mkdtemp(prefix=f"{dataset_name}_")
    print(f"📥 Downloading {dataset_name} to temporary directory: {download_dir}")

    try:
        if metadata["repo"] == "github":
            # Handle GitHub repositories using existing fetch_from_github function
            print(f"📥 Downloading GitHub dataset {dataset_name}...")
            print(f"   Repository URL: {metadata['doi']}")
            print(f"   Target directory: {download_dir}")

            # Import the existing GitHub fetch function with robust import handling
            try:
                from utils.fetch_and_preprocess import fetch_from_github
            except ImportError:
                try:
                    from data_loaders.utils.fetch_and_preprocess import fetch_from_github
                except ImportError:
                    # Add parent directory to path and try again
                    import sys
                    from pathlib import Path
                    parent_dir = Path(os.getcwd()).parent.parent
                    if str(parent_dir) not in sys.path:
                        sys.path.insert(0, str(parent_dir))
                    from utils.fetch_and_preprocess import fetch_from_github

            try:
                result = fetch_from_github(
                    doi=metadata["doi"],
                    dst=download_dir,
                    max_mb=max_mb,
                    force=force_download  # Use force_download parameter
                )

                if result and result.get('files'):
                    downloaded_files = result['files']
                    print(f"✅ Downloaded {len(downloaded_files)} files from GitHub")
                    for f in downloaded_files[:5]:  # Show first 5 files
                        file_path = Path(f)
                        print(f"   📄 {file_path.name} ({file_path.stat().st_size / 1024:.1f} KB)")
                    if len(downloaded_files) > 5:
                        print(f"   ... and {len(downloaded_files) - 5} more files")
                else:
                    print(f"⚠️  No files downloaded from GitHub for {dataset_name}")
                    print(f"   Result: {result}")

                # Update cache if using cache
                if use_cache:
                    cache_tempdir(dataset_name, download_dir)

                return download_dir

            except Exception as github_error:
                print(f"❌ GitHub download failed for {dataset_name}: {github_error}")
                print(f"   URL: {metadata['doi']}")
                raise
        else:
            # Use datahugger for DOI-based downloads
            print(f"📥 Downloading {dataset_name} from {metadata['repo']} ({metadata['doi']})...")
            print(f"   Target directory: {download_dir}")
            print(f"   Max file size: {max_mb} MB")
            print(f"   Starting download at {pd.Timestamp.now()}")

            # Call datahugger for download
            import time

            try:
                start_time = time.time()
                print(f"   📡 Calling datahugger.get()...")

                datahugger.get(
                    metadata["doi"],
                    output_folder=download_dir,
                    max_file_size=max_mb * 1024 * 1024  # Convert MB to bytes
                )

                download_time = time.time() - start_time
                print(f"   ⏱️  Download completed in {download_time:.1f} seconds")
                download_path = Path(download_dir)
                downloaded_files = list(download_path.rglob("*"))
                downloaded_files = [f for f in downloaded_files if f.is_file()]

                if downloaded_files:
                    total_size = sum(f.stat().st_size for f in downloaded_files)
                    print(f"✅ Downloaded {len(downloaded_files)} files for {dataset_name} ({total_size / 1024 / 1024:.1f} MB)")
                    for f in downloaded_files[:5]:  # Show first 5 files
                        print(f"   📄 {f.name} ({f.stat().st_size / 1024:.1f} KB)")
                    if len(downloaded_files) > 5:
                        print(f"   ... and {len(downloaded_files) - 5} more files")
                else:
                    print(f"⚠️  No files downloaded for {dataset_name}")
                    print(f"   Directory contents: {list(download_path.iterdir())}")

                # Update cache if using cache
                if use_cache:
                    cache_tempdir(dataset_name, download_dir)

                return download_dir

            except Exception as download_error:
                print(f"❌ Datahugger download failed for {dataset_name}: {download_error}")
                print(f"   DOI: {metadata['doi']}")
                print(f"   Repo: {metadata['repo']}")
                raise

    except Exception as e:
        print(f"💥 Error downloading {dataset_name}: {e}")
        print(f"   Cleaning up download directory: {download_dir}")
        # Clean up download directory on failure (only if not using cache)
        if not use_cache:
            shutil.rmtree(download_dir, ignore_errors=True)
        raise


@tag(
    data_source="doi",
    processing_stage="extract",
    file_operations="discovery",
    geospatial_formats=["geojson", "shp", "gpkg", "json"],
    description="Extract and filter geospatial files from downloaded datasets"
)
def extract_geospatial_files(
    download_path: str,
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]]
) -> List[str]:
    """
    Extract and locate geospatial files from downloaded dataset with intelligent filtering.

    This function discovers geospatial files in various formats and applies
    dataset-specific filtering rules defined in the metadata configuration.
    Supports complex include/exclude patterns for precise file selection.

    Args:
        download_path: Path to downloaded dataset directory
        dataset_name: Name of the dataset for logging and filtering
        dataset_metadata: Complete metadata with file filtering rules

    Returns:
        List[str]: Paths to filtered geospatial files

    Raises:
        ValueError: If no valid geospatial files found after filtering

    Supported Formats:
        - GeoJSON (.geojson)
        - Shapefile (.shp)
        - GeoPackage (.gpkg)
        - JSON (.json) - for non-geospatial JSON with coordinates

    Filtering Features:
        - Include/exclude patterns from metadata
        - Regex and glob pattern support
        - Dataset-specific file selection rules
    """
    from pathlib import Path

    metadata = dataset_metadata[dataset_name]
    label_fmt = metadata["label_fmt"]

    # Define file extensions to look for based on format
    format_extensions = {
        "geojson": [".geojson"],
        "shp": [".shp"],
        "gpkg": [".gpkg"],
        "json": [".json"]
    }

    extensions = format_extensions.get(label_fmt, [f".{label_fmt}"])

    # Find all files with matching extensions
    geospatial_files = []
    download_dir = Path(download_path)

    print(f"🔍 Looking for {label_fmt} files in {download_path}")
    print(f"   Expected extensions: {extensions}")

    # Debug: Show all files in directory
    all_files = list(download_dir.rglob("*"))
    all_files = [f for f in all_files if f.is_file()]
    print(f"   Found {len(all_files)} total files:")
    for f in all_files[:10]:  # Show first 10 files
        print(f"     📄 {f.relative_to(download_dir)} ({f.suffix})")
    if len(all_files) > 10:
        print(f"     ... and {len(all_files) - 10} more files")

    # Look for files with matching extensions
    for ext in extensions:
        matching_files = list(download_dir.rglob(f"*{ext}"))
        geospatial_files.extend(matching_files)
        print(f"   Files with {ext}: {len(matching_files)}")

    # Apply file filtering if specified in metadata
    file_filters = metadata.get("file_filters", {})
    if file_filters:
        print(f"   📋 Applying file filters: {file_filters.get('description', 'Custom filters')}")

        include_patterns = file_filters.get("include_patterns", [])
        exclude_patterns = file_filters.get("exclude_patterns", [])
        use_regex = file_filters.get("use_regex", False)

        if use_regex:
            import re
            print(f"   🔍 Using regex pattern matching")

        filtered_files = []
        for file_path in geospatial_files:
            file_path_str = str(file_path)
            file_name = file_path.name

            # Check include patterns (must match at least one)
            if include_patterns:
                if use_regex:
                    include_match = any(
                        re.search(pattern, file_path_str, re.IGNORECASE)
                        for pattern in include_patterns
                    )
                else:
                    # Use simple substring matching (case-insensitive)
                    include_match = any(
                        pattern.lower() in file_path_str.lower()
                        for pattern in include_patterns
                    )

                if not include_match:
                    print(f"     ❌ Excluded (missing include pattern): {file_name}")
                    continue

            # Check exclude patterns (none should match)
            if exclude_patterns:
                if use_regex:
                    exclude_match = any(
                        re.search(pattern, file_path_str, re.IGNORECASE)
                        for pattern in exclude_patterns
                    )
                else:
                    # Use simple substring matching (case-insensitive)
                    exclude_match = any(
                        pattern.lower() in file_path_str.lower()
                        for pattern in exclude_patterns
                    )

                if exclude_match:
                    print(f"     ❌ Excluded (matches exclude pattern): {file_name}")
                    continue

            filtered_files.append(file_path)
            print(f"     ✅ Included: {file_name}")

        geospatial_files = filtered_files
        print(f"   🎯 After filtering: {len(geospatial_files)} files selected")

    file_paths = [str(f) for f in geospatial_files]

    if not file_paths:
        print(f"❌ No {label_fmt} files found in {download_path}")
        print(f"   Available file extensions: {set(f.suffix for f in all_files)}")
        if file_filters:
            print(f"   Applied filters: include={file_filters.get('include_patterns')}, exclude={file_filters.get('exclude_patterns')}")
        raise ValueError(f"No {label_fmt} files found in {download_path}")

    print(f"✅ Found {len(file_paths)} {label_fmt} files for {dataset_name}")
    for fp in file_paths:
        print(f"   📄 {Path(fp).name}")
    return file_paths


@tag(
    data_source="doi",
    processing_stage="download",
    data_type="file_path",
    hamilton_node_type="transform",
    io_operation="download",
    description="Download DOI dataset and return file path"
)
def dataset_download_path(
    target_datasets: str,
    dataset_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250,
    use_cache: bool = True,
    force_download: bool = False
) -> str:
    """
    Download DOI dataset and return path to downloaded files.

    This function wraps download_doi_dataset to provide proper Hamilton dependency injection.

    Args:
        target_datasets: Dataset name from target_datasets() function (Hamilton injects individual values)
        dataset_metadata: Dataset metadata from dataset_metadata() function
        max_mb: Maximum file size in MB
        use_cache: Whether to use cached downloads
        force_download: Force re-download even if cached

    Returns:
        str: Path to downloaded/extracted files
    """
    return download_doi_dataset(target_datasets, dataset_metadata, max_mb, use_cache, force_download)


@tag(
    data_source="doi",
    processing_stage="extract",
    data_type="file_paths",
    hamilton_node_type="transform",
    io_operation="file_discovery",
    description="Extract and locate geospatial files from downloaded dataset"
)
def dataset_geospatial_files(
    dataset_download_path: str,
    target_datasets: str,
    dataset_metadata: Dict[str, Dict[str, Any]]
) -> List[str]:
    """
    Extract and locate geospatial files from downloaded dataset.

    Args:
        dataset_download_path: Path to downloaded files from dataset_download_path() function
        target_datasets: Dataset name from target_datasets() function
        dataset_metadata: Dataset metadata from dataset_metadata() function

    Returns:
        List[str]: Paths to geospatial files
    """
    return extract_geospatial_files(dataset_download_path, target_datasets, dataset_metadata)


@tag(
    data_source="doi",
    processing_stage="process",
    data_type="geodataframe",
    hamilton_node_type="transform",
    geospatial_operation="standardization",
    description="Process geospatial files into standardized GeoDataFrame"
)
def dataset_processed_gdf(
    dataset_geospatial_files: List[str],
    target_datasets: str,
    dataset_metadata: Dict[str, Dict[str, Any]]
) -> gpd.GeoDataFrame:
    """
    Process geospatial files into standardized GeoDataFrame.

    Args:
        dataset_geospatial_files: Geospatial file paths from dataset_geospatial_files() function
        target_datasets: Dataset name from target_datasets() function
        dataset_metadata: Dataset metadata from dataset_metadata() function

    Returns:
        gpd.GeoDataFrame: Processed geospatial data
    """
    return process_geospatial_data(dataset_geospatial_files, target_datasets, dataset_metadata)


@tag(
    data_source="doi",
    processing_stage="convert",
    data_type="arrow_table",
    hamilton_node_type="transform",
    data_format="arrow",
    description="Convert processed GeoDataFrame to Arrow table format"
)
def dataset_arrow_table(
    dataset_processed_gdf: gpd.GeoDataFrame,
    target_datasets: str
) -> pa.Table:
    """
    Convert processed GeoDataFrame to Arrow table format.

    Args:
        dataset_processed_gdf: Processed GeoDataFrame from dataset_processed_gdf() function
        target_datasets: Dataset name from target_datasets() function for metadata

    Returns:
        pa.Table: Arrow table with dataset metadata
    """
    print(f"   📝 Converting to Arrow using GeoPandas to_arrow() method")

    try:
        # Use GeoPandas to_arrow() with WKB encoding - this is already GeoArrow compatible!
        print(f"   📝 Converting to Arrow with WKB geometry encoding")
        geopandas_arrow = dataset_processed_gdf.to_arrow(index=False, geometry_encoding='WKB')

        # Convert GeoPandas ArrowTable to PyArrow Table
        arrow_table = pa.table(geopandas_arrow)
        print(f"   ✅ Converted to Arrow table with WKB geometry: {arrow_table.num_rows} rows, {len(arrow_table.columns)} columns")

        # Check geometry columns
        geometry_columns = [col for col in arrow_table.column_names if 'geometry' in col.lower()]
        if geometry_columns:
            print(f"   🗺️  Geometry columns in Arrow table: {geometry_columns}")
            # Show geometry column type
            for col in geometry_columns:
                col_type = arrow_table.schema.field(col).type
                print(f"     {col}: {col_type}")

    except Exception as arrow_error:
        print(f"   ❌ Arrow conversion failed: {arrow_error}")
        print(f"   🔄 Final fallback to manual WKT conversion")

        # Final fallback: manual WKT conversion
        arrow_df = dataset_processed_gdf.copy()
        if 'geometry' in arrow_df.columns:
            if 'geometry_wkt' not in arrow_df.columns:
                arrow_df['geometry_wkt'] = arrow_df['geometry'].to_wkt()
            arrow_df = arrow_df.drop(columns=['geometry'])

        arrow_table = pa.Table.from_pandas(arrow_df, preserve_index=False)
        print(f"   ✅ Manual WKT conversion successful: {arrow_table.num_rows} rows, {len(arrow_table.columns)} columns")

    # Add dataset name to table metadata for identification during storage
    metadata = {b'dataset_name': target_datasets.encode('utf-8')}
    arrow_table = arrow_table.replace_schema_metadata(metadata)
    print(f"   📝 Added dataset metadata: {target_datasets}")
    print(f"✓ Completed processing {target_datasets}: {arrow_table.num_rows} records")
    return arrow_table


@tag(data_source="doi", processing_stage="process")
def process_geospatial_data(
    geospatial_files: List[str],
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]]
) -> gpd.GeoDataFrame:
    """
    Process geospatial files into standardized GeoDataFrame.

    Uses our existing process_vector_geoms utility with staging-specific settings.
    Standardizes CRS to WGS84 to avoid concatenation issues.
    """
    metadata = dataset_metadata[dataset_name]

    print(f"🔧 Processing geospatial data for {dataset_name}")
    print(f"   Files to process: {len(geospatial_files)}")

    # Load geospatial files directly without column filtering
    # Raw layer preserves ALL original columns - staging layer handles filtering
    print(f"   📂 Loading {len(geospatial_files)} geospatial files...")

    ds_dataframes = []
    for fname in geospatial_files:
        if fname.endswith(('.geojson', '.json', '.shp', '.gpkg')):
            try:
                file_gdf = gpd.read_file(fname)
                ds_dataframes.append(file_gdf)
                print(f"     📄 Loaded {Path(fname).name}: {len(file_gdf)} features, {len(file_gdf.columns)} columns")
            except Exception as e:
                print(f"     ❌ Error reading {Path(fname).name}: {e}")
                continue

    if len(ds_dataframes) == 0:
        raise ValueError(f"No valid geospatial files found for {dataset_name}")

    # Concatenate all dataframes (preserve all columns)
    gdf = gpd.GeoDataFrame(pd.concat(ds_dataframes, ignore_index=True))

    # Ensure geometry column is properly set
    if 'geometry' not in gdf.columns and hasattr(gdf, 'geometry'):
        gdf['geometry'] = gdf.geometry

    print(f"   📊 Combined dataset: {len(gdf)} features, {len(gdf.columns)} columns")
    print(f"   📋 All columns preserved: {list(gdf.columns)}")

    # Basic cleanup only - no column filtering
    if metadata.get('rm_invalid', True):
        initial_count = len(gdf)
        gdf = gdf[gdf.geometry.is_valid]
        if len(gdf) < initial_count:
            print(f"   🧹 Removed {initial_count - len(gdf)} invalid geometries")

    # Remove exact duplicates only
    initial_count = len(gdf)
    gdf = gdf.drop_duplicates()
    if len(gdf) < initial_count:
        print(f"   🧹 Removed {initial_count - len(gdf)} duplicate rows")

    if gdf is None or len(gdf) == 0:
        raise ValueError(f"No valid geometries found for {dataset_name}")

    print(f"   📊 Loaded {len(gdf)} features")
    print(f"   🗺️  Original CRS: {gdf.crs}")

    # Standardize CRS to WGS84 to avoid concatenation issues
    target_crs = 'EPSG:4326'  # WGS84
    if gdf.crs is None:
        print(f"   ⚠️  No CRS detected, assuming WGS84")
        gdf = gdf.set_crs(target_crs)
    elif gdf.crs != target_crs:
        print(f"   🔄 Converting from {gdf.crs} to {target_crs}")
        try:
            gdf = gdf.to_crs(target_crs)
            print(f"   ✅ CRS conversion successful")
        except Exception as crs_error:
            print(f"   ❌ CRS conversion failed: {crs_error}")
            print(f"   🔧 Attempting to set CRS to WGS84 directly")
            gdf = gdf.set_crs(target_crs)
    else:
        print(f"   ✅ Already in target CRS: {target_crs}")

    print(f"   📝 Adding metadata columns to {len(gdf)} rows")

    # Ensure we're working with a copy to avoid SettingWithCopyWarning
    gdf = gdf.copy()

    # Add metadata columns (these are scalar values applied to all rows)
    gdf.loc[:, 'dataset_name'] = dataset_name
    gdf.loc[:, 'doi'] = metadata['doi']
    gdf.loc[:, 'repository_type'] = metadata['repo']
    gdf.loc[:, 'label_format'] = metadata['label_fmt']

    # Handle potentially complex metadata fields safely
    geom_type_str = str(metadata.get('geom_type', 'unknown'))
    if isinstance(metadata.get('geom_type'), (list, dict)):
        geom_type_str = str(metadata['geom_type'])
    gdf.loc[:, 'source_geometry_type'] = geom_type_str

    gdf.loc[:, 'source_crs'] = str(metadata.get('crs', 'unknown'))

    # Handle label_count which might be a list or integer
    label_count = metadata.get('label_count', 0)
    if isinstance(label_count, list):
        label_count = sum(label_count) if all(isinstance(x, (int, float)) for x in label_count) else str(label_count)
    gdf.loc[:, 'source_label_count'] = label_count
    
    # Calculate area and centroids
    print(f"   📐 Calculating areas and centroids")

    # Check geometry types in the dataset
    geom_types = gdf.geometry.geom_type.value_counts()
    print(f"   📊 Geometry types: {dict(geom_types)}")

    # Calculate area for polygon geometries
    has_polygons = any(geom_type in ['Polygon', 'MultiPolygon'] for geom_type in geom_types.index)

    if has_polygons:
        # Calculate area in square meters using Web Mercator projection (EPSG:3857)
        # - Provides reasonable area approximations for most geospatial applications
        # - Introduces some distortion, especially at high latitudes (>60°), but acceptable for PV analysis
        # References:
        # - Web Mercator distortion: https://en.wikipedia.org/wiki/Web_Mercator_projection#Distortion
        # - EPSG:3857 specification: https://epsg.io/3857
        # - Snyder, J.P. (1987). Map Projections: A Working Manual. USGS Professional Paper 1395

        print(f"   🔄 Converting to Web Mercator (EPSG:3857) for area calculation")
        try:
            gdf_projected = gdf.to_crs('EPSG:3857')  # Web Mercator for area calculation
            projected_areas = gdf_projected.geometry.area
            print(f"   ✅ Area calculation successful using Web Mercator")

            # Assign areas, handling mixed geometry types
            gdf.loc[:, 'area_m2'] = 0.0  # Initialize all to 0
            polygon_mask = gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])
            gdf.loc[polygon_mask, 'area_m2'] = projected_areas[polygon_mask]

        except Exception as crs_error:
            print(f"   ❌ Web Mercator conversion failed: {crs_error}")
            print(f"   🔄 Setting areas to 0 for all geometries")
            gdf.loc[:, 'area_m2'] = 0.0
    else:
        gdf.loc[:, 'area_m2'] = 0.0

    # Calculate centroids for all geometries
    # For accuracy, calculate centroids from projected coordinates then convert back to WGS84
    print(f"   📍 Calculating centroids using projected coordinates")
    try:
        # Use the same projected GDF if we have it from area calculation
        if has_polygons and 'gdf_projected' in locals():
            projected_centroids = gdf_projected.geometry.centroid
        else:
            # Project just for centroid calculation
            gdf_projected_centroids = gdf.to_crs('EPSG:3857')
            projected_centroids = gdf_projected_centroids.geometry.centroid

        # Convert centroids back to WGS84 for lat/lon values
        centroids_gdf = gpd.GeoDataFrame(geometry=projected_centroids, crs='EPSG:3857')
        centroids_wgs84 = centroids_gdf.to_crs('EPSG:4326')

        gdf.loc[:, 'centroid_lon'] = centroids_wgs84.geometry.x
        gdf.loc[:, 'centroid_lat'] = centroids_wgs84.geometry.y
        print(f"   ✅ Centroid calculation successful")

    except Exception as centroid_error:
        print(f"   ❌ Projected centroid calculation failed: {centroid_error}")
        print(f"   🔄 Using simple WGS84 centroids (may be less accurate)")
        centroids = gdf.geometry.centroid  # Fallback to simple calculation
        gdf.loc[:, 'centroid_lon'] = centroids.x
        gdf.loc[:, 'centroid_lat'] = centroids.y

    # Convert geometry to WKT for storage
    print(f"   📝 Converting geometries to WKT")
    gdf.loc[:, 'geometry_wkt'] = gdf.geometry.to_wkt()

    # Add processing metadata
    gdf.loc[:, 'processed_at'] = pd.Timestamp.now()
    gdf.loc[:, 'source_system'] = 'raw_pv_doi_ingest'
    
    print(f"Processed {len(gdf)} features for {dataset_name}")
    return gdf


@tag(
    data_source="doi",
    processing_stage="collect",
    data_type="arrow_tables",
    hamilton_node_type="collect",
    execution_mode="parallel",
    description="Collect all processed Arrow tables from parallel execution"
)
def collected_arrow_tables(
    dataset_arrow_table: Collect[pa.Table]
) -> List[pa.Table]:
    """
    Collect all processed Arrow tables from parallel execution.

    This function collects all Arrow tables produced by the parallel processing
    of individual datasets, using Hamilton's Collect functionality.

    Args:
        dataset_arrow_table: Collection of processed Arrow tables from dataset_arrow_table() function

    Returns:
        List[pa.Table]: List of all processed Arrow tables
    """
    # Hamilton automatically collects all Arrow tables from parallel processing
    # target_datasets -> dataset_download_path -> dataset_geospatial_files ->
    # dataset_processed_gdf -> dataset_arrow_table -> collected_arrow_tables
    return list(dataset_arrow_table)


@tag(
    data_source="doi",
    processing_stage="store",
    data_type="database_tables",
    hamilton_node_type="sink",
    io_operation="database_write",
    database_type="duckdb",
    description="Store processed datasets individually in DuckDB"
)
def store_individual_datasets(
    collected_arrow_tables: List[pa.Table],
    database_path: str = "../db/eo_pv_data.duckdb"
) -> Dict[str, str]:
    """
    Store each processed dataset individually in DuckDB.

    Each dataset maintains its own schema - staging layer handles harmonization.
    No concatenation at raw layer - preserves all original columns.

    Args:
        collected_arrow_tables: List of processed Arrow tables from collected_arrow_tables() function
        database_path: Path to DuckDB database

    Returns:
        Dict[str, str]: Mapping of dataset names to table information
    """
    if not collected_arrow_tables:
        raise ValueError("No datasets were processed successfully")

    stored_tables = {}

    with duckdb.connect(database_path) as conn:
        # Install required extensions (DuckDB has native Arrow support)
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Create raw schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw_data")

        for table in collected_arrow_tables:
            # Extract dataset name from table metadata
            dataset_name = "unknown_dataset"
            if hasattr(table, 'schema') and table.schema.metadata:
                metadata = table.schema.metadata
                if b'dataset_name' in metadata:
                    dataset_name = metadata[b'dataset_name'].decode('utf-8')

            # Create table name for raw storage
            table_name = f"raw_data.doi_{dataset_name}"

            try:
                # Use helper function following DuckDB Arrow best practices
                row_count = create_duckdb_table_from_arrow(
                    conn=conn,
                    arrow_table=table,
                    table_name=table_name,
                    add_geometry=True
                )
                stored_tables[dataset_name] = f"{table_name} ({row_count} rows)"
                print(f"✅ Stored {dataset_name} as {table_name}: {row_count} rows")

            except Exception as store_error:
                print(f"❌ Failed to store {dataset_name}: {store_error}")
                continue

    print(f"📊 Stored {len(stored_tables)} datasets individually in raw_data schema")
    return stored_tables


@tag(data_source="combined", processing_stage="load", dbt_source="raw")
def load_raw_to_duckdb(
    combined_datasets: pa.Table,
    database_path: str = "../db/eo_pv_data.duckdb",
    schema_name: str = "raw_data"
) -> str:
    """
    Load processed datasets into DuckDB raw schema for dbt consumption.

    This creates the raw data tables that dbt staging models will reference.
    Uses Arrow table for efficient zero-copy loading into DuckDB.
    """
    import duckdb

    if len(combined_datasets) == 0:
        raise ValueError("No datasets to load")

    # Connect to DuckDB and install required extensions
    conn = duckdb.connect(database_path)
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Create raw schema for Hamilton-managed data
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    # Use helper function following DuckDB Arrow best practices
    table_name = f"{schema_name}.doi_pv_features"
    count = create_duckdb_table_from_arrow(
        conn=conn,
        arrow_table=combined_datasets,
        table_name=table_name,
        add_geometry=True
    )

    conn.close()

    print(f"Loaded {count} records into {database_path}::{table_name}")
    print(f"dbt can now reference this as: source('raw_data', 'doi_pv_features')")
    return f"Successfully loaded {count} records to raw schema"


# Main pipeline orchestration function for Hamilton
def run_doi_pipeline(
    database_path: str = "../db/eo_pv_data.duckdb",
    export_geoparquet: bool = True,
    use_parallel: bool = True,
    use_cache: bool = True,
    force_download: bool = False,
    max_mb: int = 250
) -> str:
    """
    Run the complete DOI dataset pipeline using Hamilton with proper Builder patterns.

    This function demonstrates Hamilton best practices for:
    - Builder pattern usage for driver creation
    - Proper configuration management
    - Parallel vs sequential execution patterns
    - Integration with dbt workflows

    Args:
        database_path: Path to DuckDB database
        export_geoparquet: Whether to export to GeoParquet format
        use_parallel: Whether to use parallel processing
        use_cache: Whether to use download caching
        force_download: Force re-download even if cached
        max_mb: Maximum file size in MB for downloads

    Returns:
        str: Status message indicating pipeline completion
    """
    from hamilton.execution import executors
    from hamilton.base import DictResult

    # Import this module for Hamilton
    import data_loaders.raw_pv_doi_ingest as pipeline_module

    # Create Hamilton driver configuration following best practices
    config = {
        "database_path": database_path,
        "export_geoparquet": export_geoparquet,
        "use_cache": use_cache,
        "force_download": force_download,
        "max_mb": max_mb
    }

    # Create Hamilton driver using Builder pattern (recommended approach)
    print(f"🔧 Creating Hamilton driver with {'parallel' if use_parallel else 'sequential'} execution...")

    try:
        if use_parallel:
            # Use Builder pattern with V2 executor for parallel processing
            # This enables Parallelizable and Collect functionality
            builder = (
                driver.Builder()
                .enable_dynamic_execution(allow_experimental_mode=True)
                .with_modules(pipeline_module)
                .with_config(config)
                .with_local_executor(executors.SynchronousLocalTaskExecutor())
            )
            dr = builder.build()
            print("✅ Created Hamilton V2 executor with parallel processing capabilities")

            # Define target variables for parallel execution
            final_vars = ["store_individual_datasets"]

        else:
            # Use standard driver for sequential processing
            # This is more stable and easier to debug
            dr = driver.Builder().with_modules(pipeline_module).with_config(config).build()
            print("✅ Created Hamilton standard executor for sequential processing")
            return run_sequential_pipeline(dr, database_path)

    except Exception as builder_error:
        print(f"❌ Failed to create Hamilton driver: {builder_error}")
        print("🔄 Falling back to sequential processing with basic driver...")

        # Final fallback to basic driver
        dr = driver.Driver(config, pipeline_module, adapter=DictResult())
        print("✅ Created basic Hamilton driver (sequential processing)")
        return run_sequential_pipeline(dr, database_path)

    # Execute the Hamilton pipeline
    try:
        print("🚀 Starting Hamilton DOI pipeline execution...")
        print(f"   Target variables: {final_vars}")
        print(f"   Configuration: {config}")

        # Execute the complete pipeline with proper error handling
        results = dr.execute(final_vars)

        # Extract and validate results
        stored_tables = results["store_individual_datasets"]

        if not stored_tables:
            raise ValueError("No datasets were successfully processed and stored")

        status_message = f"Successfully stored {len(stored_tables)} datasets individually"
        print(f"✅ {status_message}")

        # Print detailed results
        print("📊 Stored datasets:")
        for dataset_name, table_info in stored_tables.items():
            print(f"   • {dataset_name}: {table_info}")

        # Print summary of raw_data schema
        print_raw_data_summary(database_path)

        return status_message

    except Exception as pipeline_error:
        print(f"❌ Hamilton pipeline execution failed: {pipeline_error}")
        print(f"   Error type: {type(pipeline_error).__name__}")
        print(f"   Configuration used: {config}")
        raise


def run_sequential_pipeline(
    dr: driver.Driver,
    database_path: str
) -> str:
    """
    Run pipeline sequentially without parallel processing.

    This processes datasets one by one and stores each individually.
    """
    print("Running sequential pipeline (processing datasets one by one)...")
    # Get dataset metadata and targets separately to avoid DataFrame conversion issues
    metadata_result = dr.execute(["dataset_metadata"])
    metadata = metadata_result["dataset_metadata"]

    targets_result = dr.execute(["target_datasets_list"])
    targets = targets_result["target_datasets_list"]

    print(f"Processing {len(targets)} datasets sequentially...")

    # Process each dataset individually
    processed_tables = []
    for dataset_name in targets:
        try:
            print(f"\n{'='*80}")
            print(f"Processing dataset: {dataset_name}")
            print(f"{'='*80}")
            # Process single dataset using the new Hamilton functions
            # Manually chain the functions for sequential processing
            download_path = download_doi_dataset(dataset_name, metadata)
            geospatial_files = extract_geospatial_files(download_path, dataset_name, metadata)
            processed_gdf = process_geospatial_data(geospatial_files, dataset_name, metadata)

            # Convert to Arrow table
            print(f"   📝 Converting to Arrow using GeoPandas to_arrow() method")
            try:
                geopandas_arrow = processed_gdf.to_arrow(index=False, geometry_encoding='WKB')
                arrow_table = pa.table(geopandas_arrow)
                print(f"   ✅ Converted to Arrow table with WKB geometry: {arrow_table.num_rows} rows, {len(arrow_table.columns)} columns")
            except Exception as arrow_error:
                print(f"   ❌ Arrow conversion failed: {arrow_error}")
                print(f"   🔄 Final fallback to manual WKT conversion")
                arrow_df = processed_gdf.copy()
                if 'geometry' in arrow_df.columns:
                    if 'geometry_wkt' not in arrow_df.columns:
                        arrow_df['geometry_wkt'] = arrow_df['geometry'].to_wkt()
                    arrow_df = arrow_df.drop(columns=['geometry'])
                arrow_table = pa.Table.from_pandas(arrow_df, preserve_index=False)
                print(f"   ✅ Manual WKT conversion successful: {arrow_table.num_rows} rows, {len(arrow_table.columns)} columns")

            # Add dataset name to table metadata
            metadata_dict = {b'dataset_name': dataset_name.encode('utf-8')}
            arrow_table = arrow_table.replace_schema_metadata(metadata_dict)
            result = arrow_table
            processed_tables.append(result)
            print(f"✓ Completed {dataset_name}: {len(result)} records")
        except Exception as e:
            print(f"✗ Failed {dataset_name}: {e}")
            print(f"  Error details: {str(e)}")
            continue

    if not processed_tables:
        raise ValueError("No datasets processed successfully")

    # Store each dataset individually (no concatenation
    stored_tables = {}

    with duckdb.connect(database_path) as conn:
        # Install required extensions (DuckDB has native Arrow support)
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Create raw schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw_data")

        for i, table in enumerate(processed_tables):
            # Extract dataset name from table metadata
            dataset_name = f"dataset_{i}"  # Fallback
            if hasattr(table, 'schema') and table.schema.metadata:
                metadata = table.schema.metadata
                if b'dataset_name' in metadata:
                    dataset_name = metadata[b'dataset_name'].decode('utf-8')

            # Create table name for raw storage
            table_name = f"raw_data.doi_{dataset_name}"

            try:
                # Use helper function following DuckDB Arrow best practices
                row_count = create_duckdb_table_from_arrow(
                    conn=conn,
                    arrow_table=table,
                    table_name=table_name,
                    add_geometry=True
                )
                stored_tables[dataset_name] = f"{table_name} ({row_count} rows)"
                print(f"✅ Stored {dataset_name} as {table_name}: {row_count} rows")

            except Exception as store_error:
                print(f"❌ Failed to store {dataset_name}: {store_error}")
                continue

    status_message = f"Successfully stored {len(stored_tables)} datasets individually"
    print(f"📊 {status_message}")

    # Print summary of raw_data schema
    print_raw_data_summary(database_path)

    return status_message


if __name__ == "__main__":
    """
    Example usage of the Hamilton DOI pipeline.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Run Hamilton DOI dataset pipeline")
    parser.add_argument("--database", default="../db/eo_pv_data.duckdb",
                       help="Path to DuckDB database")
    parser.add_argument("--no-geoparquet", action="store_true",
                       help="Skip GeoParquet export")
    parser.add_argument("--sequential", action="store_true",
                       help="Use sequential processing instead of parallel (better for debugging)")
    parser.add_argument("--no-cache", action="store_true",
                       help="Disable download caching (always download fresh)")
    parser.add_argument("--force-download", action="store_true",
                       help="Force re-download even if cached (updates cache)")
    parser.add_argument("--clear-cache", action="store_true",
                       help="Clear download cache and exit")

    args = parser.parse_args()

    # Handle cache clearing
    if args.clear_cache:
        clear_all_cache()
        exit(0)

    # Show cache status
    if not args.no_cache:
        cache = load_download_cache()
        if cache:
            print(f"📦 Found {len(cache)} cached datasets:")
            for dataset_name, cache_dir in cache.items():
                cache_size = sum(f.stat().st_size for f in Path(cache_dir).rglob("*") if f.is_file())
                print(f"   {dataset_name}: {cache_dir} ({cache_size / 1024 / 1024:.1f} MB)")
        else:
            print("📦 No cached datasets found")

    result = run_doi_pipeline(
        database_path=args.database,
        export_geoparquet=not args.no_geoparquet,
        use_parallel=not args.sequential,
        use_cache=not args.no_cache,
        force_download=args.force_download
    )

    print(f"Pipeline completed: {result}")
