#!/usr/bin/env python3
"""
Hamilton-based DOI dataset pipeline for EO PV data ingestion.

This replaces the previous dlt-based doi_dataset_pipeline.py with a simpler,
more maintainable Hamilton approach that provides fine-grained lineage and self-documenting pipelines.

Key improvements:
- Uses doi_manifest.json for dataset configuration
- Implements Hamilton's Parallelizable for concurrent processing
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

# Cache configuration
CACHE_FILE = Path("./hamilton_download_cache.json")

# Import our existing utilities
import sys
sys.path.append(str(Path(__file__).parent))


def create_duckdb_table_from_arrow(
    conn: 'duckdb.DuckDBPyConnection',
    arrow_table: pa.Table,
    table_name: str,
    add_geometry: bool = True
) -> int:
    """
    Create DuckDB table from Arrow table using zero-copy conversion to GeoPandas.
    
    Avoids Arrow metadata serialization issues by converting back to GeoPandas
    and using DuckDB's excellent native GeoPandas support.

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


def load_download_cache() -> Dict[str, str]:
    # Load the download cache from JSON file. Returns dataset_name -> cache_directory maps
    if not CACHE_FILE.exists():
        return {}

    try:
        with open(CACHE_FILE, 'r') as f:
            cache = json.load(f)

        # Validate that cached directories still exist
        valid_cache = {}
        for dataset_name, cache_dir in cache.items():
            if Path(cache_dir).exists():
                valid_cache[dataset_name] = cache_dir
            else:
                print(f"   🗑️  Cached directory no longer exists: {cache_dir}")
        # Update cache file if some entries were invalid
        if len(valid_cache) != len(cache):
            save_download_cache(valid_cache)

        return valid_cache

    except Exception as e:
        print(f"   ⚠️  Error loading cache: {e}")
        return {}


def save_download_cache(cache: Dict[str, str]) -> None:
    # Save the download cache to JSON file.
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"   ⚠️  Error saving cache: {e}")


def get_cached_tempdir(dataset_name: str) -> Optional[str]:
    """
    Get cached temporary directory for a dataset if it exists and is valid.

    Args:
        dataset_name: Name of the dataset

    Returns:
        Path to the cached temp directory, or None if not cached/invalid
    """
    cache = load_download_cache()
    if dataset_name not in cache:
        return None

    temp_dir = cache[dataset_name]
    if not Path(temp_dir).exists():
        # Remove invalid cache entry
        cleanup_cache_entry(dataset_name)
        return None

    # Verify cache has files
    cached_files = list(Path(temp_dir).rglob("*"))
    cached_files = [f for f in cached_files if f.is_file()]
    if not cached_files:
        cleanup_cache_entry(dataset_name)
        return None

    return temp_dir


def cache_tempdir(dataset_name: str, temp_dir: str) -> None:
    # Cache a temporary directory location 
    cache = load_download_cache()
    cache[dataset_name] = temp_dir
    save_download_cache(cache)
    print(f"   📝 Cached temp directory for {dataset_name}: {temp_dir}")


def cleanup_cache_entry(dataset_name: str) -> None:
    """
    Remove a dataset from the cache and clean up its directory.

    Args:
        dataset_name: Name of the dataset to remove from cache
    """
    cache = load_download_cache()

    if dataset_name in cache:
        cache_dir = Path(cache[dataset_name])
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir, ignore_errors=True)
            print(f"   🗑️  Cleaned up cache directory: {cache_dir}")

        del cache[dataset_name]
        save_download_cache(cache)
        print(f"   📝 Removed {dataset_name} from cache")


@tag(data_source="doi", processing_stage="raw")
def dataset_metadata() -> Dict[str, Dict[str, Any]]:
    """Load dataset metadata configuration from doi_manifest.json."""
    manifest_path = Path(__file__).parent / "doi_manifest.json"

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


@tag(data_source="doi", processing_stage="config")
def target_datasets() -> Parallelizable[str]:
    """
    Generate dataset names for parallel processing.

    Uses Hamilton's Parallelizable to enable concurrent dataset processing.
    Processes all available vector datasets by default.
    """
    manifest = dataset_metadata()

    # Process all available vector datasets
    available_datasets = list(manifest.keys())

    print(f"Processing {len(available_datasets)} datasets in parallel")
    for dataset_name in available_datasets:
        yield dataset_name


@tag(data_source="doi", processing_stage="config", execution_mode="sequential")
def target_datasets_list() -> List[str]:
    """
    Generate dataset names for sequential processing.

    Returns a simple list instead of Parallelizable for standard executor.
    Processes all available vector datasets by default.
    """
    manifest = dataset_metadata()

    # Process all available vector datasets
    available_datasets = list(manifest.keys())

    print(f"Processing {len(available_datasets)} datasets sequentially")
    return available_datasets


@tag(data_source="doi", processing_stage="download")
def download_doi_dataset(
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250,  # Increased for large datasets like Stowell
    use_cache: bool = True,  # Enable caching by default
    force_download: bool = False  # Force re-download even if cached
) -> str:
    """
    Download DOI dataset to directory with caching support.

    Args:
        dataset_name: Name of the dataset
        dataset_metadata: Metadata dictionary
        max_mb: Maximum file size in MB
        use_cache: Whether to use cached downloads
        force_download: Force re-download even if cached

    Returns:
        Path to downloaded/extracted files
    """
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

    # Always create a new temporary directory for downloads
    download_dir = tempfile.mkdtemp(prefix=f"{dataset_name}_")
    print(f"📥 Downloading {dataset_name} to temporary directory: {download_dir}")

    try:
        if metadata["repo"] == "github":
            # Handle GitHub repositories using existing fetch_from_github function
            print(f"📥 Downloading GitHub dataset {dataset_name}...")
            print(f"   Repository URL: {metadata['doi']}")
            print(f"   Target directory: {download_dir}")

            # Import the existing GitHub fetch function
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

            # Call datahugger with timeout and progress monitoring
            import signal
            import time

            def timeout_handler(signum, frame):
                # Signal handler for download timeout
                _ = signum, frame  # Suppress unused parameter warnings
                raise TimeoutError(f"Download timeout after 300 seconds for {dataset_name}")

            try:
                # Set timeout for download (5 minutes)
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(300)  # 5 minute timeout

                start_time = time.time()
                print(f"   📡 Calling datahugger.get()...")

                datahugger.get(
                    metadata["doi"],
                    output_folder=download_dir,
                    max_file_size=max_mb * 1024 * 1024  # Convert MB to bytes
                )

                # Cancel timeout
                signal.alarm(0)

                download_time = time.time() - start_time
                print(f"   ⏱️  Download completed in {download_time:.1f} seconds")

                # Check if download was successful
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

            except TimeoutError as timeout_error:
                signal.alarm(0)  # Cancel timeout
                print(f"⏰ Download timeout for {dataset_name}: {timeout_error}")
                raise
            except Exception as download_error:
                signal.alarm(0)  # Cancel timeout
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


@tag(data_source="doi", processing_stage="extract")
def extract_geospatial_files(
    download_path: str,
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]]
) -> List[str]:
    """
    Extract and locate geospatial files from downloaded dataset.

    Returns list of paths to geospatial files (GeoJSON, SHP, GPKG, JSON).
    """
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

            # Check include patterns (all must match if specified)
            if include_patterns:
                if use_regex:
                    # Use regex matching
                    include_match = all(
                        re.search(pattern, file_path_str, re.IGNORECASE)
                        for pattern in include_patterns
                    )
                else:
                    # Use simple substring matching (case-insensitive)
                    include_match = all(
                        pattern.lower() in file_path_str.lower()
                        for pattern in include_patterns
                    )

                if not include_match:
                    print(f"     ❌ Excluded (missing include pattern): {file_name}")
                    continue

            # Check exclude patterns (none should match)
            if exclude_patterns:
                if use_regex:
                    # Use regex matching
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

    # Convert to string paths
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


@tag(data_source="doi", processing_stage="process")
def process_single_dataset(
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]]
) -> pa.Table:
    """
    Complete processing pipeline for a single dataset.

    Downloads, extracts, processes, and converts to Arrow format
    for efficient downstream processing.
    """
    # Download dataset
    download_path = download_doi_dataset(dataset_name, dataset_metadata)

    # Extract geospatial files
    geospatial_files = extract_geospatial_files(download_path, dataset_name, dataset_metadata)

    # Process the data
    processed_gdf = process_geospatial_data(geospatial_files, dataset_name, dataset_metadata)

    # Convert to Arrow for efficient data exchange
    # Use GeoPandas' native to_arrow() method for proper geometry handling
    print(f"   📝 Converting to Arrow using GeoPandas to_arrow() method")

    # Determine if the download_path came from cache and shouldn't be deleted
    # This assumes download_doi_dataset is the only source for download_path
    # and that its parameters (use_cache, force_download) are accessible or implicitly handled.
    # For simplicity, we'll assume a config parameter `keep_cached_downloads` could control this.
    # A more robust way would be for download_doi_dataset to return a flag.
    # For this example, let's assume `dataset_metadata[dataset_name].get('keep_cache', False)`
    # or a global config `keep_cached_downloads`.
    # Let's simplify: if force_download was false and path was from cache, don't delete.
    # This requires knowing if download_path was a cached path.
    # The current structure makes this tricky without passing more state.
    # A simpler approach: the cache is for *temporary* resumption.
    # If persistent cache is desired, `download_doi_dataset` should download to a *persistent* cache location.
    # And `process_single_dataset` would read from there.
    # The current cleanup is okay if cache is only for single-run resiliency.
    # However, if we want to make it more persistent based on common expectations:
    try:
        # Use GeoPandas to_arrow() with WKB encoding - this is already GeoArrow compatible!
        print(f"   📝 Converting to Arrow with WKB geometry encoding")
        geopandas_arrow = processed_gdf.to_arrow(index=False, geometry_encoding='WKB')

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
        arrow_df = processed_gdf.copy()
        if 'geometry' in arrow_df.columns:
            if 'geometry_wkt' not in arrow_df.columns:
                arrow_df['geometry_wkt'] = arrow_df['geometry'].to_wkt()
            arrow_df = arrow_df.drop(columns=['geometry'])

        arrow_table = pa.Table.from_pandas(arrow_df, preserve_index=False)
        print(f"   ✅ Manual WKT conversion successful: {arrow_table.num_rows} rows, {len(arrow_table.columns)} columns")

    # Add dataset name to table metadata for identification during storage
    metadata = {b'dataset_name': dataset_name.encode('utf-8')}
    arrow_table = arrow_table.replace_schema_metadata(metadata)
    print(f"   📝 Added dataset metadata: {dataset_name}")

    # Clean up temporary directory only if it wasn't a persistent cache hit
    # This logic is simplified; a robust solution would involve `download_doi_dataset`
    # returning information about whether the path is from a persistent cache
    # and if it should be kept.
    # For now, we assume `force_download` implies it's okay to clean if it was a fresh download.
    # if not (use_cache and not force_download and get_cached_tempdir(dataset_name) == download_path):
    # import shutil
    # shutil.rmtree(download_path, ignore_errors=True)
    # The above comment block shows the complexity. The current behavior is simpler:
    # cache helps resume a single pipeline execution, but files are cleaned up.
    # To make cache persistent across runs, the download_doi_dataset should download to a non-temp,
    # managed cache area, and process_single_dataset should not clean it.

    print(f"✓ Completed processing {dataset_name}: {arrow_table.num_rows} records")
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

    # Add dataset metadata to the GeoDataFrame
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
        # Calculate area in square meters using Web Mercator projection
        #
        # Web Mercator (EPSG:3857) approach for area calculation:
        # - Provides reasonable area approximations for most geospatial applications
        # - Widely compatible with downstream tools and libraries
        # - Introduces some distortion, especially at high latitudes (>60°), but acceptable for PV analysis
        # - Follows established pattern from utils/fetch_and_preprocess.py
        #
        # References:
        # - Web Mercator distortion: https://en.wikipedia.org/wiki/Web_Mercator_projection#Distortion
        # - EPSG:3857 specification: https://epsg.io/3857
        # - Snyder, J.P. (1987). Map Projections: A Working Manual. USGS Professional Paper 1395
        #
        # Future enhancement: Validate against datasets with ground-truth area columns
        # for accuracy assessment and potential upgrade to geodesic calculations

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
    gdf.loc[:, 'source_system'] = 'hamilton_doi_pipeline'
    
    print(f"Processed {len(gdf)} features for {dataset_name}")
    return gdf


@tag(data_source="doi", processing_stage="export", dbt_source="raw")
def export_combined_geoparquet(
    combined_datasets: pa.Table
) -> str:
    """
    Export combined datasets to GeoParquet format for dbt ingestion.

    This creates the raw data layer that dbt staging models will consume.
    Uses Arrow table for efficient I/O operations.
    """
    # Ensure output directory exists
    output_dir = Path("./datasets/raw/geoparquet")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Export to single combined parquet file
    output_path = output_dir / "combined_doi_pv_features.parquet"

    # Write Arrow table to Parquet
    import pyarrow.parquet as pq
    pq.write_table(combined_datasets, output_path, compression="snappy")

    print(f"Exported {len(combined_datasets)} records to {output_path}")
    return str(output_path)


@tag(data_source="doi", processing_stage="parallel")
def parallel_dataset_processing(
    target_datasets: Parallelizable[str],
    dataset_metadata: Dict[str, Dict[str, Any]]
) -> pa.Table:
    """
    Process individual dataset using Hamilton's parallel execution.

    This function processes each dataset yielded by target_datasets
    in parallel, leveraging Hamilton's Parallelizable functionality.
    """
    return process_single_dataset(target_datasets, dataset_metadata)


@tag(data_source="doi", processing_stage="store")
def store_individual_datasets(
    parallel_dataset_processing: Collect[pa.Table],
    database_path: str = "./eo_pv_data.duckdb"
) -> Dict[str, str]:
    """
    Store each processed dataset individually in DuckDB.

    Each dataset maintains its own schema - staging layer handles harmonization.
    No concatenation at raw layer - preserves all original columns.
    """
    if not parallel_dataset_processing:
        raise ValueError("No datasets were processed successfully")

    stored_tables = {}

    with duckdb.connect(database_path) as conn:
        # Install required extensions (DuckDB has native Arrow support)
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Create raw schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw_data")

        for table in parallel_dataset_processing:
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


@tag(data_source="overture", processing_stage="query")
def overture_admin_boundaries(
    bbox: Optional[List[float]] = None
) -> pd.DataFrame:
    """
    Query Overture Maps administrative boundaries directly from S3.

    Uses DuckDB to query Overture Maps Parquet files without downloading.
    Returns view/CTE only to minimize storage for free tier usage.
    """

    # Connect to DuckDB and install required extensions
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("INSTALL h3; LOAD h3;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    # Overture Maps S3 path for administrative boundaries
    overture_s3_path = "s3://overturemaps-us-west-2/release/2024-11-13.0/theme=admins/type=*/*.parquet"

    # Build query with optional bbox filtering
    query = f"""
    SELECT
        id,
        names.primary as name,
        admin_level,
        ST_AsText(geometry) as geometry_wkt,
        bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax
    FROM read_parquet('{overture_s3_path}')
    WHERE admin_level <= 2  -- Countries and major subdivisions only
    """

    if bbox:
        # Add spatial filter if bbox provided
        xmin, ymin, xmax, ymax = bbox
        query += f"""
        AND bbox.xmin <= {xmax} AND bbox.xmax >= {xmin}
        AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin}
        """

    query += " LIMIT 1000"  # Reasonable limit for free tier

    try:
        result = conn.execute(query).fetchdf()
        print(f"Retrieved {len(result)} administrative boundaries from Overture Maps")
        return result
    except Exception as e:
        print(f"Error querying Overture Maps: {e}")
        # Return empty DataFrame with expected schema
        return pd.DataFrame(columns=[
            'id', 'name', 'admin_level', 'geometry_wkt',
            'xmin', 'ymin', 'xmax', 'ymax'
        ])
    finally:
        conn.close()


@tag(data_source="combined", processing_stage="load", dbt_source="raw")
def load_raw_to_duckdb(
    combined_datasets: pa.Table,
    database_path: str = "./eo_pv_data.duckdb",
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


def print_raw_data_summary(database_path: str) -> None:
    """
    Prints a summary of tables and row counts in the raw_data schema.

    Args:
        database_path: Path to the DuckDB database file.
    """
    print(f"\n{'='*80}")
    print(f"📊 Summary of tables in 'raw_data' schema ({database_path}):")
    print(f"{'='*80}")
    try:
        with duckdb.connect(database_path, read_only=True) as conn:
            # Ensure spatial extension is available for any geometry checks if needed,
            # though not strictly for count.
            try:
                conn.execute("INSTALL spatial; LOAD spatial;")
            except Exception:
                pass # Ignore if already loaded or fails in read-only, not critical for count

            tables_df = conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'raw_data'
                ORDER BY table_name;
            """).fetchdf()

            if not tables_df.empty:
                for table_name in tables_df['table_name']:
                    count = conn.execute(f"SELECT COUNT(*) FROM raw_data.\"{table_name}\"").fetchone()[0]
                    print(f"   - raw_data.{table_name}: {count} rows")
            else:
                print("   No tables found in 'raw_data' schema.")
    except Exception as e:
        print(f"   Could not retrieve summary for 'raw_data' schema: {e}")
    print(f"{'='*80}\n")

# Main pipeline orchestration function for Hamilton
def run_doi_pipeline(
    database_path: str = "./eo_pv_data.duckdb",
    export_geoparquet: bool = True,
    use_parallel: bool = True,
    use_cache: bool = True,
    force_download: bool = False
) -> str:
    """
    Run the complete DOI dataset pipeline using Hamilton with V2 executor.

    Args:
        database_path: Path to DuckDB database
        export_geoparquet: Whether to export to GeoParquet format
        use_parallel: Whether to use parallel processing

    Returns:
        Status message
    """
    from hamilton.execution import executors

    # Import this module for Hamilton
    import hamilton_doi_pipeline as pipeline_module

    # Create Hamilton driver configuration
    config = {
        "database_path": database_path,
        "export_geoparquet": export_geoparquet,
        "use_cache": use_cache,
        "force_download": force_download
    }

    if use_parallel:
        try:
            # Use Builder pattern with V2 executor for parallel processing
            builder = (
                driver.Builder()
                .enable_dynamic_execution(allow_experimental_mode=True)
                .with_modules(pipeline_module)
                .with_config(config)
                .with_local_executor(executors.SynchronousLocalTaskExecutor())
            )
            dr = builder.build()
            print("✓ Using Hamilton V2 executor with parallel processing")

        except Exception as e:
            print(f"⚠️  Failed to create parallel executor: {e}")
            print("   Falling back to sequential processing")
            use_parallel = False

    if not use_parallel:
        # Fallback to standard executor (sequential processing)
        from hamilton.base import DictResult
        dr = driver.Driver(config, pipeline_module, adapter=DictResult())
        print("✓ Using Hamilton standard executor (sequential processing)")
        return run_sequential_pipeline(dr, database_path, export_geoparquet)

    try:
        # Define what we want to execute - store individual datasets
        final_vars = ["store_individual_datasets"]

        # Execute the complete pipeline
        print("Starting Hamilton DOI pipeline...")
        results = dr.execute(final_vars)

        # Extract results
        stored_tables = results["store_individual_datasets"]

        status_message = f"Successfully stored {len(stored_tables)} datasets individually"
        print(status_message)

        # Print summary of raw_data schema
        print_raw_data_summary(database_path)

        return status_message

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise


def run_sequential_pipeline(
    dr: driver.Driver,
    database_path: str,
    export_geoparquet: bool = False  # Not used in individual storage approach
) -> str:
    """
    Run pipeline sequentially without parallel processing.

    This processes datasets one by one and stores each individually.
    """
    # Note: export_geoparquet parameter kept for compatibility but not used
    # in individual storage approach
    _ = export_geoparquet  # Suppress unused parameter warning

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
            # Process single dataset
            result = process_single_dataset(dataset_name, metadata)
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
    parser.add_argument("--database", default="./eo_pv_data.duckdb",
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
        import shutil
        if CACHE_FILE.exists():
            cache = load_download_cache()
            for dataset_name, temp_dir in cache.items():
                if Path(temp_dir).exists():
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    print(f"🗑️  Removed cached temp directory: {temp_dir}")
            CACHE_FILE.unlink()
            print(f"🗑️  Removed cache file: {CACHE_FILE}")
        print("✅ Cache cleared successfully")
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
