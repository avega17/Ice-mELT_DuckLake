"""
Hamilton dataflow for scientific publication (DOI) PV location datasets.

Core sequence:
doi_metadata -> download in parallel -> load with geopandas -> convert to geoarrow -> save to duckDB (WKB) + geoparquet (GeoArrow)

Uses hybrid approach:
- GeoArrow encoding between Hamilton nodes for efficient spatial processing
- WKB conversion at DuckDB storage boundary for compatibility
- Native GeoArrow for optimal GeoParquet exports

Keeps individual datasets almost as-is, delegates schema standardization and data validation to dbt staging and downstream layers.
"""

from __future__ import annotations

import json
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, List

import geopandas as gpd
import pyarrow as pa
import duckdb
import datahugger

# Simple import - let it fail if not available
import geoarrow.pandas as _  # Register geoarrow accessor

from hamilton.function_modifiers import cache, tag, config

# Import Hamilton types directly (following working pattern from raw_pv_doi_ingest.py)
from hamilton.htypes import Parallelizable, Collect

# Import storage helper functions
from ._doi_pv_helpers_storage import (
    _geoarrow_table,
    _duckdb_table_from_geoarrow,
    _geoparquet_export,
    _storage_result,
    _apply_file_filters
)





# =============================================================================
# HAMILTON DATAFLOW NODES
# =============================================================================

@cache(behavior="recompute")  # Always reload to catch manifest updates
@tag(stage="metadata", data_type="config")
def doi_metadata(manifest_path: str = "doi_manifest.json") -> Dict[str, Dict[str, Any]]:
    """Load DOI dataset metadata from manifest file."""
    manifest_file = Path(manifest_path)
    if not manifest_file.is_absolute():
        manifest_file = Path(__file__).parent.parent / manifest_path
    
    if not manifest_file.exists():
        raise FileNotFoundError(f"DOI manifest not found: {manifest_file}")
    
    with open(manifest_file, 'r') as f:
        return json.load(f)

@tag(stage="metadata", data_type="list", execution_mode="parallel")
@config.when(execution_mode="parallel")
def dataset_names__parallel(doi_metadata: Dict[str, Dict[str, Any]]) -> Parallelizable[str]:
    """Extract dataset names for parallel processing using Hamilton's Parallelizable."""
    # Filter out skipped datasets
    available_datasets = [
        name for name, metadata in doi_metadata.items()
        if not metadata.get("skip", False)
    ]
    skipped_count = len(doi_metadata) - len(available_datasets)

    print(f"Processing {len(available_datasets)} datasets in parallel")
    if skipped_count > 0:
        skipped_names = [name for name, metadata in doi_metadata.items() if metadata.get("skip", False)]
        print(f"Skipping {skipped_count} datasets marked with 'skip': true")
        print(f"   Skipped datasets: {', '.join(skipped_names)}")

    for dataset_name in available_datasets:
        yield dataset_name


@tag(stage="metadata", data_type="list", execution_mode="sequential")
@config.when(execution_mode="sequential")
def dataset_names__sequential(doi_metadata: Dict[str, Dict[str, Any]]) -> List[str]:
    """Extract dataset names for sequential processing as a simple list."""
    # Filter out skipped datasets
    available_datasets = [
        name for name, metadata in doi_metadata.items()
        if not metadata.get("skip", False)
    ]
    skipped_count = len(doi_metadata) - len(available_datasets)

    print(f"Processing {len(available_datasets)} datasets sequentially")
    if skipped_count > 0:
        skipped_names = [name for name, metadata in doi_metadata.items() if metadata.get("skip", False)]
        print(f"Skipping {skipped_count} datasets marked with 'skip': true")
        print(f"   Skipped datasets: {', '.join(skipped_names)}")

    return available_datasets

@tag(stage="download", data_type="path")
@cache(behavior="default")  # Cache download paths - let Hamilton auto-detect format for strings
@config.when(execution_mode="parallel")
def dataset_download_path__parallel(
    dataset_names: str,  # Individual dataset name from parallel processing
    doi_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250
) -> str:
    """Download individual dataset and return path."""
    metadata = doi_metadata[dataset_names]
    
    # Create temp directory for this dataset
    download_dir = tempfile.mkdtemp(prefix=f"doi_{dataset_names}_")
    
    try:
        if metadata["repo"] == "github":
            # Handle GitHub URLs - distinguish between repos and raw files
            doi_url = metadata["doi"]

            if "raw.githubusercontent.com" in doi_url:
                # Raw GitHub file - download directly
                import urllib.request
                from pathlib import Path

                print(f"   📥 Downloading raw GitHub file: {doi_url}")
                filename = Path(doi_url).name
                if not filename.endswith(('.geojson', '.json', '.shp', '.gpkg', '.kml', '.gml')):
                    # If no extension, try to infer from URL or default to .geojson
                    filename = f"{dataset_names}.geojson"

                file_path = Path(download_dir) / filename
                urllib.request.urlretrieve(doi_url, file_path)
                print(f"   ✅ Downloaded {filename} to {download_dir}")

            else:
                # GitHub repository - clone it
                import subprocess
                repo_url = doi_url.replace("https://github.com/", "")
                subprocess.run([
                    "git", "clone", "--depth", "1",
                    f"https://github.com/{repo_url}.git",
                    download_dir
                ], check=True, capture_output=True)
                print(f"   ✅ Cloned repository to {download_dir}")
        else:
            # Use datahugger for DOI downloads
            datahugger.get(
                metadata["doi"],
                output_folder=download_dir,
                max_file_size=max_mb * 1024 * 1024
            )
        
        return download_dir

    except Exception as e:
        shutil.rmtree(download_dir, ignore_errors=True)
        raise RuntimeError(f"Download failed for {dataset_names}: {e}")


@config.when(execution_mode="sequential")
@tag(stage="download", data_type="path")
def dataset_download_path__sequential(
    dataset_names: List[str],  # Not used in sequential mode
    doi_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250
) -> str:
    """Placeholder for sequential mode - actual processing happens in collected_arrow_tables__sequential."""
    # This is a placeholder since sequential processing happens all at once
    # Return empty string as this won't be used
    return ""

@tag(stage="process", data_type="geodataframe")
# Note: Caching disabled for GeoDataFrames due to potential GeoArrow serialization issues
@config.when(execution_mode="parallel")
def processed_geodataframe__parallel(
    dataset_download_path: str,  # Individual path from parallel processing
    dataset_names: str,  # Individual dataset name from parallel processing
    doi_metadata: Dict[str, Dict[str, Any]]  # DOI metadata for file filtering
) -> gpd.GeoDataFrame:
    """Load and minimally process geospatial data, keeping original schema."""
    download_path = Path(dataset_download_path)
    
    # Find geospatial files using file_filters if available
    metadata = doi_metadata[dataset_names]
    file_filters = metadata.get("file_filters", {})

    # Find all potential geospatial files
    geo_extensions = {'.shp', '.geojson', '.gpkg', '.kml', '.gml', '.json'}
    all_geo_files = []

    for ext in geo_extensions:
        all_geo_files.extend(download_path.rglob(f"*{ext}"))

    if not all_geo_files:
        raise ValueError(f"No geospatial files found in {download_path}")

    # Apply file filters if specified
    geo_files = _apply_file_filters(all_geo_files, file_filters)

    if not geo_files:
        print(f"   ⚠️  No files passed filters, using all found files")
        geo_files = all_geo_files

    print(f"   📁 Found {len(geo_files)} geospatial files after filtering:")
    for f in geo_files:
        print(f"      - {f.name} ({f.stat().st_size / 1024:.1f} KB)")

    # Load the largest geospatial file
    main_file = max(geo_files, key=lambda f: f.stat().st_size)
    print(f"   📄 Processing main file: {main_file.name}")
    
    # Load with geopandas - keep original schema
    gdf = gpd.read_file(main_file)
    
    # Only add essential metadata columns
    gdf['dataset_name'] = dataset_names
    gdf['source_file'] = str(main_file.name)
    
    # Ensure valid CRS (use WGS84 if missing)
    if gdf.crs is None:
        gdf.set_crs("EPSG:4326", inplace=True)
    
    return gdf


@config.when(execution_mode="sequential")
@tag(stage="process", data_type="geodataframe")
def processed_geodataframe__sequential(
    dataset_download_path__sequential: str,  # Placeholder dependency
    dataset_names: List[str]
) -> gpd.GeoDataFrame:
    """Placeholder for sequential mode - actual processing happens in collected_arrow_tables__sequential."""
    # Return empty geodataframe as placeholder
    return gpd.GeoDataFrame()


@tag(stage="convert", data_type="arrow")
@cache(behavior="disable")  # Disable caching for GeoArrow tables due to extension type serialization issues
@config.when(execution_mode="parallel")
def geo_arrow_table__parallel(
    processed_geodataframe: gpd.GeoDataFrame,  # Individual GDF from parallel processing
    dataset_names: str  # Individual dataset name from parallel processing
) -> pa.Table:
    """Convert geodataframe to geo arrow table using proper GeoArrow format."""
    return _geoarrow_table(processed_geodataframe, dataset_names)


@config.when(execution_mode="sequential")
@tag(stage="convert", data_type="arrow")
@cache(behavior="disable")  # Disable caching for consistency with parallel version
def geo_arrow_table__sequential(
    processed_geodataframe: gpd.GeoDataFrame,  # Use base name for consistency
    dataset_names: List[str]
) -> pa.Table:
    """Placeholder for sequential mode - actual processing happens in collected_arrow_tables__sequential."""
    # Return empty arrow table as placeholder (could use helper for consistency)
    return pa.table({})


@tag(stage="collect", data_type="arrow_list", execution_mode="parallel")
# Note: Caching disabled due to GeoArrow extension type serialization issues
@cache(behavior="disable")
@config.when(execution_mode="parallel")
def collected_arrow_tables__parallel(geo_arrow_table: Collect[pa.Table]) -> List[pa.Table]:
    """Collect all arrow tables from parallel processing."""
    return list(geo_arrow_table)


@tag(stage="collect", data_type="arrow_list", execution_mode="sequential")
# Note: Caching disabled due to GeoArrow extension type serialization issues
@cache(behavior="disable")
@config.when(execution_mode="sequential")
def collected_arrow_tables__sequential(
    dataset_names: List[str],
    doi_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250
) -> List[pa.Table]:
    """Process all datasets sequentially and return list of arrow tables."""
    arrow_tables = []

    for dataset_name in dataset_names:
        print(f"Processing dataset: {dataset_name}")

        # Download dataset
        metadata = doi_metadata[dataset_name]
        download_dir = tempfile.mkdtemp(prefix=f"doi_{dataset_name}_")

        try:
            if metadata["repo"] == "github":
                # Handle GitHub URLs - distinguish between repos and raw files
                doi_url = metadata["doi"]

                if "raw.githubusercontent.com" in doi_url:
                    # Raw GitHub file - download directly
                    import urllib.request
                    from pathlib import Path

                    print(f"   📥 Downloading raw GitHub file: {doi_url}")
                    filename = Path(doi_url).name
                    if not filename.endswith(('.geojson', '.json', '.shp', '.gpkg', '.kml', '.gml')):
                        # If no extension, try to infer from URL or default to .geojson
                        filename = f"{dataset_name}.geojson"

                    file_path = Path(download_dir) / filename
                    urllib.request.urlretrieve(doi_url, file_path)
                    print(f"   ✅ Downloaded {filename} to {download_dir}")

                else:
                    # GitHub repository - clone it
                    import subprocess
                    repo_url = doi_url.replace("https://github.com/", "")
                    subprocess.run([
                        "git", "clone", "--depth", "1",
                        f"https://github.com/{repo_url}.git",
                        download_dir
                    ], check=True, capture_output=True)
                    print(f"   ✅ Cloned repository to {download_dir}")
            else:
                datahugger.get(
                    metadata["doi"],
                    output_folder=download_dir,
                    max_file_size=max_mb * 1024 * 1024
                )

            # Process geospatial data
            download_path = Path(download_dir)
            geo_extensions = {'.shp', '.geojson', '.gpkg', '.kml', '.gml', '.json'}
            geo_files = []

            for ext in geo_extensions:
                geo_files.extend(download_path.rglob(f"*{ext}"))

            if not geo_files:
                print(f"No geospatial files found for {dataset_name}")
                continue

            # Load the largest geospatial file
            main_file = max(geo_files, key=lambda f: f.stat().st_size)
            gdf = gpd.read_file(main_file)

            # Add metadata
            gdf['source_file'] = str(main_file.name)

            # Ensure valid CRS
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)

            # Convert to arrow table using our helper function
            table = _geoarrow_table(gdf, dataset_name)

            arrow_tables.append(table)

        except Exception as e:
            print(f"Error processing {dataset_name}: {e}")
            continue
        finally:
            shutil.rmtree(download_dir, ignore_errors=True)

    return arrow_tables

@tag(stage="storage", data_type="result", execution_mode="parallel")
@cache(behavior="default")  # Cache final results
@config.when(execution_mode="parallel")
def pipeline_result__parallel(
    collected_arrow_tables: List[pa.Table],  # Reference base name, not suffixed version
    database_path: str = "../db/eo_pv_data.duckdb",
    export_geoparquet: bool = True
) -> Dict[str, Any]:
    """Save arrow tables to DuckDB and optionally export to GeoParquet (parallel mode)."""
    return _storage_result(collected_arrow_tables, database_path, export_geoparquet)


@tag(stage="storage", data_type="result", execution_mode="sequential")
@cache(behavior="default")  # Cache final results
@config.when(execution_mode="sequential")
def pipeline_result__sequential(
    collected_arrow_tables: List[pa.Table],  # Reference base name, not suffixed version
    database_path: str = "../db/eo_pv_data.duckdb",
    export_geoparquet: bool = True
) -> Dict[str, Any]:
    """Save arrow tables to DuckDB and optionally export to GeoParquet (sequential mode)."""
    return _storage_result(collected_arrow_tables, database_path, export_geoparquet)