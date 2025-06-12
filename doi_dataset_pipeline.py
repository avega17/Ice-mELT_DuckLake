"""DOI Dataset Pipeline for EO PV Data Ingestion using dlt"""

# mypy: disable-error-code="no-untyped-def,arg-type"

from typing import Optional, Dict, Any, Iterator, List
import os
import json
import tempfile
import shutil
from pathlib import Path

import dlt
from dlt.common.typing import TDataItem
from dlt.sources.helpers import requests

# Import our existing utility functions
from utils.fetch_and_preprocess import fetch_dataset_files


# Dataset metadata - loaded from dataset_manifest.json structure
DATASET_METADATA = {
    # Original datasets
    "bradbury_2016_california": {
        "doi": "https://doi.org/10.6084/m9.figshare.3385780.v4",
        "repo": "figshare",
        "compression": "zip",
        "label_fmt": "geojson",
        "description": "USA USGS aerial imagery | 2016 | ~19K labels - Distributed solar photovoltaic array location and extent dataset for remote sensing object identification",
        "geom_type": "Polygon",
        "crs": "NAD83",
        "label_count": 19433
    },
    "stowell_2020_uk": {
        "doi": "https://zenodo.org/records/4059881",
        "repo": "zenodo",
        "compression": "zip",
        "label_fmt": "geojson",
        "description": "UK Crowdsourced PV | 2020 | ~265K labels - A harmonised, high-coverage, open dataset of solar photovoltaic installations in the UK",
        "geom_type": ["Point", "Polygon", "MultiPolygon"],
        "crs": "WGS84",
        "label_count": 265418
    },
    "kruitwagen_2021_global": {
        "doi": "https://doi.org/10.5281/zenodo.5005867",
        "repo": "zenodo",
        "compression": "zip",
        "label_fmt": "geojson",
        "description": "Global PV inventory | 2021 | ~68K labels - A global inventory of photovoltaic solar energy generating units",
        "geom_type": "Polygon",
        "crs": "WGS84",
        "label_count": [50426, 68661]
    },
    # New vector datasets
    "global_harmonized_large_solar_farms_2020": {
        "doi": "https://doi.org/10.6084/m9.figshare.11310269.v6",
        "repo": "figshare",
        "compression": "zip",
        "label_fmt": "gpkg",
        "description": "Harmonised global datasets of wind and solar farm locations and power | 2020 | ~35K labels",
        "geom_type": "Polygon",
        "crs": "WGS84",
        "label_count": 35272
    },
    "chn_med_res_pv_2024": {
        "doi": "https://github.com/qingfengxitu/ChinaPV/tree/main",
        "repo": "github",
        "compression": None,
        "label_fmt": "shp",
        "description": "China PV | 2015;2020 | ~3.4K labels - Vectorized solar photovoltaic installation dataset across China",
        "geom_type": "Polygon",
        "crs": "WGS84",
        "label_count": 3356
    },
    "ind_pv_solar_farms_2022": {
        "doi": "https://raw.githubusercontent.com/microsoft/solar-farms-mapping/refs/heads/main/data/solar_farms_india_2021_merged_simplified.geojson",
        "repo": "github",
        "compression": None,
        "label_fmt": "geojson",
        "description": "India PV solar farms | 2022 | ~117 labels - An Artificial Intelligence Dataset for Solar Energy Locations in India",
        "geom_type": "MultiPolygon",
        "crs": "WGS84",
        "label_count": 117
    },
    "global_pv_inventory_sent2_2024": {
        "doi": "https://github.com/yzyly1992/GloSoFarID/tree/main/data_coordinates",
        "repo": "github",
        "compression": None,
        "label_fmt": "json",
        "description": "GloSoFarID: Global multispectral dataset for Solar Farm IDentification | 2024 | ~6.8K labels",
        "geom_type": "Point",
        "crs": "WGS84",
        "label_count": 6793
    }
}


@dlt.resource(write_disposition="replace")
def doi_dataset_resource(
    dataset_name: str,
    metadata: Dict[str, Any],
    max_mb: int = 100,
    force: bool = False
) -> Iterator[TDataItem]:
    """
    DLT resource to fetch and yield DOI dataset files.

    Args:
        dataset_name: Name of the dataset
        metadata: Dataset metadata including DOI, repo type, etc.
        max_mb: Maximum file size in MB
        force: Force download even if files exist

    Yields:
        Dataset information and file metadata
    """
    print(f"Fetching dataset: {dataset_name}")

    # Use our existing fetch function
    dataset_tree = fetch_dataset_files(dataset_name, metadata, max_mb, force)

    if dataset_tree:
        # Yield the main dataset information
        yield {
            "dataset_name": dataset_name,
            "doi": metadata["doi"],
            "repo": metadata["repo"],
            "label_format": metadata["label_fmt"],
            "description": metadata["description"],
            "geometry_type": metadata.get("geom_type", "Unknown"),
            "crs": metadata.get("crs", "Unknown"),
            "label_count": metadata.get("label_count", 0),
            "compression": metadata.get("compression", None),
            "output_folder": dataset_tree["output_folder"],
            "file_count": len(dataset_tree["files"]),
            "files": dataset_tree["files"],
            "fs_tree": dataset_tree["fs_tree"]
        }

        # Yield individual file information for detailed tracking
        for file_path in dataset_tree["files"]:
            file_info = {
                "dataset_name": dataset_name,
                "file_path": file_path,
                "file_name": os.path.basename(file_path),
                "file_size": os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                "file_extension": os.path.splitext(file_path)[1]
            }
            yield file_info


def fetch_dataset_files_to_dir(dataset_name, metadata, dst_dir, max_mb=100, force=False):
    """
    Fetch dataset files to a specific directory (modified version of fetch_dataset_files).

    Args:
        dataset_name (str): Name of the dataset
        metadata (dict): Metadata dictionary for the dataset
        dst_dir (str): Destination directory
        max_mb (int): Maximum file size in MB
        force (bool): Force download even if files exist

    Returns:
        dict: A dictionary with dataset information
    """
    from utils.fetch_and_preprocess import fetch_from_datahugger, fetch_from_github, fetch_from_sciencebase

    doi = metadata['doi']
    repo = metadata['repo']

    # Create destination path
    dst = os.path.join(dst_dir, dataset_name)

    # Use appropriate fetch function based on repository
    if repo in ['figshare', 'zenodo']:
        dataset_tree = fetch_from_datahugger(doi, dst, max_mb, force)
    elif repo == 'github':
        dataset_tree = fetch_from_github(doi, dst, max_mb, force)
    elif repo == 'sciencebase':
        dataset_tree = fetch_from_sciencebase(doi, dst, max_mb, force)
    else:
        print(f"Unsupported repository type: {repo}")
        return None

    return dataset_tree


def process_glosofarid_json(file_path: str):
    """
    Process GloSoFarID JSON files which contain coordinate data in a special format.

    Args:
        file_path: Path to the JSON file

    Returns:
        GeoDataFrame with Point geometries
    """
    try:
        import json
        import geopandas as gpd
        from shapely.geometry import Point

        with open(file_path, 'r') as f:
            data = json.load(f)

        # The JSON structure contains coordinate arrays
        # Convert to Point geometries
        geometries = []
        for item in data:
            if isinstance(item, list) and len(item) >= 2:
                # Assuming [longitude, latitude] format
                lon, lat = item[0], item[1]
                geometries.append(Point(lon, lat))
            elif isinstance(item, dict):
                # Handle different possible structures
                if 'coordinates' in item:
                    coords = item['coordinates']
                    if isinstance(coords, list) and len(coords) >= 2:
                        lon, lat = coords[0], coords[1]
                        geometries.append(Point(lon, lat))
                elif 'lon' in item and 'lat' in item:
                    geometries.append(Point(item['lon'], item['lat']))
                elif 'longitude' in item and 'latitude' in item:
                    geometries.append(Point(item['longitude'], item['latitude']))

        if geometries:
            gdf = gpd.GeoDataFrame(geometry=geometries, crs='EPSG:4326')
            return gdf
        else:
            print(f"      No valid coordinates found in {file_path}")
            return None

    except Exception as e:
        print(f"      Error processing GloSoFarID JSON {file_path}: {e}")
        return None


def process_datasets_to_geoparquet(
    datasets: Optional[List[str]] = None,
    max_mb: int = 250,  # Increased to 250MB for large files like stowell_2020_uk
    force: bool = False
) -> None:
    """
    Download datasets to temp directory and process to GeoParquet format.

    Args:
        datasets: List of dataset names to fetch. If None, fetches all available datasets.
        max_mb: Maximum file size in MB
        force: Force download even if files exist
    """
    if datasets is None:
        datasets = list(DATASET_METADATA.keys())

    # Import required libraries
    try:
        import geopandas as gpd
    except ImportError as e:
        print(f"Error: GeoPandas not available: {e}")
        return

    # Create geoparquet output directory
    geoparquet_dir = Path("datasets/raw/geoparquet")
    geoparquet_dir.mkdir(parents=True, exist_ok=True)

    for dataset_name in datasets:
        if dataset_name not in DATASET_METADATA:
            print(f"Warning: Dataset '{dataset_name}' not found in metadata")
            continue

        print(f"Processing dataset: {dataset_name}")
        metadata = DATASET_METADATA[dataset_name]

        # Check if geoparquet already exists and skip if not forcing
        output_file = geoparquet_dir / f"{dataset_name}.parquet"
        if output_file.exists() and not force:
            print(f"  GeoParquet already exists: {output_file}")
            continue

        # Use temp directory for downloads
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"  Downloading to temp directory: {temp_dir}")

            # Download to temp directory using custom destination
            dataset_tree = fetch_dataset_files_to_dir(dataset_name, metadata, temp_dir, max_mb, force=True)

            if not dataset_tree or not dataset_tree["files"]:
                print(f"  No files downloaded for dataset: {dataset_name}")
                continue

            print(f"  Processing {len(dataset_tree['files'])} files...")

            # Collect all geodataframes
            gdfs = []

            for file_path in dataset_tree["files"]:
                file_ext = os.path.splitext(file_path)[1].lower()

                # Only process geospatial files
                if file_ext in ['.geojson', '.json', '.shp', '.gpkg']:
                    try:
                        print(f"    Reading {os.path.basename(file_path)}...")

                        # Special handling for global_pv_inventory_sent2_2024 JSON files
                        if dataset_name == "global_pv_inventory_sent2_2024" and file_ext == '.json':
                            gdf = process_glosofarid_json(file_path)
                        else:
                            # Standard geospatial file reading
                            gdf = gpd.read_file(file_path)

                        if gdf is not None and len(gdf) > 0:
                            # Convert to WGS84 if needed (minimal processing)
                            if gdf.crs and gdf.crs.to_string() != 'EPSG:4326':
                                print(f"      Converting from {gdf.crs} to EPSG:4326...")
                                gdf = gdf.to_crs('EPSG:4326')

                            # Add minimal dataset metadata (leave transformations for dbt)
                            gdf['dataset_name'] = dataset_name
                            gdf['source_file'] = os.path.basename(file_path)

                            gdfs.append(gdf)
                            print(f"      Loaded {len(gdf)} features")
                        else:
                            print(f"      Warning: No features found in {file_path}")

                    except Exception as e:
                        print(f"      Error reading {file_path}: {e}")
                        continue
                else:
                    print(f"    Skipping non-geospatial file: {os.path.basename(file_path)}")

            # Combine all geodataframes for this dataset
            if gdfs:
                print(f"  Combining {len(gdfs)} files into single GeoDataFrame...")
                combined_gdf = gpd.pd.concat(gdfs, ignore_index=True)

                # Save as GeoParquet v1.1 with covering bbox
                print(f"  Saving {len(combined_gdf)} features to {output_file}")
                combined_gdf.to_parquet(
                    output_file,
                    compression='snappy',
                    write_covering_bbox=True  # GeoParquet v1.1 feature
                )
                print(f"  ✓ Saved GeoParquet: {output_file}")
            else:
                print(f"  ✗ No geospatial data found for dataset: {dataset_name}")


@dlt.resource
def parquet_files_resource():
    """
    DLT resource to load GeoParquet files content directly.
    """
    import pandas as pd
    from pathlib import Path

    # Load all parquet files from the geoparquet directory
    geoparquet_path = Path("datasets/raw/geoparquet")
    print(f"Loading GeoParquet files from: {geoparquet_path}")

    if not geoparquet_path.exists():
        print(f"Warning: GeoParquet directory does not exist: {geoparquet_path}")
        return

    parquet_files = list(geoparquet_path.glob("*.parquet"))
    print(f"Found {len(parquet_files)} parquet files: {[f.name for f in parquet_files]}")

    # Read each parquet file and yield its contents
    for parquet_file in parquet_files:
        print(f"Reading parquet file: {parquet_file.name}")
        try:
            # Read parquet file using pandas
            df = pd.read_parquet(parquet_file)
            print(f"  Loaded {len(df)} rows from {parquet_file.name}")

            # Yield each row as a dictionary
            for _, row in df.iterrows():
                yield row.to_dict()

        except Exception as e:
            print(f"  Error reading {parquet_file}: {e}")
            continue


@dlt.source
def geoparquet_filesystem_source():
    """
    DLT source to load GeoParquet files from local filesystem.
    """
    return parquet_files_resource()


def clear_pipeline_state(pipeline_name: str = "doi_dataset_pipeline"):
    """Clear dlt pipeline state to avoid schema conflicts."""
    import shutil

    pipeline_dir = Path(f".dlt/pipelines/{pipeline_name}")
    if pipeline_dir.exists():
        print(f"Clearing pipeline state: {pipeline_dir}")
        shutil.rmtree(pipeline_dir)
    else:
        print("No existing pipeline state found")


def load_doi_datasets(
    datasets: Optional[List[str]] = None,
    max_mb: int = 250,  # Increased to 250MB for large files
    force: bool = False,
    dataset_name: str = "eo_pv_datasets",
    clear_state: bool = True
) -> None:
    """
    Load DOI datasets into DuckDB using dlt pipeline.

    This function:
    1. Downloads datasets to temp directories
    2. Processes them to GeoParquet format
    3. Loads the GeoParquet files into DuckDB using dlt filesystem source

    Args:
        datasets: List of dataset names to fetch
        max_mb: Maximum file size in MB
        force: Force download even if files exist
        dataset_name: Name for the DuckDB dataset
        clear_state: Clear pipeline state to avoid schema conflicts
    """
    print("=== DOI Dataset Pipeline ===")

    # Step 0: Clear pipeline state if requested
    if clear_state:
        print("Step 0: Clearing pipeline state...")
        clear_pipeline_state()

    # Step 1: Process datasets to GeoParquet
    print("Step 1: Processing datasets to GeoParquet...")
    process_datasets_to_geoparquet(datasets=datasets, max_mb=max_mb, force=force)

    # Step 2: Load GeoParquet files into DuckDB using dlt
    print("\nStep 2: Loading GeoParquet files into DuckDB...")

    # Optionally drop existing table to avoid schema conflicts
    if clear_state:
        try:
            import duckdb
            db_path = os.path.abspath('eo_pv_data.duckdb')
            conn = duckdb.connect(db_path)
            conn.execute(f"DROP TABLE IF EXISTS {dataset_name}.pv_features")
            conn.close()
            print(f"Dropped existing table {dataset_name}.pv_features")
        except Exception as e:
            print(f"Note: Could not drop existing table (this is OK): {e}")

    # Create dlt pipeline with DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="doi_dataset_pipeline",
        destination='duckdb',
        dataset_name=dataset_name,
    )

    # Load from filesystem source
    load_info = pipeline.run(
        geoparquet_filesystem_source(),
        table_name="pv_features"
    )

    print(f"\n✓ Pipeline completed successfully!")
    print(f"Dataset location: {pipeline.dataset_name}")

    # Print load information safely
    if hasattr(load_info, 'loads'):
        print(f"Loaded {len(load_info.loads)} loads")
    elif hasattr(load_info, 'load_packages'):
        print(f"Loaded {len(load_info.load_packages)} load packages")

    print("Load info:")
    print(load_info)

    # Explicit cleanup to prevent hanging
    try:
        # Close any open connections
        if hasattr(pipeline, '_sql_job_client') and pipeline._sql_job_client:
            pipeline._sql_job_client.close()

        # Force garbage collection
        import gc
        gc.collect()

        print("✓ Pipeline cleanup completed")
    except Exception as cleanup_error:
        print(f"Note: Cleanup warning (non-critical): {cleanup_error}")


def load_sample_datasets() -> None:
    """Load a sample of DOI datasets for testing."""
    try:
        # Load a mix of small and medium datasets for testing
        sample_datasets = [
            "ind_pv_solar_farms_2022",  # Small dataset (117 labels)
            "chn_med_res_pv_2024",      # Medium dataset (3.4K labels)
            "bradbury_2016_california"  # Larger dataset (19K labels)
        ]

        load_doi_datasets(
            datasets=sample_datasets,
            max_mb=50,  # Smaller limit for testing
            force=False,
            dataset_name="eo_pv_sample"
        )
        print("✓ Sample datasets loaded successfully!")

    except Exception as e:
        print(f"✗ Error loading sample datasets: {e}")
        import traceback
        traceback.print_exc()


def load_all_datasets() -> None:
    """Load all available DOI datasets."""
    load_doi_datasets(
        datasets=None,  # Load all datasets
        max_mb=250,  # Increased to 250MB for large files
        force=False,
        dataset_name="eo_pv_complete"
    )


if __name__ == "__main__":
    import sys

    try:
        # Load all available datasets
        print("Loading all DOI datasets...")
        load_all_datasets()

        # For testing only a few datasets, use this instead:
        # print("Loading sample DOI datasets...")
        # load_sample_datasets()

        print("\n🎉 All operations completed successfully!")

    except Exception as e:
        print(f"✗ Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # Force cleanup and exit
        print("Performing final cleanup...")
        import gc
        gc.collect()

        # Give a moment for any background threads to finish
        import time
        time.sleep(1)

        print("Exiting...")
        sys.exit(0)
