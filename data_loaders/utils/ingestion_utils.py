"""
Utility functions for data ingestion pipeline.

This module contains helper functions that support the main Hamilton DOI pipeline
but are not direct components of the DAG execution graph.
"""

import json
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional
from hamilton.function_modifiers import tag

import pandas as pd
import duckdb
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq


# Cache management constants
CACHE_FILE = Path("./cache/download_cache.json")


def load_download_cache() -> Dict[str, str]:
    """
    Load the download cache from JSON file.

    Returns:
        Dict mapping dataset names to their cached directory paths
    """
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
    """
    Save the download cache to JSON file.

    Args:
        cache: Dict mapping dataset names to their cached directory paths
    """
    try:
        # Ensure cache directory exists
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
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
    """
    Cache a temporary directory location for future use.

    Args:
        dataset_name: Name of the dataset
        temp_dir: Path to the temporary directory to cache
    """
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
            shutil.rmtree(cache_dir, ignore_errors=True)
            print(f"   🗑️  Cleaned up cache directory: {cache_dir}")

        del cache[dataset_name]
        save_download_cache(cache)
        print(f"   📝 Removed {dataset_name} from cache")


def clear_all_cache() -> None:
    """
    Clear all cached downloads and remove cache file.
    """
    if CACHE_FILE.exists():
        cache = load_download_cache()
        for dataset_name, temp_dir in cache.items():
            if Path(temp_dir).exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
                print(f"🗑️  Removed cached temp directory: {temp_dir}")
        CACHE_FILE.unlink()
        print(f"🗑️  Removed cache file: {CACHE_FILE}")
    print("✅ Cache cleared successfully")


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
    output_path = output_dir / "combined_doi_pv_features.parquet"
    
    # Import geoarrow for conversion
    try:
        import geoarrow.pyarrow as ga
        gdf = ga.to_geopandas(combined_datasets)
        gdf.to_parquet(output_path, compression="snappy", write_covering_bbox=True)
    except ImportError:
        # Fallback without geoarrow
        gdf = combined_datasets.to_pandas()
        gdf.to_parquet(output_path, compression="snappy")

    print(f"Exported {len(combined_datasets)} records to {output_path}")
    return str(output_path)
