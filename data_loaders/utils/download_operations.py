"""
Download operations utility module.

This module contains utility functions for downloading DOI datasets
from various repositories (Zenodo, GitHub, USGS) with intelligent caching.

These are utility functions (not Hamilton nodes) that implement the actual
download logic called by Hamilton nodes in the data_sources module.
"""

import os
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any

# Import existing cache utilities
try:
    from .ingestion_utils import (
        get_cached_tempdir, cache_tempdir, load_download_cache
    )
except ImportError:
    # Fallback import path
    from ingestion_utils import (
        get_cached_tempdir, cache_tempdir, load_download_cache
    )


def download_doi_dataset_files_hamilton(
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250,
    force_download: bool = False
) -> str:
    """
    Download DOI dataset files for Hamilton caching (no custom cache logic).

    This function is designed to work with Hamilton's built-in caching system.
    It downloads files to a temporary directory and returns the path.
    Hamilton handles all caching logic automatically.

    Args:
        dataset_name: Name of the dataset to download
        dataset_metadata: Complete metadata dictionary from doi_manifest.json
        max_mb: Maximum file size in MB (default: 250MB for large datasets)
        force_download: Force re-download (Hamilton handles this via cache behavior)

    Returns:
        str: Path to downloaded/extracted files directory

    Raises:
        ValueError: If dataset_name not found in metadata
        Exception: If download fails after retries
    """
    if dataset_name not in dataset_metadata:
        raise ValueError(f"Unknown dataset: {dataset_name}")

    metadata = dataset_metadata[dataset_name]

    # Always create a new temporary directory for downloads
    # Hamilton's caching will handle reuse of results
    download_dir = tempfile.mkdtemp(prefix=f"hamilton_{dataset_name}_")
    print(f"📥 Hamilton downloading {dataset_name} to: {download_dir}")

    try:
        if metadata["repo"] == "github":
            return _download_from_github_hamilton(
                dataset_name=dataset_name,
                metadata=metadata,
                download_dir=download_dir,
                max_mb=max_mb,
                force_download=force_download
            )
        else:
            return _download_from_doi_hamilton(
                dataset_name=dataset_name,
                metadata=metadata,
                download_dir=download_dir,
                max_mb=max_mb
            )

    except Exception as e:
        print(f"💥 Error downloading {dataset_name}: {e}")
        print(f"   Cleaning up download directory: {download_dir}")
        # Clean up download directory on failure
        shutil.rmtree(download_dir, ignore_errors=True)
        raise


def download_doi_dataset_files(
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250,
    use_cache: bool = True,
    force_download: bool = False
) -> str:
    """
    Download DOI dataset files with intelligent caching support.

    This utility function handles downloading datasets from various DOI repositories
    with smart caching to avoid redundant downloads during development and testing.

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

    # Always create a new temporary directory for downloads
    download_dir = tempfile.mkdtemp(prefix=f"{dataset_name}_")
    print(f"📥 Downloading {dataset_name} to temporary directory: {download_dir}")

    try:
        if metadata["repo"] == "github":
            return _download_from_github(
                dataset_name=dataset_name,
                metadata=metadata,
                download_dir=download_dir,
                max_mb=max_mb,
                use_cache=use_cache,
                force_download=force_download
            )
        else:
            return _download_from_doi(
                dataset_name=dataset_name,
                metadata=metadata,
                download_dir=download_dir,
                max_mb=max_mb,
                use_cache=use_cache
            )

    except Exception as e:
        print(f"💥 Error downloading {dataset_name}: {e}")
        print(f"   Cleaning up download directory: {download_dir}")
        # Clean up download directory on failure (only if not using cache)
        if not use_cache:
            shutil.rmtree(download_dir, ignore_errors=True)
        raise


def _download_from_github_hamilton(
    dataset_name: str,
    metadata: Dict[str, Any],
    download_dir: str,
    max_mb: int,
    force_download: bool
) -> str:
    """Download dataset from GitHub repository for Hamilton caching."""
    print(f"📥 Hamilton downloading GitHub dataset {dataset_name}...")
    print(f"   Repository URL: {metadata['doi']}")
    print(f"   Target directory: {download_dir}")

    # Import the existing GitHub fetch function with robust import handling
    try:
        from .fetch_and_preprocess import fetch_from_github
    except ImportError:
        try:
            from fetch_and_preprocess import fetch_from_github
        except ImportError:
            # Try to find fetch_and_preprocess in the data_loaders directory
            import sys
            current_file_dir = Path(__file__).parent  # utils directory
            data_loaders_dir = current_file_dir.parent  # data_loaders directory

            if str(data_loaders_dir) not in sys.path:
                sys.path.insert(0, str(data_loaders_dir))

            try:
                from fetch_and_preprocess import fetch_from_github
            except ImportError:
                # Final fallback - look in project root
                project_root = data_loaders_dir.parent
                if str(project_root) not in sys.path:
                    sys.path.insert(0, str(project_root))
                from utils.fetch_and_preprocess import fetch_from_github

    try:
        result = fetch_from_github(
            doi=metadata["doi"],
            dst=download_dir,
            max_mb=max_mb,
            force=force_download
        )

        if result and result.get('files'):
            downloaded_files = result['files']
            print(f"✅ Hamilton downloaded {len(downloaded_files)} files from GitHub")
            for f in downloaded_files[:5]:  # Show first 5 files
                file_path = Path(f)
                print(f"   📄 {file_path.name} ({file_path.stat().st_size / 1024:.1f} KB)")
            if len(downloaded_files) > 5:
                print(f"   ... and {len(downloaded_files) - 5} more files")
        else:
            print(f"⚠️  No files downloaded from GitHub for {dataset_name}")
            print(f"   Result: {result}")

        return download_dir

    except Exception as github_error:
        print(f"❌ GitHub download failed for {dataset_name}: {github_error}")
        print(f"   URL: {metadata['doi']}")
        raise


def _download_from_doi_hamilton(
    dataset_name: str,
    metadata: Dict[str, Any],
    download_dir: str,
    max_mb: int
) -> str:
    """Download dataset from DOI repository using datahugger for Hamilton caching."""
    import datahugger
    import time
    import pandas as pd

    print(f"📥 Hamilton downloading {dataset_name} from {metadata['repo']} ({metadata['doi']})...")
    print(f"   Target directory: {download_dir}")
    print(f"   Max file size: {max_mb} MB")
    print(f"   Starting download at {pd.Timestamp.now()}")

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
            print(f"✅ Hamilton downloaded {len(downloaded_files)} files for {dataset_name} ({total_size / 1024 / 1024:.1f} MB)")
            for f in downloaded_files[:5]:  # Show first 5 files
                print(f"   📄 {f.name} ({f.stat().st_size / 1024:.1f} KB)")
            if len(downloaded_files) > 5:
                print(f"   ... and {len(downloaded_files) - 5} more files")
        else:
            print(f"⚠️  No files downloaded for {dataset_name}")
            print(f"   Directory contents: {list(download_path.iterdir())}")

        return download_dir

    except Exception as download_error:
        print(f"❌ Datahugger download failed for {dataset_name}: {download_error}")
        print(f"   DOI: {metadata['doi']}")
        print(f"   Repo: {metadata['repo']}")
        raise


def _download_from_github(
    dataset_name: str,
    metadata: Dict[str, Any],
    download_dir: str,
    max_mb: int,
    use_cache: bool,
    force_download: bool
) -> str:
    """Download dataset from GitHub repository."""
    print(f"📥 Downloading GitHub dataset {dataset_name}...")
    print(f"   Repository URL: {metadata['doi']}")
    print(f"   Target directory: {download_dir}")

    # Import the existing GitHub fetch function with robust import handling
    try:
        from .fetch_and_preprocess import fetch_from_github
    except ImportError:
        try:
            from fetch_and_preprocess import fetch_from_github
        except ImportError:
            # Try to find fetch_and_preprocess in the data_loaders directory
            import sys
            current_file_dir = Path(__file__).parent  # utils directory
            data_loaders_dir = current_file_dir.parent  # data_loaders directory

            if str(data_loaders_dir) not in sys.path:
                sys.path.insert(0, str(data_loaders_dir))

            try:
                from fetch_and_preprocess import fetch_from_github
            except ImportError:
                # Final fallback - look in project root
                project_root = data_loaders_dir.parent
                if str(project_root) not in sys.path:
                    sys.path.insert(0, str(project_root))
                from utils.fetch_and_preprocess import fetch_from_github

    try:
        result = fetch_from_github(
            doi=metadata["doi"],
            dst=download_dir,
            max_mb=max_mb,
            force=force_download
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


def _download_from_doi(
    dataset_name: str,
    metadata: Dict[str, Any],
    download_dir: str,
    max_mb: int,
    use_cache: bool
) -> str:
    """Download dataset from DOI repository using datahugger."""
    import datahugger
    import time
    import pandas as pd

    print(f"📥 Downloading {dataset_name} from {metadata['repo']} ({metadata['doi']})...")
    print(f"   Target directory: {download_dir}")
    print(f"   Max file size: {max_mb} MB")
    print(f"   Starting download at {pd.Timestamp.now()}")

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
