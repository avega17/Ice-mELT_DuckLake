"""
File operations utility module.

This module contains utility functions for extracting and filtering
geospatial files from downloaded datasets with intelligent filtering.

These are utility functions (not Hamilton nodes) that implement the actual
file discovery and filtering logic called by Hamilton nodes.
"""

import re
from pathlib import Path
from typing import List, Dict, Any


def extract_geospatial_file_paths(
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
        geospatial_files = _apply_file_filters(
            geospatial_files=geospatial_files,
            file_filters=file_filters,
            dataset_name=dataset_name
        )

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


def _apply_file_filters(
    geospatial_files: List[Path],
    file_filters: Dict[str, Any],
    dataset_name: str
) -> List[Path]:
    """Apply file filtering rules to geospatial files."""
    print(f"   📋 Applying file filters: {file_filters.get('description', 'Custom filters')}")

    include_patterns = file_filters.get("include_patterns", [])
    exclude_patterns = file_filters.get("exclude_patterns", [])
    use_regex = file_filters.get("use_regex", False)

    if use_regex:
        print(f"   🔍 Using regex pattern matching")

    filtered_files = []
    for file_path in geospatial_files:
        file_path_str = str(file_path)
        file_name = file_path.name

        # Check include patterns (must match at least one)
        if include_patterns:
            include_match = _check_patterns(
                file_path_str=file_path_str,
                patterns=include_patterns,
                use_regex=use_regex
            )
            if not include_match:
                print(f"     ❌ Excluded (missing include pattern): {file_name}")
                continue

        # Check exclude patterns (none should match)
        if exclude_patterns:
            exclude_match = _check_patterns(
                file_path_str=file_path_str,
                patterns=exclude_patterns,
                use_regex=use_regex
            )
            if exclude_match:
                print(f"     ❌ Excluded (matches exclude pattern): {file_name}")
                continue

        filtered_files.append(file_path)
        print(f"     ✅ Included: {file_name}")

    print(f"   🎯 After filtering: {len(filtered_files)} files selected")
    return filtered_files


def _check_patterns(
    file_path_str: str,
    patterns: List[str],
    use_regex: bool
) -> bool:
    """Check if file path matches any of the given patterns."""
    if use_regex:
        return any(
            re.search(pattern, file_path_str, re.IGNORECASE)
            for pattern in patterns
        )
    else:
        # Use simple substring matching (case-insensitive)
        return any(
            pattern.lower() in file_path_str.lower()
            for pattern in patterns
        )
