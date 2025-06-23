"""
dbt Python Model Template: Hamilton Data Loader

This template demonstrates how to use Hamilton drivers within dbt Python models
for complex data loading operations that go beyond SQL capabilities.

Usage:
    1. Copy this template to your dbt models directory
    2. Rename to match your model (e.g., stg_pv_datasets.py)
    3. Customize the Hamilton modules and configuration
    4. Update the model function logic as needed

Template follows dbt Python model best practices:
- Proper function signature with dbt and session parameters
- Returns a pandas DataFrame for dbt materialization
- Includes error handling and logging
- Uses Hamilton for complex transformations
"""

import pandas as pd
from typing import Dict, Any

# Import Hamilton components
from hamilton import driver
from hamilton.base import DictResult

# Import your Hamilton modules (adjust paths as needed)
try:
    # Adjust import paths based on your project structure
    from data_loaders.hamilton_modules import (
        data_sources, transformations, validations
    )
except ImportError:
    # Fallback import for different project structures
    import sys
    from pathlib import Path
    
    # Add data_loaders to path
    data_loaders_path = Path(__file__).parent.parent / "data_loaders"
    sys.path.insert(0, str(data_loaders_path))
    
    from hamilton_modules import (
        data_sources, transformations, validations
    )


def model(dbt, session) -> pd.DataFrame:
    """
    dbt Python model that uses Hamilton for complex data loading.
    
    This model demonstrates the integration pattern where Hamilton handles
    complex data processing logic while dbt manages orchestration and materialization.
    
    Args:
        dbt: dbt context object providing access to refs, vars, etc.
        session: Database session object (DuckDB, Snowflake, etc.)
        
    Returns:
        pd.DataFrame: Processed data ready for dbt materialization
        
    Example dbt model configuration:
        {{ config(
            materialized='table',
            python_env='hamilton_env'
        ) }}
    """
    
    # Get dbt configuration and variables
    config = _get_hamilton_config(dbt)
    
    # Log execution start
    print(f"🚀 Starting Hamilton data loading with config: {config}")
    
    try:
        # Create Hamilton driver with modular components
        hamilton_driver = _create_hamilton_driver(config)
        
        # Define target variables to execute
        target_variables = [
            "processed_geodataframe",  # Main processed data
            "validation_summary",      # Quality validation results
            "data_lineage_info"       # Lineage tracking
        ]
        
        # Execute Hamilton pipeline
        print(f"   Executing Hamilton pipeline: {target_variables}")
        results = hamilton_driver.execute(target_variables)
        
        # Extract main result
        processed_data = results["processed_geodataframe"]
        validation_summary = results["validation_summary"]
        
        # Log validation results
        _log_validation_results(validation_summary)
        
        # Validate result before returning to dbt
        if processed_data is None or len(processed_data) == 0:
            raise ValueError("Hamilton pipeline returned empty dataset")
        
        print(f"✅ Hamilton processing complete: {len(processed_data)} rows, {len(processed_data.columns)} columns")
        
        # Return processed data for dbt materialization
        return processed_data
        
    except Exception as hamilton_error:
        print(f"❌ Hamilton pipeline failed: {hamilton_error}")
        print(f"   Error type: {type(hamilton_error).__name__}")
        
        # Return empty DataFrame with expected schema for graceful failure
        return _create_empty_fallback_dataframe()


def _get_hamilton_config(dbt) -> Dict[str, Any]:
    """
    Extract Hamilton configuration from dbt variables and context.
    
    Args:
        dbt: dbt context object
        
    Returns:
        Dict[str, Any]: Configuration for Hamilton driver
    """
    # Get dbt variables (defined in dbt_project.yml or --vars)
    hamilton_vars = dbt.var("hamilton_config", {})
    
    # Default configuration
    default_config = {
        "use_cache": True,
        "force_download": False,
        "max_mb": 250,
        "database_path": "../db/eo_pv_data.duckdb"
    }
    
    # Merge with dbt variables
    config = {**default_config, **hamilton_vars}
    
    # Override with target-specific settings if available
    if hasattr(dbt, 'target') and dbt.target:
        if hasattr(dbt.target, 'path'):
            config["database_path"] = f"{dbt.target.path}/eo_pv_data.duckdb"
    
    return config


def _create_hamilton_driver(config: Dict[str, Any]) -> driver.Driver:
    """
    Create Hamilton driver with proper configuration and modules.
    
    Args:
        config: Configuration dictionary for Hamilton
        
    Returns:
        driver.Driver: Configured Hamilton driver
    """
    try:
        # Create driver with modular components
        hamilton_driver = (
            driver.Builder()
            .with_modules(data_sources, transformations, validations)
            .with_config(config)
            .with_adapter(DictResult())
            .build()
        )
        
        print(f"   ✅ Created Hamilton driver with {len(config)} config parameters")
        return hamilton_driver
        
    except Exception as driver_error:
        print(f"   ❌ Failed to create Hamilton driver: {driver_error}")
        raise


def _log_validation_results(validation_summary: Dict[str, Any]) -> None:
    """
    Log validation results from Hamilton pipeline.
    
    Args:
        validation_summary: Validation results from Hamilton
    """
    if not validation_summary:
        print("   ⚠️  No validation summary available")
        return
    
    status = validation_summary.get("validation_status", "unknown")
    quality_score = validation_summary.get("quality_score", 0.0)
    
    print(f"   📊 Validation Status: {status}")
    print(f"   📊 Quality Score: {quality_score:.2f}")
    
    issues = validation_summary.get("issues_found", [])
    if issues:
        print(f"   ⚠️  Issues found: {len(issues)}")
        for issue in issues[:3]:  # Show first 3 issues
            print(f"      - {issue}")
        if len(issues) > 3:
            print(f"      ... and {len(issues) - 3} more issues")


def _create_empty_fallback_dataframe() -> pd.DataFrame:
    """
    Create empty DataFrame with expected schema for graceful failure handling.
    
    Returns:
        pd.DataFrame: Empty DataFrame with expected columns
    """
    # Define expected schema based on your data model
    expected_columns = [
        "dataset_name",
        "doi", 
        "geometry_wkt",
        "centroid_lat",
        "centroid_lon",
        "area_m2",
        "processed_at"
    ]
    
    # Create empty DataFrame with proper dtypes
    empty_df = pd.DataFrame(columns=expected_columns)
    
    # Set appropriate dtypes
    empty_df = empty_df.astype({
        "dataset_name": "string",
        "doi": "string", 
        "geometry_wkt": "string",
        "centroid_lat": "float64",
        "centroid_lon": "float64",
        "area_m2": "float64",
        "processed_at": "datetime64[ns]"
    })
    
    print(f"   🔄 Created empty fallback DataFrame with {len(expected_columns)} columns")
    return empty_df


# Optional: Add model-specific configuration
"""
Example dbt model configuration to add at the top of your model file:

{{ config(
    materialized='table',
    python_env='hamilton_env',
    pre_hook="CREATE SCHEMA IF NOT EXISTS staging",
    post_hook="ANALYZE {{ this }}",
    tags=['hamilton', 'geospatial', 'staging']
) }}

SELECT * FROM {{ ref('your_model_name') }}
"""
