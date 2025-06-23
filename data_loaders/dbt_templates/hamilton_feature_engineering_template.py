"""
dbt Python Model Template: Hamilton Feature Engineering

This template demonstrates how to use Hamilton drivers within dbt Python models
for complex feature engineering operations, particularly spatial and ML features.

Usage:
    1. Copy this template to your dbt models directory
    2. Rename to match your model (e.g., prep_spatial_features.py)
    3. Customize the Hamilton modules and feature engineering logic
    4. Update input references and output schema as needed

Template focuses on:
- Spatial feature engineering (H3 indexing, clustering, etc.)
- ML feature preparation
- Complex transformations beyond SQL capabilities
- Integration with upstream dbt models
"""

import pandas as pd
import geopandas as gpd
from typing import Dict, Any, Optional

# Import Hamilton components
from hamilton import driver
from hamilton.base import DictResult

# Import Hamilton modules for feature engineering
try:
    from data_loaders.hamilton_modules import (
        transformations, validations
    )
    # Import specialized feature engineering modules
    from data_loaders.hamilton_modules.dbt_adapters import (
        spatial_features, ml_features
    )
except ImportError:
    # Fallback import handling
    import sys
    from pathlib import Path
    
    data_loaders_path = Path(__file__).parent.parent / "data_loaders"
    sys.path.insert(0, str(data_loaders_path))
    
    from hamilton_modules import transformations, validations
    # Note: You'll need to create these specialized modules
    print("⚠️  Specialized feature engineering modules not found - using basic transformations")


def model(dbt, session) -> pd.DataFrame:
    """
    dbt Python model that uses Hamilton for complex feature engineering.
    
    This model takes processed geospatial data from upstream dbt models and
    applies sophisticated feature engineering using Hamilton's modular approach.
    
    Args:
        dbt: dbt context object providing access to refs, vars, etc.
        session: Database session object
        
    Returns:
        pd.DataFrame: Feature-engineered data ready for ML or analysis
        
    Example upstream dependencies:
        - {{ ref('stg_pv_datasets') }} - Raw processed PV data
        - {{ ref('stg_admin_boundaries') }} - Administrative boundaries
        - {{ ref('stg_elevation_data') }} - Elevation/terrain data
    """
    
    # Get input data from upstream dbt models
    input_data = _get_input_data(dbt)
    
    # Get Hamilton configuration
    config = _get_feature_engineering_config(dbt)
    
    print(f"🔧 Starting Hamilton feature engineering with {len(input_data)} input records")
    
    try:
        # Create Hamilton driver for feature engineering
        hamilton_driver = _create_feature_engineering_driver(config)
        
        # Define target features to generate
        target_features = [
            "spatial_features",      # H3 indices, spatial clusters, etc.
            "contextual_features",   # Admin boundaries, elevation, etc.
            "derived_features",      # Calculated metrics, ratios, etc.
            "ml_ready_features"      # Scaled, encoded features for ML
        ]
        
        # Execute Hamilton feature engineering pipeline
        print(f"   Executing feature engineering: {target_features}")
        results = hamilton_driver.execute(
            target_features,
            inputs={"input_geodataframe": input_data}
        )
        
        # Combine all features into final dataset
        feature_engineered_data = _combine_features(results, input_data)
        
        # Validate feature engineering results
        _validate_features(feature_engineered_data, config)
        
        print(f"✅ Feature engineering complete: {len(feature_engineered_data)} rows, {len(feature_engineered_data.columns)} columns")
        
        return feature_engineered_data
        
    except Exception as feature_error:
        print(f"❌ Hamilton feature engineering failed: {feature_error}")
        
        # Return input data with minimal features as fallback
        return _create_minimal_features_fallback(input_data)


def _get_input_data(dbt) -> pd.DataFrame:
    """
    Get input data from upstream dbt models.
    
    Args:
        dbt: dbt context object
        
    Returns:
        pd.DataFrame: Combined input data for feature engineering
    """
    # Primary input: processed PV datasets
    pv_data = dbt.ref("stg_pv_datasets")
    
    # Optional: Additional context data
    try:
        # Admin boundaries for spatial context
        admin_boundaries = dbt.ref("stg_admin_boundaries")
        print(f"   📍 Loaded admin boundaries: {len(admin_boundaries)} records")
        
        # Elevation data for terrain features
        elevation_data = dbt.ref("stg_elevation_data")
        print(f"   🏔️  Loaded elevation data: {len(elevation_data)} records")
        
        # For now, return primary data - spatial joins handled in Hamilton
        return pv_data
        
    except Exception as ref_error:
        print(f"   ⚠️  Optional context data not available: {ref_error}")
        return pv_data


def _get_feature_engineering_config(dbt) -> Dict[str, Any]:
    """
    Get configuration for Hamilton feature engineering.
    
    Args:
        dbt: dbt context object
        
    Returns:
        Dict[str, Any]: Feature engineering configuration
    """
    # Get dbt variables
    feature_config = dbt.var("feature_engineering", {})
    
    # Default configuration
    default_config = {
        # Spatial features
        "h3_resolution": 8,
        "spatial_clustering": True,
        "buffer_distance_m": 1000,
        
        # ML features
        "scale_features": True,
        "encode_categorical": True,
        "handle_missing": "median",
        
        # Performance
        "use_parallel": True,
        "chunk_size": 10000
    }
    
    return {**default_config, **feature_config}


def _create_feature_engineering_driver(config: Dict[str, Any]) -> driver.Driver:
    """
    Create Hamilton driver for feature engineering.
    
    Args:
        config: Feature engineering configuration
        
    Returns:
        driver.Driver: Configured Hamilton driver
    """
    try:
        # Import available feature engineering modules
        available_modules = [transformations, validations]
        
        # Add specialized modules if available
        try:
            available_modules.extend([spatial_features, ml_features])
            print(f"   ✅ Using specialized feature engineering modules")
        except NameError:
            print(f"   ⚠️  Using basic transformation modules only")
        
        # Create driver
        hamilton_driver = (
            driver.Builder()
            .with_modules(*available_modules)
            .with_config(config)
            .with_adapter(DictResult())
            .build()
        )
        
        return hamilton_driver
        
    except Exception as driver_error:
        print(f"   ❌ Failed to create feature engineering driver: {driver_error}")
        raise


def _combine_features(
    hamilton_results: Dict[str, Any], 
    input_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine Hamilton feature engineering results with input data.
    
    Args:
        hamilton_results: Results from Hamilton execution
        input_data: Original input data
        
    Returns:
        pd.DataFrame: Combined dataset with all features
    """
    # Start with input data
    combined_data = input_data.copy()
    
    # Add spatial features if available
    if "spatial_features" in hamilton_results:
        spatial_features = hamilton_results["spatial_features"]
        if isinstance(spatial_features, pd.DataFrame):
            combined_data = pd.concat([combined_data, spatial_features], axis=1)
            print(f"   ✅ Added spatial features: {len(spatial_features.columns)} columns")
    
    # Add contextual features if available
    if "contextual_features" in hamilton_results:
        contextual_features = hamilton_results["contextual_features"]
        if isinstance(contextual_features, pd.DataFrame):
            combined_data = pd.concat([combined_data, contextual_features], axis=1)
            print(f"   ✅ Added contextual features: {len(contextual_features.columns)} columns")
    
    # Add derived features if available
    if "derived_features" in hamilton_results:
        derived_features = hamilton_results["derived_features"]
        if isinstance(derived_features, pd.DataFrame):
            combined_data = pd.concat([combined_data, derived_features], axis=1)
            print(f"   ✅ Added derived features: {len(derived_features.columns)} columns")
    
    # Add ML-ready features if available
    if "ml_ready_features" in hamilton_results:
        ml_features = hamilton_results["ml_ready_features"]
        if isinstance(ml_features, pd.DataFrame):
            combined_data = pd.concat([combined_data, ml_features], axis=1)
            print(f"   ✅ Added ML features: {len(ml_features.columns)} columns")
    
    return combined_data


def _validate_features(data: pd.DataFrame, config: Dict[str, Any]) -> None:
    """
    Validate feature engineering results.
    
    Args:
        data: Feature-engineered dataset
        config: Feature engineering configuration
    """
    # Check for required columns
    required_columns = ["dataset_name", "geometry_wkt"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    
    if missing_columns:
        print(f"   ⚠️  Missing required columns: {missing_columns}")
    
    # Check for null values in key features
    null_counts = data.isnull().sum()
    high_null_columns = null_counts[null_counts > len(data) * 0.5].index.tolist()
    
    if high_null_columns:
        print(f"   ⚠️  High null value columns (>50%): {high_null_columns}")
    
    # Check data types
    numeric_columns = data.select_dtypes(include=['number']).columns
    print(f"   📊 Numeric features: {len(numeric_columns)}")
    
    categorical_columns = data.select_dtypes(include=['object', 'string']).columns
    print(f"   📊 Categorical features: {len(categorical_columns)}")


def _create_minimal_features_fallback(input_data: pd.DataFrame) -> pd.DataFrame:
    """
    Create minimal features as fallback when Hamilton feature engineering fails.
    
    Args:
        input_data: Original input data
        
    Returns:
        pd.DataFrame: Input data with basic calculated features
    """
    print(f"   🔄 Creating minimal features fallback")
    
    fallback_data = input_data.copy()
    
    # Add basic calculated features
    if "area_m2" in fallback_data.columns:
        fallback_data["area_km2"] = fallback_data["area_m2"] / 1_000_000
        fallback_data["log_area"] = pd.np.log1p(fallback_data["area_m2"])
    
    if "centroid_lat" in fallback_data.columns and "centroid_lon" in fallback_data.columns:
        # Simple distance from equator
        fallback_data["distance_from_equator"] = abs(fallback_data["centroid_lat"])
    
    # Add processing metadata
    fallback_data["feature_engineering_status"] = "fallback"
    fallback_data["features_generated_at"] = pd.Timestamp.now()
    
    print(f"   ✅ Minimal features created: {len(fallback_data.columns)} total columns")
    
    return fallback_data
