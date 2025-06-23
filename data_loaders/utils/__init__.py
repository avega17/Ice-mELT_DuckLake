"""
Utilities for Hamilton Data Loading Pipelines

This package contains utility functions and classes that support the Hamilton-based
data loading pipelines in the EO PV project.

Modules:
- hamilton_dbt_bridge: Integration utilities for connecting Hamilton pipelines with dbt
- ingestion_utils: Common utilities for data ingestion and processing (when created)

Key Components:
- HamiltonDbtBridge: Automatic dbt source generation from Hamilton pipeline results
- Pipeline validation and dependency checking utilities
- Common data processing functions for geospatial data
"""

try:
    from .hamilton_dbt_bridge import HamiltonDbtBridge
    __all__ = ['HamiltonDbtBridge']
except ImportError as e:
    import warnings
    warnings.warn(f"Could not import hamilton_dbt_bridge: {e}")
    __all__ = []

# Version information
__version__ = "0.1.0"
__description__ = "Utilities for Hamilton data loading pipelines"
