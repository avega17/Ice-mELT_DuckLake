"""
Hamilton modules for DOI dataset pipeline.

This package contains modular Hamilton DAG components following best practices:
- data_sources.py: Dataset metadata and download operations
- transformations.py: Geospatial processing and standardization
- validations.py: Data quality checks and validation
- storage.py: DuckDB storage operations

Each module contains Hamilton nodes (functions) that follow proper naming conventions
(nouns, not verbs) and dependency injection patterns.
"""

__version__ = "1.0.0"
__author__ = "ice-mELT DuckLake Team"

# Import all Hamilton nodes for easy access
from .data_sources import *
from .transformations import *
from .validations import *
from .storage import *

# Define module groups for different execution patterns
DATA_SOURCE_MODULES = ["data_loaders.hamilton_modules.data_sources"]
TRANSFORMATION_MODULES = ["data_loaders.hamilton_modules.transformations"]
VALIDATION_MODULES = ["data_loaders.hamilton_modules.validations"]
STORAGE_MODULES = ["data_loaders.hamilton_modules.storage"]

# Complete pipeline modules
ALL_MODULES = [
    "data_loaders.hamilton_modules.data_sources",
    "data_loaders.hamilton_modules.transformations", 
    "data_loaders.hamilton_modules.validations",
    "data_loaders.hamilton_modules.storage"
]
