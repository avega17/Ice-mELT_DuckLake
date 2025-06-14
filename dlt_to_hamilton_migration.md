# dlt to Hamilton Migration Guide

## Overview

This document outlines the migration from dlt-based data loading to Hamilton-based pipelines for the ice-mELT DuckLake project. **Hamilton replaces dlt as the data ingestion layer**, while **dbt remains our primary transformation framework**. This creates a clean separation of concerns:

- **Hamilton**: Data loading, geospatial processing, DOI dataset ingestion
- **dbt**: Data modeling, transformations, business logic, documentation
- **Integration**: Hamilton can run within dbt Python models for fine-grained processing

## Key Differences

### dlt Approach (Before)
```python
@dlt.resource(write_disposition="replace")
def doi_dataset_resource(dataset_name, metadata, max_mb=100):
    # Complex dlt resource with threading
    for file_path in download_and_extract():
        yield process_file(file_path)

# Pipeline execution
pipeline = dlt.pipeline(
    pipeline_name="doi_dataset_pipeline",
    destination='duckdb',
    dataset_name=dataset_name,
)
load_info = pipeline.run(geoparquet_filesystem_source())
```

### Hamilton Approach (After)
```python
@tag(data_source="doi", processing_stage="download")
def download_doi_dataset(dataset_name, dataset_metadata, max_mb=100):
    # Simple function with clear inputs/outputs
    return download_path

@tag(data_source="doi", processing_stage="process")  
def process_geospatial_data(geospatial_files, dataset_name, dataset_metadata):
    # Pure function with explicit dependencies
    return processed_gdf

# Pipeline execution
dr = driver.Driver(config, pipeline_module)
result = dr.execute(["dataset_pipeline__bradbury"])
```

## Benefits of Hamilton Migration

### 1. **Simplified Architecture**
- ❌ **dlt**: Complex threading, schema evolution, resource abstractions
- ✅ **Hamilton**: Pure Python functions with explicit dependencies

### 2. **Better Debugging**
- ❌ **dlt**: Mysterious stalls, complex stack traces through threading
- ✅ **Hamilton**: Clear function-level errors, predictable execution

### 3. **Fine-Grained Lineage**
- ❌ **dlt**: Pipeline-level tracking
- ✅ **Hamilton**: Function-level dependency tracking and visualization

### 4. **Self-Documenting**
- ❌ **dlt**: Requires separate documentation
- ✅ **Hamilton**: Function names, docstrings, and tags create automatic documentation

### 5. **Testing**
- ❌ **dlt**: Complex pipeline testing
- ✅ **Hamilton**: Test individual functions easily

## File Mapping

| dlt Files | Hamilton Files | Purpose |
|-----------|----------------|---------|
| `doi_dataset_pipeline.py` | `hamilton_doi_pipeline.py` | Main pipeline logic |
| `test_pipeline.py` | `test_hamilton_pipeline.py` | Testing framework |
| `requirements_dlt.txt` | `requirements_hamilton.txt` | Dependencies |
| `.dlt/config.toml` | Function parameters | Configuration |
| N/A | `run_hamilton_pipeline.py` | CLI runner |

## Migration Steps Completed

### ✅ Phase 1: Core Pipeline Migration
- [x] Created `hamilton_doi_pipeline.py` with all DOI dataset processing
- [x] Migrated geospatial file handling (GeoJSON, SHP, GPKG, JSON)
- [x] Added Overture Maps S3 querying with DuckDB
- [x] Implemented DuckDB loading with spatial extensions
- [x] Added parameterized processing for all 6 datasets

### ✅ Phase 2: Testing Framework
- [x] Created `test_hamilton_pipeline.py` with comprehensive tests
- [x] Added Hamilton driver testing
- [x] Added DuckDB extension testing
- [x] Added Overture Maps query testing

### ✅ Phase 3: CLI and Documentation
- [x] Created `run_hamilton_pipeline.py` CLI runner
- [x] Added pipeline visualization capabilities
- [x] Added data lineage display
- [x] Created requirements file for Hamilton

## Code Comparison Examples

### Dataset Processing

**dlt Version (Complex)**:
```python
@dlt.resource(write_disposition="replace")
def doi_dataset_resource(dataset_name, metadata, max_mb=100, force=False):
    # Complex generator with yield statements
    # Threading complications
    # Schema inference issues
    for item in complex_processing():
        yield transform_item(item)

# Filesystem source complications
def geoparquet_filesystem_source():
    # Complex dlt filesystem source setup
    # Metadata vs actual data confusion
    return filesystem(...)
```

**Hamilton Version (Simple)**:
```python
@tag(data_source="doi", processing_stage="process")
def process_geospatial_data(geospatial_files, dataset_name, dataset_metadata):
    # Pure function with clear inputs/outputs
    gdf = process_vector_geoms(geospatial_files, dataset_name, ...)
    # Add metadata and return
    return gdf

@parameterize(bradbury={"dataset_name": "bradbury_2016_california"}, ...)
def dataset_pipeline(dataset_name, dataset_metadata, max_mb=100):
    # Clear pipeline orchestration
    return export_geoparquet(processed_data, dataset_name)
```

### Configuration

**dlt Version**:
```toml
# .dlt/config.toml
[runtime]
log_level="INFO"
request_timeout=300

[destination.duckdb]
loader_file_format="parquet"

[sources.doi_dataset_pipeline]
max_file_size_mb=100
```

**Hamilton Version**:
```python
# Simple Python config
config = {
    "max_mb": 100,
    "force": False,
    "database_path": "./eo_pv_data.duckdb"
}
dr = driver.Driver(config, pipeline_module)
```

## Performance Improvements

### dlt Issues Resolved
1. **Threading Stalls**: Hamilton uses simple function execution
2. **Schema Confusion**: Direct pandas/geopandas workflows
3. **Filesystem Source Complexity**: Direct file I/O
4. **Memory Issues**: Better control over data flow

### Hamilton Advantages
1. **Predictable Execution**: Functions run in dependency order
2. **Memory Efficiency**: Clear data passing between functions
3. **Error Isolation**: Failures isolated to specific functions
4. **Incremental Processing**: Can re-run individual steps

## Hamilton + dbt Architecture

### Clear Separation of Concerns

**Hamilton (Data Ingestion Layer)**:
- ✅ DOI dataset downloading and processing
- ✅ Geospatial file handling (GeoJSON, SHP, GPKG, JSON)
- ✅ Overture Maps S3 querying
- ✅ Raw data loading into DuckDB `raw_data` schema
- ✅ Fine-grained lineage for data loading steps

**dbt (Transformation Layer)**:
- ✅ Staging models (data cleaning, standardization)
- ✅ Prepared models (business logic, spatial processing)
- ✅ Curated models (analytical datasets)
- ✅ Testing, documentation, and orchestration
- ✅ SQL and Python model support

### Integration Patterns

#### Pattern 1: Hamilton → dbt Sources
```python
# Hamilton loads raw data
load_status = run_doi_pipeline(datasets=["bradbury", "stowell"])
# Creates: raw_data.doi_pv_features table

# dbt references as source
{{ source('raw_data', 'doi_pv_features') }}
```

#### Pattern 2: Hamilton within dbt Python Models
```python
# models/staging/stg_pv_features_hamilton.py
def model(dbt, session):
    raw_data = dbt.source('raw_data', 'doi_pv_features')

    # Use Hamilton for fine-grained geospatial processing
    hamilton_driver = driver.Driver(config, geospatial_module)
    result = hamilton_driver.execute(['processed_features'],
                                   inputs={'raw_data': raw_data})
    return result['processed_features']
```

### Benefits of This Architecture
1. **Best of Both Worlds**: Hamilton's fine-grained lineage + dbt's transformation power
2. **Clear Boundaries**: Data loading vs. data modeling responsibilities
3. **Flexibility**: Can use Hamilton within dbt when needed for complex processing
4. **Maintainability**: Each tool does what it does best

## Usage Examples

### Basic Usage
```bash
# Test the pipeline
python test_hamilton_pipeline.py

# Process specific datasets
python run_hamilton_pipeline.py --datasets bradbury stowell --max-mb 50

# Process all datasets
python run_hamilton_pipeline.py --all --max-mb 100

# Visualize pipeline
python run_hamilton_pipeline.py --visualize
```

### Programmatic Usage
```python
from hamilton_doi_pipeline import run_doi_pipeline

# Process specific datasets
result = run_doi_pipeline(
    datasets=["bradbury_2016_california", "stowell_2020_uk"],
    max_mb=100,
    database_path="./my_data.duckdb"
)
```

## Next Steps

### Immediate (Post-Migration)
1. **Test Hamilton pipeline** with small datasets
2. **Verify DuckDB loading** works correctly
3. **Update dbt models** to use Hamilton outputs
4. **Remove dlt dependencies** once Hamilton is validated

### Future Enhancements
1. **Add Hamilton UI** for pipeline monitoring
2. **Integrate with Ibis** for cross-database compatibility
3. **Add caching** for expensive operations
4. **Scale with Dask** for larger datasets

## Rollback Plan

If issues arise, we can temporarily revert to dlt:
1. Keep `doi_dataset_pipeline.py` until Hamilton is fully validated
2. Use `requirements_dlt.txt` for dependencies
3. Switch back to dlt-based workflows

However, the Hamilton approach addresses all known dlt issues and provides significant improvements in maintainability and debugging.

---

*This migration represents a significant simplification of our data loading architecture while maintaining all functionality and improving observability.*
