# Hamilton + dbt Integration for ice-mELT DuckLake

## Architecture Overview

This project uses **Hamilton for data ingestion** and **dbt for data transformation**, creating a clean separation of concerns:

```
DOI Datasets → Hamilton (Ingestion) → DuckDB Raw Schema → dbt (Transformation) → Analytics
```

### Why This Architecture?

- **Hamilton**: Excels at fine-grained data loading, geospatial processing, and lineage tracking
- **dbt**: Excels at SQL transformations, testing, documentation, and orchestration
- **Integration**: Hamilton can run within dbt Python models when needed

## Quick Start

### 1. Install Dependencies

```bash
# Hamilton for data ingestion
pip install -r requirements_hamilton.txt

# dbt for transformations (already configured)
pip install dbt-duckdb
```

### 2. Run Hamilton Data Ingestion

```bash
# Test the pipeline
python test_hamilton_pipeline.py

# Load specific datasets
python run_hamilton_pipeline.py --datasets bradbury stowell --max-mb 50

# Load all datasets
python run_hamilton_pipeline.py --all --max-mb 100
```

This creates raw data in DuckDB's `raw_data` schema:
- `raw_data.doi_pv_features` - PV installation features
- `raw_data.admin_boundaries` - Overture Maps boundaries (views)

### 3. Run dbt Transformations

```bash
# Run staging models (consume Hamilton data)
dbt run --select staging

# Run full pipeline
dbt run

# Generate documentation
dbt docs generate && dbt docs serve
```

## File Structure

```
├── raw_pv_doi_ingest.py          # Main Hamilton ingestion pipeline
├── hamilton_dbt_integration.py       # Hamilton functions for use in dbt
├── run_hamilton_pipeline.py          # CLI for Hamilton pipeline
├── test_hamilton_pipeline.py         # Hamilton tests
├── requirements_hamilton.txt         # Hamilton dependencies
├── models/
│   ├── sources_hamilton.yml          # dbt sources for Hamilton data
│   ├── staging/
│   │   ├── stg_hamilton_pv_features.sql  # dbt staging model
│   │   └── stg_doi_datasets.py       # Existing dbt Python model
│   ├── prepared/                     # dbt prepared models
│   └── curated/                      # dbt curated models
└── dlt_to_hamilton_migration.md      # Migration documentation
```

## Hamilton Pipeline Features

### Data Sources Handled
- ✅ **DOI Datasets**: Zenodo, USGS, GitHub repositories
- ✅ **Geospatial Formats**: GeoJSON, SHP, GPKG, JSON (not CSV)
- ✅ **Overture Maps**: Direct S3 querying with DuckDB
- ✅ **Spatial Processing**: CRS conversion, area calculation, centroids

### Pipeline Capabilities
- ✅ **Fine-grained lineage**: Function-level dependency tracking
- ✅ **Self-documenting**: Function names and tags create automatic docs
- ✅ **Error isolation**: Clear function-level error reporting
- ✅ **Parameterized processing**: Individual dataset functions
- ✅ **Visualization**: DAG generation and lineage display

## dbt Integration Patterns

### Pattern 1: Hamilton → dbt Sources

Hamilton loads raw data that dbt references as sources:

```yaml
# models/sources_hamilton.yml
sources:
  - name: raw_data
    tables:
      - name: doi_pv_features  # Loaded by Hamilton
```

```sql
-- models/staging/stg_pv_features.sql
select * from {{ source('raw_data', 'doi_pv_features') }}
```

### Pattern 2: Hamilton within dbt Python Models

For complex geospatial processing within dbt:

```python
# models/staging/stg_pv_features_hamilton.py
def model(dbt, session):
    raw_data = dbt.source('raw_data', 'doi_pv_features')
    
    # Use Hamilton for fine-grained processing
    from hamilton import driver
    import hamilton_dbt_integration as processing
    
    hamilton_driver = driver.Driver(config, processing)
    result = hamilton_driver.execute(['processed_features'], 
                                   inputs={'raw_data': raw_data})
    return result['processed_features']
```

## Data Flow

### 1. Hamilton Ingestion Layer
```python
# Download DOI datasets
download_path = download_doi_dataset(dataset_name, metadata)

# Extract geospatial files  
geospatial_files = extract_geospatial_files(download_path, dataset_name)

# Process with geopandas
processed_data = process_geospatial_data(geospatial_files, dataset_name)

# Export to GeoParquet and load to DuckDB
export_geoparquet(processed_data, dataset_name)
load_raw_to_duckdb(processed_data, database_path)
```

### 2. dbt Transformation Layer
```sql
-- Staging: Clean and standardize
{{ source('raw_data', 'doi_pv_features') }} → stg_pv_features

-- Prepared: Add business logic and spatial context  
stg_pv_features → prep_pv_datasets_unified

-- Curated: Final analytical datasets
prep_pv_datasets_unified → curated_pv_dataset_summary
```

## Benefits Over dlt

### Complexity Reduction
- ❌ **dlt**: Complex threading, schema evolution, resource abstractions
- ✅ **Hamilton**: Pure Python functions with explicit dependencies

### Debugging & Lineage
- ❌ **dlt**: Mysterious stalls, complex stack traces through threading
- ✅ **Hamilton**: Function-level errors, visual DAG, fine-grained lineage

### Geospatial Handling
- ❌ **dlt**: Filesystem source confusion, metadata vs data issues
- ✅ **Hamilton**: Direct geopandas processing of all geospatial formats

### Integration
- ❌ **dlt**: Separate tool with its own abstractions
- ✅ **Hamilton**: Can run within dbt Python models seamlessly

## Usage Examples

### Basic Hamilton Pipeline
```bash
# Process specific datasets
python run_hamilton_pipeline.py --datasets bradbury stowell

# Visualize pipeline DAG
python run_hamilton_pipeline.py --visualize

# Show data lineage
python run_hamilton_pipeline.py --lineage
```

### Programmatic Usage
```python
from raw_pv_doi_ingest import run_doi_pipeline

# Load data for dbt consumption
result = run_doi_pipeline(
    datasets=["bradbury_2016_california"],
    max_mb=100,
    database_path="./db/eo_pv_data.duckdb"
)

# Data is now available in raw_data.doi_pv_features
```

### dbt Integration
```bash
# After Hamilton loads raw data
dbt source freshness  # Check Hamilton data freshness
dbt run --select staging  # Process Hamilton data
dbt test  # Validate transformations
```

## Next Steps

### Immediate
1. **Test Hamilton pipeline** with small datasets
2. **Update dbt models** to use Hamilton sources
3. **Validate end-to-end** workflow

### Future Enhancements
1. **Hamilton UI** for pipeline monitoring
2. **Ibis integration** for cross-database compatibility
3. **Caching** for expensive operations
4. **Dask scaling** for larger datasets

## Troubleshooting

### Hamilton Issues
- **Import errors**: Ensure `pip install -r requirements_hamilton.txt`
- **Download failures**: Check network and DOI URLs
- **Geometry errors**: Verify CRS and geometry validity

### dbt Integration Issues
- **Source not found**: Ensure Hamilton pipeline completed successfully
- **Schema errors**: Check `raw_data` schema exists in DuckDB
- **Spatial functions**: Verify DuckDB spatial extension is loaded

---

*This architecture provides the best of both worlds: Hamilton's fine-grained data loading capabilities with dbt's powerful transformation and orchestration features.*
