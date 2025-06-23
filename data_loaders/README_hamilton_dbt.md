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

## Hamilton Pipeline Features (Refactored)

### ✅ **Proper Dependency Management**
The refactored pipeline now follows Hamilton's core principle: **"dependencies are defined by matching parameter names with function names"**

**Before (Problematic):**
```python
def process_single_dataset(dataset_name: str, dataset_metadata: Dict):
    # Direct function calls - breaks Hamilton dependency tracking
    download_path = download_doi_dataset(dataset_name, dataset_metadata)
    files = extract_geospatial_files(download_path, dataset_name, dataset_metadata)
```

**After (Proper Hamilton):**
```python
def dataset_download_path(target_datasets: str, dataset_metadata: Dict) -> str:
    # Parameter name 'target_datasets' matches function name 'target_datasets()'
    return download_doi_dataset(target_datasets, dataset_metadata)

def dataset_geospatial_files(dataset_download_path: str, target_datasets: str, dataset_metadata: Dict) -> List[str]:
    # Parameter name 'dataset_download_path' matches function name 'dataset_download_path()'
    return extract_geospatial_files(dataset_download_path, target_datasets, dataset_metadata)
```

### ✅ **Enhanced Function Modifiers and Tags**
```python
@tag(
    data_source="doi",
    processing_stage="download",
    data_type="file_path",
    hamilton_node_type="transform",
    io_operation="download",
    description="Download DOI dataset and return file path"
)
def dataset_download_path(...):
```

### ✅ **Proper Parallelizable and Collect Usage**
```python
def target_datasets(dataset_metadata: Dict) -> Parallelizable[str]:
    # Yields individual dataset names for parallel processing
    for dataset_name in dataset_metadata.keys():
        yield dataset_name

def collected_arrow_tables(dataset_arrow_table: Collect[pa.Table]) -> List[pa.Table]:
    # Collects all processed Arrow tables from parallel execution
    return list(dataset_arrow_table)
```

### Data Sources Handled
- ✅ **DOI Datasets**: Zenodo, USGS, GitHub repositories
- ✅ **Geospatial Formats**: GeoJSON, SHP, GPKG, JSON (not CSV)
- ✅ **Overture Maps**: Direct S3 querying with DuckDB
- ✅ **Spatial Processing**: CRS conversion, area calculation, centroids

### Pipeline Capabilities
- ✅ **Rich DAG visualizations**: Multi-dimensional dependency graphs (not flat/linear)
- ✅ **Fine-grained lineage**: Function-level dependency tracking with proper injection
- ✅ **Self-documenting**: Enhanced function names and tags create automatic docs
- ✅ **Error isolation**: Clear function-level error reporting
- ✅ **Parameterized processing**: Individual dataset functions with proper dependencies
- ✅ **Builder patterns**: Proper Hamilton driver creation for dbt integration

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

### 1. Hamilton Ingestion Layer (Refactored)
```python
# Hamilton automatically manages this dependency chain:

# 1. Load metadata configuration
dataset_metadata() -> Dict[str, Dict[str, Any]]

# 2. Generate dataset names for parallel processing
target_datasets(dataset_metadata) -> Parallelizable[str]

# 3. Download each dataset (parallel execution)
dataset_download_path(target_datasets, dataset_metadata) -> str

# 4. Extract geospatial files from downloads
dataset_geospatial_files(dataset_download_path, target_datasets, dataset_metadata) -> List[str]

# 5. Process geospatial data into standardized format
dataset_processed_gdf(dataset_geospatial_files, target_datasets, dataset_metadata) -> gpd.GeoDataFrame

# 6. Convert to Arrow format for efficient storage
dataset_arrow_table(dataset_processed_gdf, target_datasets) -> pa.Table

# 7. Collect all processed tables from parallel execution
collected_arrow_tables(dataset_arrow_table: Collect[pa.Table]) -> List[pa.Table]

# 8. Store individual datasets in DuckDB
store_individual_datasets(collected_arrow_tables, database_path) -> Dict[str, str]
```

**Key Improvement**: Hamilton now automatically tracks dependencies and enables rich DAG visualizations showing the true data flow relationships.

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

## 1. Standalone Hamilton Pipeline Execution

### Prerequisites
Ensure you have the required Hamilton dependencies installed:

```bash
# Core Hamilton dependencies
pip install sf-hamilton[visualization]  # Includes graphviz for DAG visualization
pip install pyarrow>=10.0.0            # Arrow format support
pip install geopandas>=0.13.0          # Geospatial data processing
pip install duckdb>=0.9.0              # Database operations
pip install datahugger                 # DOI dataset downloads

# Optional: For enhanced visualizations
pip install graphviz                   # System-level graphviz for better DAGs
```

### Basic Execution
```bash
# Activate your conda environment
conda activate eo-pv-cv

# Run the refactored pipeline with default settings
python raw_pv_doi_ingest.py

# Run with custom configuration
python -c "
from raw_pv_doi_ingest import run_doi_pipeline
result = run_doi_pipeline(
    database_path='./db/custom_eo_pv_data.duckdb',
    use_parallel=True,
    use_cache=True,
    max_mb=500
)
print(result)
"
```

### Configuration Parameters
The refactored pipeline supports the following configuration options:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database_path` | str | `"./db/eo_pv_data.duckdb"` | Path to DuckDB database |
| `export_geoparquet` | bool | `True` | Export to GeoParquet format |
| `use_parallel` | bool | `True` | Enable parallel processing |
| `use_cache` | bool | `True` | Use download caching |
| `force_download` | bool | `False` | Force re-download cached files |
| `max_mb` | int | `250` | Maximum file size in MB |

### Verification and Troubleshooting

**Verify Successful Execution:**
```bash
# Check database was created and populated
python -c "
import duckdb
conn = duckdb.connect('./db/eo_pv_data.duckdb')
tables = conn.execute('SHOW TABLES FROM raw_data').fetchall()
print(f'Created {len(tables)} raw data tables:')
for table in tables:
    count = conn.execute(f'SELECT COUNT(*) FROM raw_data.{table[0]}').fetchone()[0]
    print(f'  - {table[0]}: {count} records')
conn.close()
"
```

**Common Issues and Solutions:**

1. **Import Errors**: Ensure all dependencies are installed in the correct conda environment
2. **Download Failures**: Check internet connection and increase `max_mb` for large datasets
3. **Memory Issues**: Reduce parallel processing with `use_parallel=False`
4. **Visualization Errors**: Install system-level graphviz: `conda install graphviz`

### DAG Visualization
Generate rich dependency visualizations:

```bash
# Create DAG visualization
python -c "
from hamilton import driver
import raw_pv_doi_ingest as pipeline_module

dr = driver.Builder().with_modules(pipeline_module).build()
dr.display_all_functions('./dag_visualizations/hamilton_full_dag.png')
print('DAG visualization saved to ./dag_visualizations/hamilton_full_dag.png')
"
```

**Visualization Improvements**: The refactored pipeline now produces **rich, multi-dimensional DAG visualizations** instead of flat/linear graphs:
- Clear dependency relationships from sources to sinks
- Parallel processing branches and collection points
- Proper Hamilton node types (source → generator → transform → collect → sink)

## 2. dbt Integration Strategy

### Optimal Integration Approach
With our refactored Hamilton pipeline using proper Builder patterns and dependency injection, we recommend a **hybrid approach** that leverages both Hamilton's enhanced capabilities and dbt's transformation strengths:

#### A. Hamilton as dbt Pre-hook (Recommended)
```yaml
# dbt_project.yml
models:
  eo_pv_pipeline:
    raw:
      +pre-hook: "{{ run_hamilton_pipeline('doi_datasets') }}"
    staging:
      +materialized: table
```

#### B. Hamilton Pipeline Integration Macro
```sql
-- macros/run_hamilton_pipeline.sql
{% macro run_hamilton_pipeline(pipeline_name) %}
  {% if execute %}
    {% set result = run_operation('execute_hamilton_pipeline', {'pipeline': pipeline_name}) %}
    {{ log("Hamilton pipeline '" ~ pipeline_name ~ "' completed: " ~ result, info=True) }}
  {% endif %}
{% endmacro %}
```

```sql
-- macros/execute_hamilton_pipeline.sql
{% macro execute_hamilton_pipeline(pipeline) %}
  {% if pipeline == 'doi_datasets' %}
    {{ return(run_python_script('raw_pv_doi_ingest.py')) }}
  {% endif %}
{% endmacro %}
```

### Enhanced Source Registration
Leverage our improved `store_individual_datasets()` function that returns `Dict[str, str]` mappings:

```python
# hamilton_dbt_bridge.py
def generate_dbt_sources_from_hamilton():
    """Generate dbt sources.yml from Hamilton pipeline results."""
    from raw_pv_doi_ingest import run_doi_pipeline

    # Run Hamilton pipeline and get table mappings
    result = run_doi_pipeline()
    stored_tables = result  # This is now Dict[str, str] from store_individual_datasets()

    # Generate dbt sources configuration
    sources_config = {
        'version': 2,
        'sources': [{
            'name': 'raw_data',
            'description': 'Raw datasets ingested via Hamilton DOI pipeline',
            'tables': []
        }]
    }

    for dataset_name, table_info in stored_tables.items():
        table_config = {
            'name': dataset_name,
            'description': f'Raw {dataset_name} dataset from DOI source',
            'meta': {
                'hamilton_generated': True,
                'processing_stage': 'raw',
                'data_source': 'doi'
            }
        }
        sources_config['sources'][0]['tables'].append(table_config)

    return sources_config
```

### Manual vs Automatic Refresh Strategy
```bash
# Manual refresh (recommended for development)
dbt run-operation execute_hamilton_pipeline --args '{pipeline: doi_datasets}'
dbt run --models staging+

# Automatic refresh via pre-hooks (production)
dbt run --models raw+  # Triggers Hamilton pipeline via pre-hook
```

## 3. Project Directory Structure

### Recommended Organization
```
eo_pv_pipeline/
├── dbt_project.yml
├── models/
│   ├── staging/          # dbt transformation models only
│   ├── prepared/         # dbt business logic models
│   └── curated/          # dbt analytics-ready models
├── data_loaders/         # Hamilton pipelines (NEW)
│   ├── __init__.py
│   ├── raw_pv_doi_ingest.py      # DOI datasets pipeline
│   ├── overture_maps_loader.py   # Overture Maps pipeline
│   ├── solar_irradiance_loader.py # Solar data pipeline
│   └── utils/
│       ├── __init__.py
│       ├── ingestion_utils.py
│       └── hamilton_dbt_bridge.py
├── macros/
│   ├── run_hamilton_pipeline.sql
│   └── execute_hamilton_pipeline.sql
├── sources/
│   ├── raw_data.yml      # Auto-generated from Hamilton
│   └── external.yml      # Manual external sources
└── db/                   # DuckDB databases
    └── eo_pv_data.duckdb
```

### Separation of Concerns
- **`models/`**: Reserved exclusively for dbt SQL/Python models
- **`data_loaders/`**: Hamilton pipelines for data ingestion
- **`macros/`**: dbt macros for Hamilton integration
- **`sources/`**: dbt source definitions (some auto-generated)

### Multiple Hamilton Pipelines Organization
```python
# data_loaders/__init__.py
"""
Data loading pipelines using Hamilton framework.

Each pipeline handles a specific data source:
- raw_pv_doi_ingest: DOI-based PV datasets
- overture_maps_loader: Global administrative boundaries
- solar_irradiance_loader: NREL and Google Solar API data
"""

from .raw_pv_doi_ingest import run_doi_pipeline
from .overture_maps_loader import run_overture_pipeline
from .solar_irradiance_loader import run_solar_pipeline

__all__ = ['run_doi_pipeline', 'run_overture_pipeline', 'run_solar_pipeline']
```

## 4. Automatic Source Updates

### Hamilton-to-dbt Bridge Implementation
```python
# data_loaders/utils/hamilton_dbt_bridge.py
import yaml
from pathlib import Path
from typing import Dict, Any
from raw_pv_doi_ingest import run_doi_pipeline

class HamiltonDbtBridge:
    """Bridge between Hamilton pipelines and dbt source definitions."""

    def __init__(self, dbt_project_root: str = "."):
        self.dbt_root = Path(dbt_project_root)
        self.sources_dir = self.dbt_root / "sources"
        self.sources_dir.mkdir(exist_ok=True)

    def update_sources_from_hamilton(self, pipeline_name: str) -> Dict[str, Any]:
        """Update dbt sources.yml from Hamilton pipeline results."""

        if pipeline_name == "doi_datasets":
            stored_tables = run_doi_pipeline()
            source_config = self._generate_doi_sources(stored_tables)
        else:
            raise ValueError(f"Unknown pipeline: {pipeline_name}")

        # Write updated sources.yml
        sources_file = self.sources_dir / f"{pipeline_name}_sources.yml"
        with open(sources_file, 'w') as f:
            yaml.dump(source_config, f, default_flow_style=False)

        return source_config

    def _generate_doi_sources(self, stored_tables: Dict[str, str]) -> Dict[str, Any]:
        """Generate dbt sources for DOI datasets."""
        return {
            'version': 2,
            'sources': [{
                'name': 'raw_data',
                'description': 'Raw datasets from Hamilton DOI pipeline',
                'meta': {
                    'hamilton_generated': True,
                    'last_updated': '{{ run_started_at }}',
                    'pipeline': 'doi_datasets'
                },
                'tables': [
                    {
                        'name': dataset_name,
                        'description': f'Raw {dataset_name} from DOI source',
                        'meta': {
                            'table_info': table_info,
                            'hamilton_tags': {
                                'data_source': 'doi',
                                'processing_stage': 'raw',
                                'data_type': 'arrow_table'
                            }
                        },
                        'freshness': {
                            'warn_after': {'count': 7, 'period': 'day'},
                            'error_after': {'count': 14, 'period': 'day'}
                        }
                    }
                    for dataset_name, table_info in stored_tables.items()
                ]
            }]
        }
```

### dbt Macro for Automatic Updates
```sql
-- macros/update_hamilton_sources.sql
{% macro update_hamilton_sources(pipeline_name) %}
  {% if execute %}
    {% set python_script %}
from data_loaders.utils.hamilton_dbt_bridge import HamiltonDbtBridge

bridge = HamiltonDbtBridge()
result = bridge.update_sources_from_hamilton('{{ pipeline_name }}')
print(f"Updated sources for {pipeline_name}: {len(result['sources'][0]['tables'])} tables")
    {% endset %}

    {% set result = run_operation('run_python_code', {'code': python_script}) %}
    {{ log("Hamilton sources updated for " ~ pipeline_name, info=True) }}
  {% endif %}
{% endmacro %}
```

### Integration with dbt Source Freshness
```yaml
# sources/doi_datasets_sources.yml (auto-generated)
version: 2
sources:
  - name: raw_data
    description: Raw datasets from Hamilton DOI pipeline
    meta:
      hamilton_generated: true
      last_updated: "{{ run_started_at }}"
      pipeline: doi_datasets
    tables:
      - name: global_harmonized_large_solar_farms_2020
        description: Raw global_harmonized_large_solar_farms_2020 from DOI source
        freshness:
          warn_after: {count: 7, period: day}
          error_after: {count: 14, period: day}
        meta:
          hamilton_tags:
            data_source: doi
            processing_stage: raw
            data_type: arrow_table
```

### Complete Workflow Commands
```bash
# 1. Run Hamilton pipeline and update sources
dbt run-operation update_hamilton_sources --args '{pipeline_name: doi_datasets}'

# 2. Check source freshness (uses Hamilton metadata)
dbt source freshness --select source:raw_data

# 3. Run dbt transformations
dbt run --models staging+

# 4. Test everything
dbt test
```

## Next Steps

### Immediate Implementation
1. **Create `data_loaders/` directory** and move Hamilton pipelines
2. **Implement Hamilton-dbt bridge** for automatic source updates
3. **Update dbt models** to use new source structure
4. **Test end-to-end workflow** with small datasets

### Future Enhancements
1. **Hamilton UI** for pipeline monitoring and lineage
2. **Ibis integration** for cross-database compatibility
3. **Advanced caching** for expensive geospatial operations
4. **Dask scaling** for processing larger datasets
5. **CI/CD integration** with automatic Hamilton → dbt workflows

## Troubleshooting

### Hamilton Pipeline Issues
- **Import errors**: Ensure all dependencies installed: `pip install sf-hamilton[visualization] pyarrow geopandas duckdb datahugger`
- **Download failures**: Check internet connection, increase `max_mb` parameter, or disable `use_cache`
- **Geometry errors**: Verify CRS transformations and geometry validity in source data
- **Memory issues**: Reduce parallel processing with `use_parallel=False`
- **Visualization errors**: Install system graphviz: `conda install graphviz`

### dbt Integration Issues
- **Source not found**: Run `dbt run-operation update_hamilton_sources` first
- **Schema errors**: Verify `raw_data` schema exists: `SELECT * FROM information_schema.schemata WHERE schema_name = 'raw_data'`
- **Spatial functions**: Ensure DuckDB spatial extension loaded: `INSTALL spatial; LOAD spatial;`
- **Macro errors**: Check Python path includes `data_loaders/` directory
- **Freshness failures**: Hamilton data may need refresh - run pipeline manually

### Performance Optimization
- **Large datasets**: Use `max_mb` parameter to limit file sizes
- **Slow downloads**: Enable caching with `use_cache=True` (default)
- **Memory constraints**: Process datasets sequentially with `use_parallel=False`
- **Database locks**: Ensure only one Hamilton pipeline runs at a time

---

*This refactored architecture provides the best of both worlds: Hamilton's enhanced dependency management and fine-grained data loading capabilities with dbt's powerful transformation and orchestration features.*
