# dbt-Hamilton Integration Architecture Plan

## Overview

This document outlines the architecture for integrating our refactored Hamilton DOI pipeline with dbt Python models, following best practices for modern data stack integration.

## Integration Strategy

### Current State
- **Hamilton Pipeline**: Modular, refactored DOI dataset ingestion with proper dependency injection
- **dbt Project**: Existing staging/prepared/curated layer structure
- **Storage**: DuckDB with raw_data schema populated by Hamilton

### Target Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw Data      │    │   dbt Models    │    │  Data Products  │
│   (Hamilton)    │───▶│   (Python+SQL)  │───▶│   (Analytics)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
│                      │                      │
│ • DOI datasets       │ • Staging models     │ • Curated PV DB
│ • Geospatial data    │ • Prepared models    │ • Quality metrics
│ • Raw ingestion      │ • Hamilton drivers   │ • Spatial indices
│ • Quality validation │ • Feature engineering│ • Analysis views
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Integration Patterns

### Pattern 1: Hamilton as dbt Python Model Data Loader

**Use Case**: Load and process complex geospatial data within dbt Python models

```python
# models/staging/stg_pv_datasets.py
import pandas as pd
from hamilton import driver
from data_loaders.hamilton_modules import data_sources, transformations

def model(dbt, session):
    """
    dbt Python model that uses Hamilton for complex data loading.
    """
    # Hamilton driver for data loading
    config = {"use_cache": True, "max_mb": 300}
    dr = driver.Builder().with_modules(
        data_sources, transformations
    ).with_config(config).build()
    
    # Execute Hamilton pipeline
    results = dr.execute(["processed_geodataframe"])
    
    # Return processed data for dbt
    return results["processed_geodataframe"]
```

### Pattern 2: Hamilton for Feature Engineering in dbt

**Use Case**: Complex spatial feature engineering within dbt models

```python
# models/prepared/prep_spatial_features.py
def model(dbt, session):
    """
    dbt model using Hamilton for spatial feature engineering.
    """
    # Get staging data from dbt
    raw_pv_data = dbt.ref("stg_pv_datasets")
    
    # Hamilton driver for feature engineering
    from hamilton_modules import spatial_features
    dr = driver.Builder().with_modules(spatial_features).build()
    
    # Execute feature engineering
    results = dr.execute(
        ["h3_indices", "spatial_clusters", "area_calculations"],
        inputs={"pv_geodataframe": raw_pv_data}
    )
    
    return results["spatial_features_combined"]
```

### Pattern 3: Hamilton for Data Quality in dbt

**Use Case**: Comprehensive data quality checks and validation

```python
# models/quality/quality_pv_datasets.py
def model(dbt, session):
    """
    dbt model using Hamilton for data quality assessment.
    """
    prepared_data = dbt.ref("prep_spatial_features")
    
    # Hamilton driver for quality checks
    from hamilton_modules import validations, quality_metrics
    dr = driver.Builder().with_modules(
        validations, quality_metrics
    ).build()
    
    results = dr.execute(
        ["data_quality_report"],
        inputs={"dataset": prepared_data}
    )
    
    return results["data_quality_report"]
```

## Implementation Phases

### Phase 1: Foundation Setup (Current Sprint)
- [x] Refactor Hamilton pipeline to modular architecture
- [x] Implement proper dependency injection patterns
- [x] Create utility modules for reusable operations
- [ ] **Create dbt Python model templates**
- [ ] **Test basic Hamilton-dbt integration**

### Phase 2: Staging Models Integration
- [ ] Convert existing staging models to use Hamilton drivers
- [ ] Implement spatial feature engineering modules
- [ ] Add data quality validation modules
- [ ] Create reusable Hamilton components for dbt

### Phase 3: Advanced Integration
- [ ] Implement H3 spatial indexing with Hamilton
- [ ] Add administrative boundary enrichment
- [ ] Create ML feature engineering pipelines
- [ ] Integrate with STAC catalog operations

### Phase 4: Production Optimization
- [ ] Optimize Hamilton-dbt performance
- [ ] Implement caching strategies
- [ ] Add monitoring and observability
- [ ] Create deployment automation

## Technical Architecture

### Directory Structure
```
├── models/
│   ├── staging/
│   │   ├── stg_pv_datasets.py          # Hamilton data loading
│   │   └── stg_spatial_context.py      # Spatial context loading
│   ├── prepared/
│   │   ├── prep_spatial_features.py    # Hamilton feature engineering
│   │   └── prep_quality_metrics.py     # Hamilton quality assessment
│   └── curated/
│       ├── curated_pv_database.sql     # Final SQL aggregations
│       └── curated_spatial_indices.py  # Hamilton spatial processing
├── hamilton_modules/                    # Reusable Hamilton components
│   ├── dbt_adapters/                   # dbt-specific Hamilton modules
│   │   ├── data_loaders.py
│   │   ├── feature_engineering.py
│   │   ├── spatial_operations.py
│   │   └── quality_assessment.py
│   └── shared/                         # Shared utility modules
└── macros/
    └── hamilton_utils.sql              # dbt macros for Hamilton integration
```

### Configuration Management

**dbt_project.yml additions:**
```yaml
models:
  ice_melt_ducklake:
    staging:
      +materialized: table
      +python_env: hamilton_env
    prepared:
      +materialized: table
      +python_env: hamilton_env
    curated:
      +materialized: view

vars:
  hamilton_config:
    use_cache: true
    max_mb: 300
    database_path: "{{ target.path }}/eo_pv_data.duckdb"
```

### Hamilton Module Design for dbt

**Key Principles:**
1. **dbt-Aware Modules**: Hamilton modules designed specifically for dbt integration
2. **Stateless Operations**: All Hamilton functions remain pure and stateless
3. **Configuration Injection**: Use dbt variables for Hamilton configuration
4. **Error Handling**: Graceful failures that don't break dbt runs
5. **Performance**: Optimized for dbt's execution patterns

## Benefits of This Architecture

### For Development
- **Separation of Concerns**: SQL for data modeling, Python for complex transformations
- **Reusability**: Hamilton modules can be used across multiple dbt models
- **Testing**: Both dbt tests and Hamilton unit tests
- **Documentation**: Combined dbt docs and Hamilton lineage

### For Production
- **Scalability**: Hamilton's parallel execution within dbt's orchestration
- **Monitoring**: dbt's built-in monitoring + Hamilton's detailed lineage
- **Maintenance**: Modular components easier to update and debug
- **Quality**: Comprehensive validation at multiple levels

## Next Steps

1. **Create dbt Python Model Templates**
2. **Implement Basic Integration Test** (This Week)
3. **Document Integration Patterns** (Ongoing)

## Resources

- [Hamilton dbt Integration Guide](https://medium.com/data-science/hamilton-dbt-in-5-minutes-62e4cb63f08f)
- [dbt Python Models Documentation](https://docs.getdbt.com/docs/build/python-models)
- [Hamilton Best Practices](https://hamilton.dagworks.io/en/latest/concepts/best-practices/)

---

*This integration plan will be updated as we implement and learn from the dbt-Hamilton integration process.*
