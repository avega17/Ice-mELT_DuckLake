# dbt Model Layers for EO PV Pipeline

This document outlines the three-layer dbt model architecture for the EO PV data pipeline.

## Layer Architecture

```
Raw Data (DuckDB) 
    ↓
📊 STAGING Layer (views)
    ↓ 
🔧 PREPARED Layer (views)
    ↓
🎯 CURATED Layer (tables)
```

## Layer Definitions

### 📊 **STAGING Layer** (`staging/`)
**Purpose**: Raw geospatial data processing with geopandas
- **Materialization**: Tables (Python models)
- **Schema**: `staging`
- **Function**: Load vector files, apply dataset-specific preprocessing, validate geometries, standardize formats

**Models**:
- `stg_doi_datasets.py` - Python model that:
  - Loads vector files (GeoJSON, Shapefile, GeoPackage) using geopandas
  - Applies existing preprocessing utilities from `utils/fetch_and_preprocess.py`
  - Validates and cleans geometries (removes invalid, deduplicates)
  - Calculates area and centroids in projected CRS
  - Converts geometries to WKT for DuckDB compatibility
  - Returns actual PV installation records (not just metadata)

### 🔧 **PREPARED Layer** (`prepared/`)
**Purpose**: Business logic transformations and data preparation
- **Materialization**: Tables (Python models)
- **Schema**: `prepared`
- **Function**: Advanced geospatial analysis, standardization, quality scoring, classification

**Models**:
- `prep_pv_datasets_unified.py` - Python model with:
  - Geometry type extraction from WKT
  - CRS standardization and validation
  - Regional and temporal classification
  - Installation size categorization
  - Quality scoring (0-100 based on completeness)
  - Advanced pandas/numpy transformations

### 🎯 **CURATED Layer** (`curated/`)
**Purpose**: Final analytical datasets ready for research and analysis
- **Materialization**: Tables
- **Schema**: `curated`
- **Function**: Aggregations, metrics, research-ready datasets

**Models**:
- `curated_pv_dataset_summary.sql` - Dataset summary with:
  - Quality scores and research priority
  - Analysis type classification
  - Regional contribution metrics
  - Usability indicators

- `curated_pv_geographic_coverage.sql` - Geographic analysis with:
  - Regional distribution statistics
  - Geometry type breakdowns
  - Repository and format analysis
  - Research value scoring

## Data Flow Example

```sql
-- STAGING: Raw data cleaning
SELECT 
    dataset_name,
    doi,
    repo as repository_type,
    label_format,
    current_timestamp as loaded_at
FROM raw_doi_datasets

-- PREPARED: Business logic and standardization  
SELECT
    dataset_name,
    CASE 
        WHEN geometry_type ILIKE '%point%' THEN 'Point'
        WHEN geometry_type ILIKE '%polygon%' THEN 'Polygon'
    END as geometry_type_standardized,
    CASE 
        WHEN dataset_name ILIKE '%usa%' THEN 'USA'
        WHEN dataset_name ILIKE '%china%' THEN 'China'
    END as region
FROM staging_data

-- CURATED: Analytics and aggregations
SELECT
    region,
    COUNT(*) as dataset_count,
    SUM(label_count) as total_installations,
    AVG(quality_score) as avg_quality
FROM prepared_data
GROUP BY region
```

## Quality Assurance

### Data Tests
- **Staging**: Basic not_null, unique constraints
- **Prepared**: Business rule validation, accepted values
- **Curated**: Analytical consistency checks

### Quality Scoring
Each dataset receives a quality score (0-100) based on:
- **Has Labels** (25 points): Dataset contains actual PV installation data
- **Valid Geometry** (25 points): Geometry type is recognized and valid
- **Valid CRS** (25 points): Coordinate system is properly defined
- **Has Files** (25 points): Dataset files are accessible

## Usage Patterns

### For Data Scientists
```sql
-- Get high-quality datasets for analysis
SELECT * FROM curated_pv_dataset_summary 
WHERE research_priority = 'High Priority'
AND quality_score >= 75;
```

### For Geographic Analysis
```sql
-- Regional coverage analysis
SELECT * FROM curated_pv_geographic_coverage
ORDER BY total_pv_installations DESC;
```

### For Data Quality Monitoring
```sql
-- Monitor data quality across regions
SELECT 
    region,
    avg_quality_score,
    dataset_count
FROM curated_pv_geographic_coverage
WHERE avg_quality_score < 75;
```

## Benefits of This Architecture

1. **🔄 Separation of Concerns**: Each layer has a clear purpose
2. **🚀 Performance**: Views for transformation, tables for analytics
3. **🔍 Transparency**: Clear data lineage from raw to curated
4. **🛡️ Quality**: Built-in data quality checks at each layer
5. **📈 Scalability**: Easy to add new datasets and transformations
6. **🔬 Research-Ready**: Curated layer optimized for analysis

## Next Steps

1. **Run the models**:
   ```bash
   dbt run
   ```

2. **Test data quality**:
   ```bash
   dbt test
   ```

3. **Generate documentation**:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

4. **Add new datasets**: Follow the same layer pattern for consistency
