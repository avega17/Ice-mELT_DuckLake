# DuckLake SQL Quick Reference

## 📚 Reference Material

- [DuckLake Documentation](https://ducklake.select/docs/stable/)
- [Choosing a Database Catalog for DuckLake](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database)
- [DuckLake: Generally Regarded as a Good Idea](https://brojonat.com/posts/ducklake/)
- [DuckLake with Ibis Python DataFrames](https://emilsadek.com/blog/ducklake-ibis/)
- [A new data lakehouse with DuckLake and dbt](https://giacomo.coletto.io/blog/ducklake/)
- [Data Lakehouse with dbt and DuckLake](https://datalabtechtv.com/posts/data-lakehouse-dbt-ducklake/)
- [Extract data from Databases into DuckLake](https://blog.slingdata.io/extract-data-from-databases-into-ducklake)
- [cloudflare-ducklake](https://github.com/tobilg/cloudflare-ducklake)

## 🦆 Getting Started with DuckLake

### Connect to DuckLake
```sql
-- Start DuckDB CLI
duckdb

-- Install and load DuckLake extension
INSTALL ducklake;
LOAD ducklake;

-- Attach your DuckLake catalog (local development)
ATTACH 'ducklake:sqlite:db/ducklake_catalog.sqlite' AS eo_pv_lakehouse
    (DATA_PATH 'db/ducklake_data');

-- For production (MotherDuck + Neon PostgreSQL)
-- ATTACH 'ducklake:postgresql://user:pass@host/db' AS eo_pv_lakehouse
--     (DATA_PATH 'r2://bucket-name/');

-- Switch to DuckLake database
USE eo_pv_lakehouse;
```

### Basic Exploration
```sql
-- List all tables
SHOW TABLES;

-- Available tables:
-- Raw layer: raw_uk_crowdsourced_pv_2020, raw_chn_med_res_pv_2024, etc.
-- Staging layer: stg_uk_crowdsourced_pv_2020, stg_chn_med_res_pv_2024, etc.
-- Consolidated: stg_pv_consolidated
-- Prepared: prepared_dedup_h3_pv_locations

-- Describe table structure
DESCRIBE stg_pv_consolidated;

-- Quick row count
SELECT COUNT(*) FROM stg_pv_consolidated;  -- Expected: 443,917+ records

-- Sample data with H3 indexing
SELECT
    dataset_name,
    LEFT(geometry, 50) || '...' as geometry_sample,
    area_m2,
    centroid_lat,
    centroid_lon,
    h3_index_12,
    processed_at
FROM stg_pv_consolidated
LIMIT 5;
```

## 📊 Data Analysis Queries

### Dataset Overview
```sql
-- Records per dataset with H3 indexing stats
SELECT
    dataset_name,
    COUNT(*) as record_count,
    ROUND(AVG(area_m2), 2) as avg_area_m2,
    ROUND(SUM(area_m2), 2) as total_area_m2,
    COUNT(h3_index_12) as with_h3_index,
    COUNT(*) - COUNT(h3_index_12) as missing_h3_index
FROM stg_pv_consolidated
GROUP BY dataset_name
ORDER BY record_count DESC;
```

### Geographic Analysis
```sql
-- Geographic distribution (1-degree grid)
SELECT 
    FLOOR(centroid_lon) as lon_grid,
    FLOOR(centroid_lat) as lat_grid,
    COUNT(*) as installations
FROM stg_pv_consolidated
GROUP BY lon_grid, lat_grid
HAVING installations > 10
ORDER BY installations DESC;

-- Bounding box for each dataset
SELECT 
    dataset_name,
    MIN(centroid_lon) as min_lon,
    MAX(centroid_lon) as max_lon,
    MIN(centroid_lat) as min_lat,
    MAX(centroid_lat) as max_lat,
    COUNT(*) as records
FROM stg_pv_consolidated
GROUP BY dataset_name;
```

### Size Analysis
```sql
-- Installation size distribution
SELECT 
    CASE 
        WHEN area_m2 < 100 THEN 'Small (<100 m²)'
        WHEN area_m2 < 1000 THEN 'Medium (100-1000 m²)'
        WHEN area_m2 < 10000 THEN 'Large (1K-10K m²)'
        ELSE 'Very Large (>10K m²)'
    END as size_category,
    COUNT(*) as count,
    ROUND(AVG(area_m2), 2) as avg_area
FROM stg_pv_consolidated
GROUP BY size_category
ORDER BY avg_area;
```

## 🗺️ Spatial Queries

### H3 Spatial Indexing
```sql
-- Query by H3 index for efficient spatial operations
SELECT
    dataset_name,
    COUNT(*) as installations_in_h3_cell
FROM stg_pv_consolidated
WHERE h3_index_12 = 631243922309816319  -- Example H3 index
GROUP BY dataset_name;

-- Find H3 cells with multiple installations (potential duplicates)
SELECT
    h3_index_12,
    COUNT(*) as installations_count,
    COUNT(DISTINCT dataset_name) as datasets_count
FROM stg_pv_consolidated
WHERE h3_index_12 IS NOT NULL
GROUP BY h3_index_12
HAVING installations_count > 1
ORDER BY installations_count DESC
LIMIT 10;
```

### Regional Filtering
```sql
-- UK/Ireland region (approximate)
SELECT dataset_name, COUNT(*) as uk_installations
FROM stg_pv_consolidated
WHERE centroid_lon BETWEEN -10 AND 2 
  AND centroid_lat BETWEEN 50 AND 60
GROUP BY dataset_name;

-- California region (approximate)
SELECT dataset_name, COUNT(*) as ca_installations
FROM stg_pv_consolidated
WHERE centroid_lon BETWEEN -125 AND -114
  AND centroid_lat BETWEEN 32 AND 42
GROUP BY dataset_name;

-- China region (approximate)
SELECT dataset_name, COUNT(*) as china_installations
FROM stg_pv_consolidated
WHERE centroid_lon BETWEEN 73 AND 135
  AND centroid_lat BETWEEN 18 AND 54
GROUP BY dataset_name;
```

### Geometry Analysis
```sql
-- Sample WKT geometries with spatial stats
SELECT
    dataset_name,
    LEFT(geometry, 50) || '...' as geometry_sample,
    area_m2,
    centroid_lat,
    centroid_lon,
    h3_index_12
FROM stg_pv_consolidated
WHERE geometry IS NOT NULL
LIMIT 10;

-- Check for missing spatial data and H3 indexing
SELECT
    dataset_name,
    COUNT(*) as total_records,
    COUNT(geometry) as with_geometry,
    COUNT(h3_index_12) as with_h3_index,
    COUNT(*) - COUNT(geometry) as missing_geometry,
    COUNT(*) - COUNT(h3_index_12) as missing_h3_index
FROM stg_pv_consolidated
GROUP BY dataset_name;
```

### Spatial Deduplication Analysis
```sql
-- Check the prepared deduplicated table
SELECT
    COUNT(*) as deduplicated_count,
    COUNT(DISTINCT dataset_name) as datasets_count
FROM prepared_dedup_h3_pv_locations;

-- Compare before and after deduplication
SELECT
    'Before Deduplication' as stage,
    COUNT(*) as record_count
FROM stg_pv_consolidated
UNION ALL
SELECT
    'After Deduplication' as stage,
    COUNT(*) as record_count
FROM prepared_dedup_h3_pv_locations;
```

## 📤 Data Export

### Export Full Dataset
```sql
-- Export all data to Parquet
COPY (SELECT * FROM stg_pv_consolidated) 
TO 'exports/full_pv_dataset.parquet';

-- Export with compression
COPY (SELECT * FROM stg_pv_consolidated) 
TO 'exports/full_pv_dataset_compressed.parquet' 
(FORMAT PARQUET, COMPRESSION GZIP);
```

### Export by Region
```sql
-- Export UK data
COPY (
    SELECT * FROM stg_pv_consolidated
    WHERE centroid_lon BETWEEN -10 AND 2 
      AND centroid_lat BETWEEN 50 AND 60
) TO 'exports/uk_pv_installations.parquet';

-- Export large installations only
COPY (
    SELECT * FROM stg_pv_consolidated
    WHERE area_m2 > 10000
) TO 'exports/large_pv_installations.parquet';
```

### Export Summary Statistics
```sql
-- Export dataset summary
COPY (
    SELECT 
        dataset_name,
        COUNT(*) as total_installations,
        ROUND(AVG(area_m2), 2) as avg_area_m2,
        ROUND(SUM(area_m2), 2) as total_area_m2,
        MIN(centroid_lon) as min_lon,
        MAX(centroid_lon) as max_lon,
        MIN(centroid_lat) as min_lat,
        MAX(centroid_lat) as max_lat,
        MIN(processed_at) as first_processed,
        MAX(processed_at) as last_processed
    FROM stg_pv_consolidated
    GROUP BY dataset_name
) TO 'exports/dataset_summary.csv' (HEADER);
```

## 🔧 Maintenance & Utilities

### Check DuckLake Status
```sql
-- Show attached databases
SHOW DATABASES;

-- Check DuckLake tables
SELECT table_name, table_type 
FROM information_schema.tables 
WHERE table_schema = 'main';

-- Storage information (if available)
SELECT 
    table_name,
    COUNT(*) as row_count
FROM stg_pv_consolidated, information_schema.tables
WHERE table_name = 'stg_pv_consolidated'
GROUP BY table_name;
```

### Performance Queries
```sql
-- Processing timestamps
SELECT 
    MIN(processed_at) as earliest_processing,
    MAX(processed_at) as latest_processing,
    COUNT(DISTINCT DATE(processed_at)) as processing_days
FROM stg_pv_consolidated;

-- Data freshness by dataset
SELECT 
    dataset_name,
    MAX(processed_at) as last_updated,
    COUNT(*) as records
FROM stg_pv_consolidated
GROUP BY dataset_name
ORDER BY last_updated DESC;
```

## 🚀 Advanced Usage

### Join with External Data
```sql
-- Example: Join with external reference data
-- (Assumes you have a countries reference table)
/*
SELECT 
    p.dataset_name,
    c.country_name,
    COUNT(*) as installations
FROM stg_pv_consolidated p
JOIN countries c ON (
    p.centroid_lon BETWEEN c.min_lon AND c.max_lon AND
    p.centroid_lat BETWEEN c.min_lat AND c.max_lat
)
GROUP BY p.dataset_name, c.country_name;
*/
```

### Create Views
```sql
-- Create a view for large installations
CREATE VIEW large_pv_installations AS
SELECT *
FROM stg_pv_consolidated
WHERE area_m2 > 1000;

-- Use the view
SELECT dataset_name, COUNT(*) 
FROM large_pv_installations 
GROUP BY dataset_name;
```

## 💡 Tips & Best Practices

### Performance Tips
- Use `LIMIT` for exploratory queries
- Filter by `dataset_name` to reduce data scanned
- Use geographic filters (`centroid_lon/lat`) for regional analysis
- Export large results rather than displaying in terminal

### Common Patterns
```sql
-- Template for regional analysis
SELECT 
    dataset_name,
    COUNT(*) as installations,
    ROUND(AVG(area_m2), 2) as avg_area,
    MIN(processed_at) as first_seen
FROM stg_pv_consolidated
WHERE centroid_lon BETWEEN ? AND ?  -- Replace with your bounds
  AND centroid_lat BETWEEN ? AND ?   -- Replace with your bounds
GROUP BY dataset_name
ORDER BY installations DESC;
```

### Troubleshooting
```sql
-- If DuckLake seems disconnected
USE eo_pv_lakehouse;

-- If tables seem missing
SHOW TABLES;

-- If data seems stale
SELECT MAX(processed_at) FROM stg_pv_consolidated;
```

---

**Note**: This DuckLake instance contains **443,917+ PV installation records** from 6 DOI datasets, processed through the Hamilton + dbt Python model pipeline. Features include:
- **H3 spatial indexing** at resolution 12 for efficient spatial operations
- **Spatial deduplication** using H3-based overlap detection
- **Geometry statistics** including area calculations and centroid coordinates
- **WKT geometry format** for DuckLake compatibility
- **Cloud deployment** with MotherDuck + Cloudflare R2 + Neon PostgreSQL
