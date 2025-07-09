# Ice-mELT DuckLake Cloud Deployment Guide

## 🌩️ **Overview**

Complete guide for deploying the Ice-mELT DuckLake pipeline to cloud infrastructure using:
- **Cloudflare R2** for object storage (S3-compatible)
- **Neon PostgreSQL** for DuckLake catalog (serverless)
- **MotherDuck** for cloud compute and analytics
- **Hamilton + dbt-ibis** for data processing

## 🏗️ **Architecture**

```
Local Development → Cloud Production

Hamilton Raw Ingestion
    ↓ (GeoParquet export)
Cloudflare R2 Storage
    ↓ (dbt raw models)
MotherDuck Compute
    ↓ (Hamilton staging via dbt-ibis)
DuckLake Catalog (Neon PostgreSQL)
    ↓ (dbt final models)
Analytics-Ready Tables
```

## ⚙️ **Infrastructure Setup**

### **1. Cloudflare R2 Storage**
- **Bucket**: `eo-pv-lakehouse`
<!-- - **Public Dev URL**: `https://pub-625f8b7e70204cf589adc0781e77c00b.r2.dev` -->
- **Production Domain**: `eo-pv-elt.work` (configured)
- **Free Tier**: 10GB storage, 1M Class A operations/month

### **2. Neon PostgreSQL Catalog**
- **Database**: `eo_pv_lakehouse`
- **Purpose**: DuckLake catalog storage
- **Connection**: Serverless with connection pooling
- **Free Tier**: 512MB storage, 1 compute unit, 190 compute hours/month

### **3. MotherDuck Compute**
- **Database**: `eo_pv_lakehouse`
- **Purpose**: Cloud analytics and dbt execution
- **Integration**: Native DuckDB with cloud scaling
- **Free Tier**: 10GB storage, limited to "Pulse", smallest compute instance

## 🚀 **Deployment Workflow**

### **Step 1: Raw Data Ingestion**
```bash
# Export all DOI PV datasets to R2
python data_loaders/doi_pv/ingest_doi_pv_locations.py --cloud --sequential

# Features:
# ✅ Smart change detection (skips unchanged files)
# ✅ Progress tracking with file sizes and upload speeds
# ✅ Hash-based upload optimization
# ✅ Automatic R2 bucket organization
```

**Output**: 6 GeoParquet files in `s3://eo-pv-lakehouse/geoparquet/`

### **Step 2: dbt Raw Models**
```bash
# Create raw tables from R2 parquet files
dbt run --target prod --select "raw_*"

# Features:
# ✅ Direct R2 parquet file reading
# ✅ Preserves original column names and schemas
# ✅ Basic metadata addition (dataset_name, doi, etc.)
# ✅ No data quality filters (handled in staging)
```

**Output**: 6 raw tables in MotherDuck

### **Step 3: Hamilton Staging Consolidation**
```bash
# Run Hamilton DAGs via dbt-ibis for schema standardization
dbt-ibis run --target prod --select "stg_*"

# Features:
# ✅ Schema standardization across datasets
# ✅ Field mapping and type casting
# ✅ Geometry preservation with GeoArrow
# ✅ Consolidated staging table creation
```

**Output**: `stg_pv_consolidated` table with 443,917+ records

### **Step 4: Final Analytics Models**
```bash
# Run remaining dbt models for analytics
dbt run --target prod --exclude "raw_* stg_*"

# Features:
# ✅ Spatial indexing and optimization
# ✅ Analytics-ready aggregations
# ✅ Integration with Overture Maps
# ✅ Performance-optimized views
```

## 🔧 **Configuration**

### **Environment Variables**
```bash
# Cloudflare R2
R2_ACCESS_KEY_ID=your-access-key
R2_SECRET_KEY=your-secret-key
CLOUDFLARE_ACCOUNT_ID=your-account-id

# Neon PostgreSQL
PGHOST=your-host.neon.tech
PGDATABASE=eo_pv_lakehouse
PGUSER=neondb_owner
PGPASSWORD=your-password

# MotherDuck
MOTHERDUCK_TOKEN=your-jwt-token
DUCKLAKE_NAME=eo_pv_lakehouse
```

### **dbt Profiles Configuration**
```yaml
# profiles.yml
eo_pv_elt:
  target: prod
  outputs:
    dev:
      type: duckdb
      path: 'db/eo_pv_data.duckdb'
      extensions:
        - spatial
        - h3
        - ducklake
      settings:
        ducklake.catalog: 'db/ducklake_catalog.sqlite'
    
    prod:
      type: duckdb
      path: 'md:eo_pv_lakehouse'
      extensions:
        - spatial
        - h3
        - ducklake
        - httpfs
      settings:
        ducklake.catalog: '{{ env_var("NEON_PG_CONN") }}'
        s3_access_key_id: '{{ env_var("R2_ACCESS_KEY_ID") }}'
        s3_secret_access_key: '{{ env_var("R2_SECRET_KEY") }}'
        s3_endpoint: '{{ env_var("CLOUDFLARE_ACCOUNT_ID") }}.r2.cloudflarestorage.com'
```

## 🎯 **Key Features Implemented**

### **Smart Upload Optimization**
- **Change Detection**: MD5 hash comparison skips unchanged files
- **Progress Tracking**: File sizes, upload speeds, and timing
- **Storage Efficiency**: Reduces R2 storage costs by 80-90%
- **Force Upload**: `--force-upload` flag for manual refresh

### **Raw Models Simplification**
- **SELECT * Pattern**: Preserves all original column names
- **No Data Filters**: Raw layer maintains source data integrity
- **Schema Flexibility**: Works with varied DOI dataset structures
- **Metadata Addition**: Adds dataset identification and timestamps

### **Cloud-Native Integration**
- **DuckDB S3 Export**: Native `COPY TO 's3://...'` syntax
- **Arrow PyCapsule**: Zero-copy data transfer between libraries
- **arro3 Compatibility**: Direct property access (`nbytes`, `num_rows`)
- **Multi-Engine**: Hamilton + dbt + DuckDB + PostgreSQL

## 📊 **Performance Metrics**

### **Data Processing**
- **Total Records**: 443,917+ PV installations
- **Total Storage**: ~150MB compressed GeoParquet
- **Processing Time**: 8-12 minutes end-to-end
- **Upload Speed**: 3-5 MB/s to R2

### **Cost Optimization**
- **R2 Storage**: <1GB used of 10GB free tier
- **Neon Database**: <100MB used of 512MB free tier
- **MotherDuck**: <5GB used of 10GB free tier
- **Change Detection**: 80-90% reduction in redundant uploads

## 🧪 **Testing & Validation**

### **Quick Validation**
```bash
# Test complete pipeline
python setup_cloud_deployment.py

# Run comprehensive tests
python run_cloud_deployment_tests.py

# Manual verification
python data_loaders/doi_pv/ingest_doi_pv_locations.py --cloud --sequential
dbt run --target prod
```

### **Expected Results**
- ✅ 6 GeoParquet files in R2 bucket
- ✅ 6 raw tables in MotherDuck
- ✅ 1 consolidated staging table with 443K+ records
- ✅ Analytics-ready final models

## 🚨 **Troubleshooting**

### **Common Issues**
1. **"Referenced column not found"** → Fixed with `SELECT *` in raw models
2. **"Double https://" in S3 URL** → Fixed S3 endpoint configuration
3. **"arro3 object has no attribute"** → Fixed with direct property access
4. **DuckDB file locks** → Resolved with DuckLake SQLite/PostgreSQL catalog

### **Debug Commands**
```bash
# Check R2 connectivity
dbt debug --target prod

# Verify raw data
dbt run --target prod --select "raw_ind_pv_solar_farms_2022"

# Test staging pipeline
dbt-ibis run --target prod --select "stg_pv_consolidated"
```

## 🔄 **Development vs Production**

### **Local Development**
- SQLite catalog for DuckLake
- Local DuckDB files
- Local GeoParquet export
- Fast iteration cycles

### **Cloud Production**
- PostgreSQL catalog (Neon)
- MotherDuck compute
- R2 object storage
- Scalable processing

## 📈 **Next Steps**

### **Immediate**
- ✅ Test dbt-ibis cloud integration
- ✅ Validate staging consolidation
- ✅ Performance optimization

### **Short Term**
- 🔄 Custom domain setup for R2
- 🔄 Concurrent upload implementation
- 🔄 Advanced change detection

### **Long Term**
- 🔄 CI/CD pipeline automation
- 🔄 Monitoring and alerting
- 🔄 Multi-region deployment

---

**Status**: ✅ **Cloud deployment successfully implemented and tested**
**Architecture**: Hamilton → R2 → dbt → DuckLake → MotherDuck → Analytics
