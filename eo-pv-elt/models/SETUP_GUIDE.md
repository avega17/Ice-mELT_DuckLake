# 🚀 Ice-mELT DuckLake Setup Guide

Complete setup instructions for the Hamilton + dbt + DuckDB geospatial data pipeline.

## 📋 Prerequisites

- Python 3.9+
- Git
- 16GB+ RAM recommended for large datasets

## 🔧 Installation

### Step 1: Install Core Dependencies

```bash
# Install dbt with DuckDB support
pip install dbt-core>=1.8.0 dbt-duckdb>=1.8.0

# Install all project dependencies
pip install -r requirements.txt

# Verify installations
dbt --version
python -c "import hamilton; print('Hamilton installed')"
python -c "import duckdb; print(f'DuckDB {duckdb.__version__}')"
```

### Step 2: Environment Configuration

Create `.env` file in project root:

```bash
# Project Paths
REPO_ROOT=/path/to/ice-mELT_ducklake
INGEST_METADATA=/path/to/ice-mELT_ducklake/data_loaders/doi_manifest.json

# MotherDuck Cloud Configuration (optional)
MOTHERDUCK_TOKEN=your_token_here
MOTHERDUCK_DATABASE=eo_pv_cloud

# Neon PostgreSQL Configuration (optional)
NEON_DATABASE_URL=postgresql://user:pass@host/db
CLOUD_POSTGRES_URL=postgresql://user:pass@host/db
```

### Step 3: Database Setup

```bash
# Create database directory
mkdir -p db

# Test DuckDB connection and extensions
dbt debug

# Initialize database with extensions
dbt run-operation setup_extensions
```

## 🏗️ Project Structure

```
ice-mELT_ducklake/
├── dataflows/                 # Hamilton DAGs
│   ├── doi_pv_locations.py   # Core DOI ingestion
│   ├── stg_doi_pv_*.py       # Staging pipelines
│   └── _doi_pv_helpers_*.py  # Helper modules
├── models/                    # dbt models
│   ├── sources.yml           # Source definitions
│   ├── schema.yml            # Model documentation
│   └── staging/              # Python models
├── macros/                    # dbt macros
├── db/                        # DuckDB database
└── profiles.yml              # dbt connection config
```

## 🔄 Data Pipeline Workflow

### Phase 1: Hamilton DOI Ingestion

```bash
# Run Hamilton DOI pipeline
python data_loaders/ingest_doi_pv_locations.py

# Verify data loaded
duckdb db/eo_pv_data.duckdb -c "SHOW TABLES;"
```

### Phase 2: dbt Staging and Transformation

```bash
# Test dbt configuration
dbt debug

# Run staging models (executes Hamilton DAGs)
dbt run --select staging

# Run data quality tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Phase 3: Cloud Scaling (Optional)

```bash
# Test MotherDuck connection
dbt debug --target prod

# Sync to cloud
dbt run --target prod

# Validate cloud deployment
dbt test --target prod
```

## 🧪 Testing and Validation

### Data Quality Tests

```bash
# Run all tests
dbt test

# Run specific test categories
dbt test --select tag:spatial
dbt test --select tag:ground_truth
dbt test --select tag:deduplication
```

### Hamilton DAG Validation

```bash
# Visualize Hamilton DAGs
python data_loaders/visualize_hamilton_dag.py

# Test individual modules
python -m pytest data_loaders/test_doi_pv_pipeline.py
```

## 🌐 Cloud Configuration

### MotherDuck Setup

1. Sign up at [motherduck.com](https://motherduck.com)
2. Get your token from the dashboard
3. Add to `.env`: `MOTHERDUCK_TOKEN=your_token`
4. Test: `dbt debug --target prod`

### Neon PostgreSQL Setup

1. Sign up at [neon.tech](https://neon.tech)
2. Create database and get connection string
3. Add to `.env`: `NEON_DATABASE_URL=postgresql://...`
4. Test: `dbt run-operation test_postgres_connection`

## 🔍 Troubleshooting

### Common Issues

**DuckDB Extension Errors:**
```bash
# Reinstall extensions
dbt run-operation reinstall_extensions
```

**Hamilton Import Errors:**
```bash
# Check Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

**Memory Issues:**
```bash
# Increase DuckDB memory limit in profiles.yml
memory_limit: '24GB'
max_memory: '32GB'
```

### Performance Optimization

**Spatial Indexing:**
- H3 resolution 8-10 for PV installations
- Use Hilbert curve ordering for large datasets
- Enable DuckDB spatial indexing

**Parallel Processing:**
- Use Hamilton multiprocessing for country-level operations
- Configure dbt threads based on available cores
- Leverage MotherDuck for cloud scaling

## 📊 Monitoring and Maintenance

### Data Freshness

```bash
# Check data freshness
dbt source freshness

# Update source data
dbt run --select source:doi_pv_raw
```

### Performance Monitoring

```bash
# Profile query performance
dbt run --profiles-dir . --profile-template performance

# Monitor database size
duckdb db/eo_pv_data.duckdb -c "PRAGMA database_size;"
```

## 🎯 Next Steps

1. **Install Dependencies**: Follow Step 1-3 above
2. **Run Initial Pipeline**: Execute Hamilton DOI ingestion
3. **Test dbt Integration**: Run staging models
4. **Configure Cloud**: Set up MotherDuck/Neon tokens
5. **Scale to Production**: Deploy to cloud environment

For detailed technical documentation, see:
- `docs/hamilton_best_practices.ipynb`
- `data_loaders/README_hamilton_dbt.md`
- Generated dbt docs: `dbt docs serve`
