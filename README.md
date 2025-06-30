# ice-mELT DuckLake: EO PV Data Pipeline

A modern data lakehouse pipeline for Earth Observation (EO) photovoltaic (PV) solar panel segmentation and energy forecasting, built on open-source technologies and cloud-native architectures. This work is developed as part of the methodology for my Computer Science master's thesis at [UPR, Río Piedras](https://natsci.uprrp.edu/ccom/masters/). 

## 🎯 Project Overview

This project implements a comprehensive data pipeline for processing and analyzing global photovoltaic installation datasets, combining:
- **Vector data**: PV installation polygons and point coordinates from multiple DOI open-access datasets
- **Raster data**: Satellite multispectral imagery and irradiance data via public STAC catalogs
- **Analytical processing**: Spatial indexing, administrative boundary enrichment, and energy forecasting

## 🏗️ Architecture

<div align="center">
<img src="figures/ice-mELT_ducklake_draft.png" alt="ice-mELT DuckLake Architecture" width="40%">
</div>

### Project Name Explained

**ice-mELT DuckLake** reflects our modern data architecture approach:

- **Ice**: Leverages **Iceberg** and related open table formats and **Icechunk** tensor storage engine
- **mELT**: **Modern** data stack with **Extract-Load-Transform** pipelines ([dbt methodology](https://www.getdbt.com/blog/extract-load-transform))
- **DuckLake**: Data lakehouse architecture using the new **DuckLake** [open table and lakehouse format](https://ducklake.select/faq#what-is-ducklake) for SQL-based metadata management (see [the file explosion problem](https://www.starburst.io/blog/apache-iceberg-files/))



### Core Technologies

**Storage & Formats**
- **Local filesystem** and **S3-compatible buckets** for data storage
- **Zarr** data format with **Icechunk** tensor storage engine for rasters
- **VirtualiZarr** for virtual datasets referencing original imagery from STAC assets
- **Apache Parquet/GeoParquet** for lakehouse tables and vector data
- **Apache Arrow**, an [in-memory columnar format](https://arrow.apache.org/docs/dev/format/Intro.html) that enables [zero-copy shared memory](https://arrow.apache.org/docs/dev/format/Columnar.html) and [RPC-based data movement](https://arrow.apache.org/docs/dev/format/Flight.html) between processes and networked services
- **COG/GeoTIFF** for raster sources

**Data Lakehouse & Catalogs**
- **DuckLake** - Open lakehouse format using SQL databases for metadata management
- **Apache Iceberg** open table format for ACID transactions (complementary to DuckLake)
- **STAC** (SpatioTemporal Asset Catalog) for metadata and asset discovery
- **H3 spatial indexing** for efficient spatial operations

### 🦆 **Why DuckLake? A Key Architectural Decision**

**DuckLake** addresses fundamental limitations in existing lakehouse formats by storing metadata in a transactional SQL database rather than as "many small files" in object storage. This enables single-query metadata access, reliable ACID transactions, and seamless integration with existing SQL tools.

**Key Benefits for EO Research:**
- **Fast metadata access** for spatial workloads
- **Reliable cross-table transactions** for multi-dataset integration
- **Collaborative research** with consistent concurrent access
- **Cost-effective scaling** using free tier PostgreSQL for metadata

*For detailed design philosophy and "Big Data is Dead" perspective, see [modern_data_stack.md](modern_data_stack.md)*

**Query Engines**
- **Transactional**: PostgreSQL with pgstac, pg_mooncake, cloud via Neon/Supabase
- **Analytical**: DuckDB (local) + MotherDuck (cloud scaling)

**Transform & Processing**
- **Hamilton DAGs** for composable, self-documenting ELT dataflow pipelines
- **dbt** for Python + SQL model development, lineage, testing
- **Ibis** Python dataframe API compiling to multiple SQL backends
- **Xarray** for labeled multi-dimensional arrays
- **Tensorstore** for reading/writing large ND-arrays

### 🔄 **Hamilton Dataflows: The Modern Pipeline Approach**

Our pipeline architecture leverages **Hamilton** for function-based DAG dataflows that provide:

**Key Benefits:**
- **Lineage as Code**: Dependencies encoded directly in function signatures
- **Self-Documentation**: Pipeline structure is immediately visible and understandable
- **Composable Design**: Functions can be reused across different execution contexts
- **Built-in Caching**: Intelligent caching with automatic invalidation
- **Parallel Execution**: Native support for parallel processing with dependency management

**Design Philosophy**: Following the "Big Data is Dead" approach, our Hamilton dataflows are optimized for **medium data** workloads that fit comfortably on modern single-node systems while providing sophisticated analysis capabilities without distributed computing complexity.

*For detailed insights on modern data stack integration, see [docs/DAGs_and_Composable_Data.md](docs/DAGs_and_Composable_Data.md) and [docs/hamilton_best_practices.ipynb](docs/hamilton_best_practices.ipynb)*

**Data Sources**
- DOI datasets via **datahugger**
- STAC assets for satellite imagery
- **Overture Maps** for admin boundaries, building footprints, land cover
- **Google Solar API** and **NREL NSRDB** for irradiance data

## 📊 Current State

### ✅ Completed
- **Hamilton dataflow pipeline** for DOI PV vector datasets
  - 6+ global DOI datasets processed (~360K+ PV installations)
  - Parallel/sequential execution modes with intelligent caching
  - File filtering system using regex patterns from manifest
  - Hybrid GeoArrow/WKB approach for optimal spatial processing
  - DuckDB storage with spatial extensions + GeoParquet export
- **Modern data stack integration**
  - Hamilton DAGs for composable, self-documenting pipelines
  - dbt project structure with staging/prepared/curated layers
  - DuckDB + dbt-duckdb integration with spatial extensions
  - Apache Arrow for zero-copy data exchange
- **Development environment** with conda, extensions, and comprehensive testing

### 🔄 In Progress
- dbt Python + SQL models for data transformations
- STAC catalog integration for satellite imagery
- Spatial processing utilities (H3 indexing, admin boundaries)

## 🚀 Pipeline Features

### DOI PV Locations Dataflow (`dataflows/doi_pv_locations.py`)

**Core Capabilities:**
- **Multi-source ingestion**: Supports DOI repositories (Zenodo, Figshare, ScienceBase) and GitHub
- **Intelligent file filtering**: Regex-based patterns from `doi_manifest.json` for precise file selection
- **Parallel/Sequential execution**: Choose optimal mode based on system resources and dataset size
- **Hybrid spatial processing**: GeoArrow for efficient inter-node data exchange, WKB for DuckDB storage
- **Comprehensive caching**: Hamilton's built-in caching with dependency-aware invalidation
- **Dual output formats**: DuckDB tables for analysis + GeoParquet files for interoperability

**Processing Pipeline:**
```
DOI Metadata → Parallel Download → File Filtering → GeoPandas Loading →
GeoArrow Conversion → DuckDB Storage (WKB) + GeoParquet Export (GeoArrow)
```

**Current Datasets Processed:**
- UK Crowdsourced PV 2020: ~265K installations
- China PV 2024: ~3.4K installations
- USA California USGS PV 2016: ~19K installations
- Global PV Inventory 2021: ~50K installations
- *Total: ~360K+ PV installations across 4 active datasets*

## 🗺️ Roadmap

### Phase 1: Core Data Pipeline ✅
- [x] **Hamilton dataflow implementation**: DOI PV datasets with parallel/sequential modes
- [x] **File filtering system**: Regex-based filtering from manifest configuration
- [x] **Hybrid GeoArrow/WKB approach**: Optimal spatial processing with DuckDB compatibility
- [x] **Comprehensive caching**: Hamilton built-in caching with intelligent invalidation
- [ ] **Hamilton dataflow**: STAC catalogs and conversion to GeoParquet
- [ ] **Initial dbt models**: Python + SQL transformations leveraging Hamilton outputs

### Phase 2: STAC & Raster Integration
- [ ] **STAC dataflow module**: Hamilton pipeline for satellite imagery ingestion
- [ ] **STAC database features** using rustac and pgstac
- [ ] **STAC querying workflows** for rasters using H3 and PV labels
- [ ] **Raster processing dataflows** with Xarray and Tensorstore integration

### Phase 3: Multi-Backend & Cloud
- [ ] **Ibis integration**: Multi-SQL backend dataframe library for Hamilton outputs
- [ ] **R2 object storage** and Iceberg catalog features
- [ ] **Neon serverless PostgreSQL** for transactional workloads
- [ ] **MotherDuck integration** for cloud analytical scaling

### Phase 4: Advanced Analytics
- [ ] **VirtualiZarr dataflows** for virtual datasets referencing original assets
- [ ] **Raster-vector integration** Hamilton pipelines
- [ ] **STAC-PV datacubes** using Hamilton + dbt integration
- [ ] **Energy forecasting models** with Hamilton feature engineering

## 🚀 Getting Started

### Prerequisites
```bash
# Install conda environment
conda env create -f environment.yml
conda activate eo-pv-cv

# Install DuckDB extensions
# Extensions are auto-loaded via dbt configuration
```

### Quick Start
```bash
# 1. Run DOI PV locations pipeline (parallel mode with caching)
python run_doi_pv_pipeline.py --parallel

# 2. Alternative: sequential mode without caching
python run_doi_pv_pipeline.py --sequential --no-cache

# 3. Test dbt connection
dbt debug

# 4. Run dbt transformations
dbt run

# 5. Check data quality
dbt test

# 6. Visualize Hamilton DAG (optional)
python data_loaders/visualize_hamilton_dag.py
```

### Project Structure
```
├── dataflows/                   # Hamilton dataflow modules
│   ├── doi_pv_locations.py     # DOI PV datasets pipeline
│   └── _doi_pv_helpers_storage.py  # Storage helper functions
├── data_loaders/                # Data loading utilities
│   ├── hamilton_modules/        # Reusable Hamilton components
│   ├── utils/                   # Arrow operations, validation
│   ├── doi_manifest.json       # Dataset metadata & file filters
│   └── visualize_hamilton_dag.py  # DAG visualization
├── models/                      # dbt transformations
│   ├── staging/                 # Raw data standardization
│   ├── prepared/                # Business logic & spatial processing
│   └── curated/                 # Final analytical datasets
├── db/                          # Database files
│   ├── eo_pv_data.duckdb       # Main DuckDB database
│   └── geoparquet/             # Exported GeoParquet files
├── docs/                        # Documentation
│   ├── modern_data_stack.md    # Architecture philosophy
│   ├── DAGs_and_Composable_Data.md  # Hamilton integration
│   └── hamilton_best_practices.ipynb  # Interactive guide
├── utils/                       # Shared utilities
├── run_doi_pv_pipeline.py      # Main pipeline runner
├── dbt_project.yml             # dbt configuration
└── profiles.yml                # Database connections
```

## 🔧 Configuration

### Free Tier Strategy

Our architecture is designed to maximize free tier usage for research workloads:

**Current Setup (Local Development)**
- **DuckDB**: Local analytical processing (free)
- **Local filesystem**: Development data storage (free)

**Planned Cloud Integration (Research Scale)**
- **Cloudflare R2**: 10GB free storage + free egress
- **MotherDuck**: 10GB free analytical processing
- **Neon PostgreSQL**: 0.5GB free for DuckLake metadata
- **Supabase**: Alternative free PostgreSQL for pgstac

**Cost Projection**: Research workloads should operate entirely within free tiers (~$0/month) or minimal costs (~$5-10/month) for larger datasets.

### Environment Variables
```bash
# Optional: MotherDuck cloud scaling
export MOTHERDUCK_TOKEN="your_token"

# Optional: PostgreSQL connections (free tiers)
export NEON_POSTGRES_URL="postgresql://..."
export SUPABASE_POSTGRES_URL="postgresql://..."

# Optional: Cloudflare R2 object storage
export R2_ACCESS_KEY_ID="your_key"
export R2_SECRET_ACCESS_KEY="your_secret"
export R2_BUCKET_NAME="your_bucket"

# Optional: Google Sheets integration
export GOOGLE_SERVICE_ACCOUNT_JSON="path/to/credentials.json"
```

### Key Files
- **`profiles.yml`**: Database connections (DuckDB, MotherDuck, PostgreSQL)
- **`dbt_project.yml`**: Model configurations and DuckDB optimizations
- **`.dlt/config.toml`**: Data loading pipeline settings

## 📈 Data Products

### Current Datasets
- **Global PV installations**: 360K+ installations from 4 validated DOI sources
  - Mixed geometry types: Points, Polygons, MultiPolygons
  - Standardized to EPSG:4326 (WGS84) coordinate system
  - Available in both DuckDB (spatial queries) and GeoParquet (interoperability)
- **Administrative boundaries**: Country/region context via Overture Maps (planned)
- **H3 spatial index**: Multi-resolution hexagonal grid for efficient queries (planned)

### Pipeline Usage Examples

```bash
# Basic parallel execution with caching
python run_doi_pv_pipeline.py --parallel

# Custom database location with larger download limits
python run_doi_pv_pipeline.py --database /path/to/custom.duckdb --max-mb 500

# Sequential mode without caching for debugging
python run_doi_pv_pipeline.py --sequential --no-cache

# Disable GeoParquet export for faster processing
python run_doi_pv_pipeline.py --no-geoparquet
```

### Planned Products
- **PV-STAC datacubes**: Satellite imagery aligned with PV locations using Hamilton dataflows sourced from existing STAC catalogs without data duplication
- **Irradiance time series**: Solar potential analysis with NREL/Google Solar API integration
- **Energy forecasting models**: ML-based production estimates using Hamilton feature engineering
- **Global PV database**: Curated, harmonized installation dataset via dbt transformations

## 🤝 Contributing

This is a research project for MS thesis work. The pipeline architecture and methodologies are designed to be reproducible and extensible for similar EO data processing workflows.

## 📄 License

TBA FOSS

---

<!-- **Built with**: dlt, dbt, DuckDB, Apache Iceberg, STAC, H3, and the modern data stack -->