# Ice-m<sup>3</sup>ELT DuckLake

Modern data lakehouse pipeline for Earth Observation (EO) photovoltaic (PV) solar panel segmentation and energy forecasting, built on open-source and cloud-native technologies. This work is developed as part of the methodology for my Computer Science master's thesis at [UPR, Río Piedras](https://natsci.uprrp.edu/ccom/masters/). 

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
- **m<sup>3</sup>ELT**: **Modern** and **Multi-Modal** data stack with **Extract-Load-Transform** pipelines ([dbt methodology](https://www.getdbt.com/blog/extract-load-transform))
- **DuckLake**: Data lakehouse architecture using the new **DuckLake** [open table and lakehouse format](https://ducklake.select/faq#what-is-ducklake) for SQL-based metadata management (see [the file explosion problem](https://www.starburst.io/blog/apache-iceberg-files/))

### Core Technologies and Spatio-Temporal Data

**Storage & Formats**
- **Local filesystem** and **S3-compatible buckets** for data storage
- **Zarr** data format with **Icechunk** tensor storage engine for rasters
- **VirtualiZarr** for virtual datasets referencing original imagery from STAC assets
- **Apache Parquet/GeoParquet** for lakehouse tables and vector data
- **Apache Arrow**, an [in-memory columnar format](https://arrow.apache.org/docs/dev/format/Intro.html) that enables [zero-copy shared memory](https://arrow.apache.org/docs/dev/format/Columnar.html) and [RPC-based data movement](https://arrow.apache.org/docs/dev/format/Flight.html) between processes and networked services
- **COG/GeoTIFF** for raster sources


**Transform & Processing**
- **Hamilton DAGs** for composable, self-documenting dataflows and pipelines
- **dbt** for Python + SQL model development, lineage, testing
- **Ibis** Python dataframe API compiling to multiple SQL backends
- **Xarray** for labeled multi-dimensional arrays
- **Tensorstore** for performant reading/writing of large ND-arrays

**Query Engines**
- **Transactional**: PostgreSQL with pgstac, pg_mooncake, cloud via Neon/Supabase
- **Analytical**: DuckDB (local) + MotherDuck (cloud scaling)

**Data Lakehouse & Catalogs**
- **DuckLake** - Open lakehouse format using SQL databases for metadata management
- **Apache Iceberg** open table format for ACID transactions (complementary to DuckLake)
- **STAC** (SpatioTemporal Asset Catalog) for metadata and asset discovery
- **H3 spatial indexing** for efficient spatial operations

**Data Sources**
- DOI datasets via **datahugger**
- STAC assets for satellite imagery
- **Overture Maps** for admin boundaries, building footprints, land cover
- **Google Solar API** and **NREL NSRDB** for irradiance data

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

#### 🚀 Implemented Pipeline Features

##### DOI PV Locations Dataflow (`dataflows/doi_pv_locations.py`)

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
GeoArrow-RS Conversion → DuckDB Storage + GeoParquet Export (native I/O)
```

**Current Datasets Processed:**
- doi_uk_crowdsourced_pv_2020: 265,406 records
- doi_chn_med_res_pv_2024: 1,852 records
- doi_usa_cali_usgs_pv_2016: 19,433 records
- doi_ind_pv_solar_farms_2022: 1,363 records
- doi_global_pv_inventory_sent2_spot_2021: 36,882 records
- doi_global_harmonized_large_solar_farms_2020: 35,272 records
- *Total: ~380K deduplicated PV installations across 6 active datasets*

### 🦆 **Why DuckLake? A Key Architectural Decision**

**DuckLake** addresses fundamental limitations in existing lakehouse formats by storing metadata in a transactional SQL database rather than as "many small files" in object storage. This enables single-query metadata access, reliable ACID transactions, and seamless integration with existing tools and decades of DBMS advances since at it's core it simply **builds on SQL and Parquet**. A key DuckLake contribution to the data lakehouse architecture is is adding another dimmension to scale: Storage, compute, AND metadata can all scale independently.

> *DuckLake re-imagines what a “Lakehouse” format should look like by acknowledging two simple truths:*  
> *1. Storing data files in open formats on blob storage is a great idea for scalability and to prevent [cloud and data vendor] lock-in.*  
> *2. Managing metadata is a complex and interconnected data management task best left to a database management system.*  

-- [The DuckLake Manifesto: SQL as a Lakehouse Format](https://ducklake.select/manifesto/#ducklake)

#### Key Benefits for EO Research:
- **Fast metadata access** for spatial workloads
- **Reliable cross-table transactions** for multi-dataset integration
- **Collaborative research** with consistent concurrent access
- **Cost-effective scaling** using free tier PostgreSQL for metadata

*For more details on design philosophy and "Big Data is Dead" perspective, see [modern_data_stack.md](docs/modern_data_stack.md)*

## 📊 Current State

### ✅ Completed
- **Hamilton dataflow pipeline** for DOI PV vector datasets
  - 6 global DOI datasets processed (443,917+ PV installations)
  - Parallel/sequential execution modes with intelligent caching
  - File filtering system using regex or glob patterns from DOI manifest
  - GeoArrow-RS integration for efficient spatial operations with native I/O
  - DuckDB storage with spatial extensions + GeoParquet export
- **ELT Pipeline with dbt + Hamilton Integration**
  - Individual staging models with Hamilton DAG spatial processing
  - Geometry statistics calculation (area_m2, centroid_lat/lon)
  - H3 spatial indexing for efficient deduplication (configurable resolution)
  - Consolidated staging model with basic union and exact duplicate removal
  - Spatial deduplication using H3-based overlap detection
  - dbt Python models replacing dbt-ibis patterns for better reliability
- **Cloud Production Deployment**
  - MotherDuck for cloud compute and analytics
  - Cloudflare R2 for object storage (S3-compatible)
  - Neon PostgreSQL for DuckLake catalog metadata
  - same dbt models work for both dev (local) and prod (cloud) environments 
- **Modern data stack integration**
  - Hamilton DAGs for composable, self-documenting pipelines
  - dbt project structure with raw/staging/prepared/curated layers
  - DuckDB + dbt-duckdb integration with spatial extensions
  - Apache Arrow for zero-copy data exchange
  - DuckLake for SQL-based lakehouse metadata management
- **Development environment** with conda, extensions, and comprehensive testing
  - [Future] Migrate dependencies to [uv, a python package manager](https://www.datacamp.com/tutorial/python-uv) [implemented in rust](https://www.saaspegasus.com/guides/uv-deep-dive/#why-use-uv) (see [pros and cons](https://www.bitecode.dev/p/a-year-of-uv-pros-cons-and-should))

### 🔄 In Progress
- dbt Python + SQL staging models for data transformations (overture, ERA5, other prepared models, etc)
- STAC catalog integration for satellite imagery
- Overture Maps integration for administrative boundaries
- Spatial processing utilities (H3 indexing, admin boundaries) for enhanced spatial context

## 🗺️ Roadmap

### Phase 1: Core Data Pipeline ✅
- [x] **Hamilton dataflow implementation**: DOI PV datasets with parallel/sequential modes; will be used in other coming dataflows
- [x] **File filtering system**: Regex-based filtering from manifest configuration
- [x] **GeoArrow-RS integration**: (WIP) Rust-based spatial operations with native from_geopandas()/to_geopandas() and I/O
- [x] **Comprehensive caching**: Hamilton built-in caching with intelligent invalidation
- [x] **dbt raw models**: Loading ingestion resulting files as dbt models for both dev and prod environments
- [x] **dbt staging models**: Individual models with Hamilton DAG spatial processing and H3 indexing
- [x] **dbt consolidation**: Union staging models with exact duplicate removal
- [x] **Spatial deduplication**: H3-based overlap detection and removal in prepared layer (init draft)
- [ ] **Data Fusion with Overture Maps and other data sources** 


### Phase 2: STAC & Raster Integration
- [ ] **STAC dataflow module**: Hamilton pipeline for satellite imagery ingestion as static GeoParquet collections
- [ ] **STAC database features** using rustac and pgstac
- [ ] **STAC querying workflows** for rasters using H3 and PV labels
- [ ] **Raster processing dataflows** with Xarray and Tensorstore integration

### Phase 3: Multi-Backend & Cloud ✅
- [x] **Cloud deployment**: MotherDuck + Cloudflare R2 + Neon PostgreSQL production setup (WIP; only raw layer)
- [x] **R2 object storage**: Zero egress fees with S3-compatible API for data storage
- [x] **Neon serverless PostgreSQL**: Multi-user DuckLake catalog with connection pooling
- [x] **MotherDuck integration**: Cloud analytical scaling and serving for production workloads (init draft)
- [ ] **Ibis integration**: Multi-SQL backend dataframe library for future expansion (explored but `dbt-ibis` is still not viable for DuckLake and production environments)

### Phase 4: Advanced Analytics and Data Products
- [ ] **VirtualiZarr dataflows** for virtual datasets referencing original assets
- [ ] **Raster-vector integration** Hamilton pipelines
- [ ] **STAC-PV datacubes** using Hamilton + dbt integration
- [ ] **Energy forecasting models** with Hamilton feature engineering

## 🚀 Getting Started

### Prerequisites
```bash
# create conda environment
conda env create -n eo-pv-cv python=3.10
conda activate eo-pv-cv
pip install -r requirements.txt

# Install DuckDB extensions
# Extensions are auto-loaded via dbt configuration
```

### Project Structure
<!-- TODO: Add data layer subdirectories in dataflows, data_loaders to reflect current proj structure -->
```
├── dataflows/                   # Hamilton dataflow modules
│   ├── doi_pv_locations.py     # DOI PV datasets pipeline
│   ├── _doi_pv_helpers_storage.py  # Storage helper functions
│   └── stg_doi_pv_consolidation.py  # Staging consolidation with spatial processing
├── data_loaders/                # Data loading utilities
│   ├── hamilton_modules/        # Reusable Hamilton components
│   ├── utils/                   # Arrow operations, validation
│   ├── doi_manifest.json       # Dataset metadata & file filters
│   └── visualize_hamilton_dag.py  # DAG visualization
├── eo-pv-elt/                   # dbt project root
│   └── models/                  # dbt transformations
│       ├── raw/                 # Raw data loading (Python models)
│       ├── staging/             # Individual dataset processing with Hamilton DAGs
│       ├── prepared/            # Consolidated data with spatial deduplication
│       └── curated/             # Final analytical datasets
├── db/                          # Database files
│   ├── eo_pv_data.duckdb       # Main DuckDB database
│   └── geoparquet/             # Exported GeoParquet files
├── docs/                        # Documentation
│   ├── modern_data_stack.md    # Architecture philosophy
│   ├── DAGs_and_Composable_Data.md  # Hamilton integration
│   └── hamilton_best_practices.ipynb  # Interactive guide
├── utils/                       # Shared utilities
├── run_doi_pv_pipeline.py      # Main pipeline runner
├── eo-pv-elt/dbt_project.yml   # dbt configuration
└── profiles.yml      # Database connections
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
# Required for production cloud deployment
export MOTHERDUCK_TOKEN="your_token"
export R2_ACCESS_KEY_ID="your_key"
export R2_SECRET_ACCESS_KEY="your_secret"
export DUCKLAKE_NAME="eo_pv_lakehouse"
export NEON_PG_CONN="postgresql://user:pass@host/db"

# Development configuration
export REPO_ROOT="/path/to/ice-mELT_ducklake"
export DBT_TARGET="dev"  # or "prod"

# Optional: Spatial processing configuration
export H3_DEDUP_RES="12"
export OVERLAP_THRESHOLD="0.5"
```

### Key Files
- **`profiles.yml`**: Database connections (DuckDB, MotherDuck, PostgreSQL)
- **`eo-pv-elt/dbt_project.yml`**: Model configurations and DuckDB optimizations
- **`.env`**: Environment variables for development and production

## 📈 Data Products

### Current Datasets
- **Global PV installations**: 443,917+ installations from 6 validated DOI sources
  - Mixed geometry types: Points, Polygons, MultiPolygons
  - Standardized to EPSG:4326 (WGS84) coordinate system
  - H3 spatial indexing at configurable resolution depending on image sensor GSD and use case (default: 12)
  - Spatial deduplication using H3-based overlap detection or GeoPandas spatial index and predicates
  - Available in both DuckDB tables (spatial queries) and (Geo)Parquet files (interoperability)
- **Administrative boundaries**: Country/region context via Overture Maps (in development)
- **Geometry statistics**: Area calculations and centroid coordinates for all installations

### Planned Products
- **PV-STAC datacubes**: Satellite imagery aligned with PV locations using Hamilton dataflows sourced from existing STAC catalogs without data duplication
- **Irradiance time series**: Solar potential analysis with NREL/Google Solar API integration
- **Energy forecasting models**: ML-based production estimates using Hamilton feature engineering
- **Global PV database**: Curated, harmonized installation dataset via dbt transformations

## 🤝 Contributing

This is a research project for MS thesis work. The pipeline architecture and methodologies are designed to be reproducible and extensible for similar EO data processing workflows.

## 📄 License

MIT License

---

<!-- **Built with**: dlt, dbt, DuckDB, Apache Iceberg, STAC, H3, and the modern data stack -->
