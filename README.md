# Ice-m<sup>3</sup>ELT DuckLake

Modern data lakehouse pipeline for Earth Observation (EO) photovoltaic (PV) solar panel segmentation and energy forecasting, built on open-source and cloud-native technologies. This work is developed as part of the methodology for my Computer Science master's thesis at [UPR, Río Piedras](https://natsci.uprrp.edu/ccom/masters/). 

## 🎯 Project Overview

This project implements a comprehensive data pipeline for processing and analyzing global photovoltaic installation datasets, combining:
- **Vector data**: PV installation polygon and point coordinates from multiple DOI open-access datasets
- **Raster data**: Satellite multispectral imagery and irradiance data via public STAC catalogs
- **Analytical processing**: Spatial indexing, administrative boundary enrichment, and energy forecasting

## 🏗️ Architecture

<div align="center">
<img src="figures/ice-mELT_ducklake_draft.png" alt="ice-mELT DuckLake Architecture" width="40%">
</div>

### Project Name Explained

**ice-mELT DuckLake** reflects our modern data architecture approach:

- **Ice**: Leverages *Iceberg-inspired* open table formats and **Icechunk** [tensor storage engine](https://earthmover.io/blog/icechunk-1-0-production-grade-cloud-native-array-storage-is-here)
- **m<sup>3</sup>ELT**: **Modern** and **Multi-Modal** data stack with **Extract-Load-Transform** pipelines ([dbt methodology](https://www.getdbt.com/blog/extract-load-transform))
- **DuckLake**: Data lakehouse architecture using the new **DuckLake** [open table and lakehouse format](https://ducklake.select/faq#what-is-ducklake) for SQL-based metadata management (see [the file explosion problem](https://www.starburst.io/blog/apache-iceberg-files/))

### Core Technologies and Spatio-Temporal Data

**Storage & Formats**
- **Local filesystem** and **S3-compatible buckets** for data storage
- **Zarr** data format with **Icechunk** tensor storage engine for rasters
- **VirtualiZarr** for virtual datasets referencing original imagery from STAC assets
- **Apache (Geo)Parquet** for lakehouse tables and vector data where GeoParquet adds native support for spatial geometry types like points, lines, and polygons
- **Apache (Geo)Arrow**, an [in-memory columnar format](https://arrow.apache.org/docs/dev/format/Intro.html) that enables [zero-copy shared memory](https://arrow.apache.org/docs/dev/format/Columnar.html) and [RPC-based data movement](https://arrow.apache.org/docs/dev/format/Flight.html) between processes and networked services. The [GeoArrow specification](https://geoarrow.org/format.html#motivation) simply codifies conventions for "representing spatial data in Apache Arrow formats (e.g., C Data Interface, Serialized IPC) and implementations (e.g., PyArrow, Arrow C++, arrow-rs)".
- **COG/GeoTIFF/NetCDF** for underlying imagery and raster assets accessed via Zarr stores


**Transform & Processing**
- [Apache Hamilton DAGs](https://github.com/apache/hamilton) for composable, self-documenting dataflows and pipelines
- [dbt core](https://github.com/dbt-labs/dbt-core?tab=readme-ov-file) and [dbt-duckdb adapter](https://duckdb.org/2025/04/04/dbt-duckdb.html) for Python **AND** SQL data model development, lineage, docs, and testing
- [Ibis](https://ibis-project.org/why) Python dataframe API compiles and executes on any (supported) SQL query engine
- [Xarray](https://docs.xarray.dev/en/latest/getting-started-guide/why-xarray.html) for labeled multi-dimensional arrays and API for accessing Zarr stores
- [Google Tensorstore](https://google.github.io/tensorstore/) for performant reading/writing of large ND-arrays

**Query Engines**
- **Unified OLTP Catalog**: Unified [Neon PostgreSQL](https://neon.com/docs/get-started-with-neon/why-neon) catalog for DuckLake metadata
- **OLAP Embedded Query Engine**: DuckDB (local development) + MotherDuck (cloud scaling) 
- **Production**: Neon PostgreSQL with connection pooling and multi-user concurrency
- **Development**: Neon Local container proxy with ephemeral branching from production snapshots for safe experimentation and testing changes 

**Data Lakehouse & Catalogs**
- **DuckLake** - Open lakehouse format with SQL catalog for all metadata management
- **Apache Iceberg** open table format for ACID transactions (can be complementary to DuckLake and definitely has wider adoption)
- **STAC** (SpatioTemporal Asset Catalog) for satellite imagery metadata and asset discovery
- [H3 spatial indexing](https://h3geo.org/docs/highlights/indexing) for efficient spatial aggregations and operations while limiting data volume to hierarchical Areas-of-Interest

**Data Sources**
- DOI datasets via [datahugger](https://github.com/J535D165/datahugger/tree/main)
- [STAC assets](data_loaders/stac_manifest.json) for satellite imagery
- [Overture Maps](https://overturemaps.org/blog/2025/overture-maps-foundation-making-open-data-the-winning-choice/) for admin boundaries, building footprints, land cover
- [Google Solar API](https://developers.google.com/maps/documentation/solar/data-layers) and [NREL NSRDB](https://nsrdb.nrel.gov/about/what-is-the-nsrdb) for irradiance data

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
- **Unified Development & Production Architecture**
  - **Single DuckLake catalog**: PostgreSQL-based metadata for both dev and prod
  - **Neon Local ephemeral branches**: Safe development with automatic cleanup
  - **Hybrid compute**: Local DuckDB (dev) + MotherDuck (prod) with intelligent query routing
  - **Cloud storage**: Cloudflare R2 for zero-egress data access
  - **Environment parity**: Identical dbt models work across dev/local and prod/cloud
- **Modern data stack integration**
  - Hamilton DAGs for composable, self-documenting pipelines
  - dbt project structure with raw/staging/prepared/curated layers
  - DuckDB + dbt-duckdb integration with spatial extensions
  - Apache Arrow for zero-copy data exchange
  - DuckLake for SQL-based lakehouse metadata management
- **Development environment** with conda, extensions, and comprehensive testing
  - [Future] Migrate dependencies to [uv, a python package manager](https://www.datacamp.com/tutorial/python-uv) [implemented in rust](https://www.saaspegasus.com/guides/uv-deep-dive/#why-use-uv) (see [pros and cons](https://www.bitecode.dev/p/a-year-of-uv-pros-cons-and-should))

### 🔄 In Progress
- dbt Python + SQL staging models for data fusion (Overture Maps themes, ERA5, Solar Irradiance, etc)
- STAC catalog integration and ingestion for satellite imagery
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
conda env create -n eo-pv-cv python>=3.11
conda activate eo-pv-cv
pip install -r requirements.txt

# Install DuckDB extensions
# Extensions are auto-loaded via dbt configuration
```

### Project Structure
<!-- TODO: Add data layer subdirectories in dataflows, data_loaders to reflect current proj structure -->
```
├── dataflows/                   # Hamilton dataflow modules
│   ├── doi_pv_locations.py     # DOI PV datasets ingestion
│   ├── _doi_pv_helpers_storage.py  # Storage helper functions
│   └── stg_doi_pv_consolidation.py  # Testing staging consolidation transforms for dbt python models
├── data_loaders/                
│   ├── hamilton_modules/        # Reusable Hamilton components
│   ├── utils/                   # Arrow operations, validation
│   ├── doi_manifest.json       # Dataset metadata & file filters
│   └── visualize_hamilton_dag.py  
├── eo-pv-elt/                   # dbt project dir
│   └── dbt_project.yml          # dbt configuration
│   └── models/                  # dbt transformations
│       ├── raw/                 # Raw data loading (Python models)
│       ├── staging/             # Individual dataset processing with Hamilton DAGs
│       ├── prepared/            # Consolidated data with spatial deduplication
│       └── curated/             # Final analytical datasets
├── db/                          
│   └── geoparquet/             # Exported GeoParquet files
├── docs/                        
│   ├── modern_data_stack.md    # Architecture philosophy
│   ├── DAGs_and_Composable_Data.md  # Hamilton integration
│   └── hamilton_best_practices.ipynb  # WIP guide with DAG visualizations
├── utils/                       # Shared utilities mostly initial work left to be refactored
└── profiles.yml      # Database connections and configurations for dbt
```

## 🔧 Configuration

### Unified Architecture Strategy

Our architecture provides seamless development-to-production workflows with cost-effective scaling:

**Development Environment (Neon Local + Local DuckDB)**
- **Neon Local**: Ephemeral or Persisting DB branches from production schema and data (free)
- **DuckDB**: Local analytical processing with full spatial extensions (free)
- **Local storage**: Development data materialization for fast iteration (free)
- **Docker**: Containerized Neon Local proxy with automatic branch cleanup (free)

**Production Environment (Neon Cloud + MotherDuck + R2)**
- **Neon PostgreSQL**: Serverless catalog with connection pooling (0.5GB free tier)
- **MotherDuck**: Hybrid query processing with intelligent local/cloud routing (10GB free tier)
- **Cloudflare R2**: Zero-egress object storage for data lakehouse (10GB free tier)
- **Same dbt models**: Identical transformations beyond environment configuration

**Key Benefits**:
- **Environment parity**: Same catalog structure, same models, same data
- **Safe experimentation**: Ephemeral branches prevent production impact
- **Cost optimization**: Free tiers for research, pay-as-you-scale for production
- **Hybrid processing**: Intelligent query routing between local and cloud compute for some production workloads

**Cost Projection**: Some academic research workloads operate entirely within free tiers (~$0/month), production scales cost-effectively (~$10's/month for certain TB-scale datasets).

### Environment Variables
```bash
# Unified DuckLake catalog (both dev and prod)
export DUCKLAKE_CONNECTION_STRING="ducklake:postgres:host=localhost port=5432 dbname=neondb user=neon password=npg"  # Dev (Neon Local)
export DUCKLAKE_CONNECTION_STRING_PROD="ducklake:postgres:host=ep-broad-rain-a4tdwnxn-pooler.us-east-1.aws.neon.tech..."  # Prod

# Neon Local ephemeral branches (development)
export NEON_API_KEY="napi_..."
export NEON_PROJECT_ID="your_project_id"

# Cloud storage and compute (production)
export MOTHERDUCK_TOKEN="your_token"
export R2_ACCESS_KEY_ID="your_key"
export R2_SECRET_ACCESS_KEY="your_secret"
export R2_BUCKET_NAME="your-eo-bucket"

# Environment targeting
export DBT_TARGET="dev"  # or "prod"
export REPO_ROOT="/path/to/ice-mELT_ducklake"

# Spatial processing configuration
export H3_DEDUP_RES="12" # default used for detecting spatially overlapping, duplicate geometries
export OVERLAP_THRESHOLD="0.5"
```

### Key Files
- **`profiles.yml`**: Database connections (DuckDB, MotherDuck, PostgreSQL)
- **`eo-pv-elt/dbt_project.yml`**: Model configurations and DuckDB optimizations
- **`.env`**: Environment variables for development and production

## 📈 Data Products

### Current Datasets
- **Global PV installations**: 100's of thousands of PV installations from validated, published DOI sources
  - Mixed geometry types: Points, Polygons, MultiPolygons
  - Standardized to EPSG:4326 (WGS84) coordinate system
  - H3 spatial indexing at configurable resolution depending on image sensor GSD and use case (default: 12)
  - Spatial deduplication using H3-based overlap detection or GeoPandas spatial index and predicates
  - Will be available in both DuckDB tables (spatial queries) and (Geo)Parquet files (interoperability)
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
