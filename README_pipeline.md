# EO PV Data Ingestion Pipeline with dlt

This directory contains a data pipeline built with [dlt (data load tool)](https://dlthub.com/) for ingesting geospatial vector data of PV Solar Panel locations from DOI-linked scientific datasets.

## Overview

The pipeline fetches open access scientific datasets using DOI URIs via the [datahugger library](https://github.com/J535D165/datahugger) and loads them into DuckDB for analysis. It's designed to work with both local DuckDB and cloud-scaled MotherDuck.

## Architecture

```
DOI Datasets (Zenodo, Figshare, GitHub, ScienceBase)
    ↓ (datahugger, sciencebasepy, custom fetchers)
Raw Data Files (GeoJSON, Shapefile, GeoPackage, etc.)
    ↓ (dlt pipeline)
DuckDB Database (local)
    ↓ (future: MotherDuck scaling)
Cloud Database (MotherDuck)
    ↓ (dbt models)
Curated Data Products
```

## Setup Instructions

### 1. Environment Setup

Activate your conda environment:
```bash
conda activate eo-pv-cv
```

### 2. Storage Optimization Note

**Important**: This pipeline is optimized for free tier usage during research prototyping:
- **Overture Maps data**: Materialized as views only (no storage cost)
- **H3 decomposition**: Computed on-demand (no storage cost)
- **PV datasets**: Materialized as tables (small footprint ~100MB)
- **Total storage**: <500MB vs. >5GB if Overture data were materialized

See `storage_optimization_strategy.md` for details.

### 2. Verify Installation

Test that all dependencies are available:
```bash
python test_pipeline.py
```

### 3. Pipeline Configuration

The pipeline is configured via `.dlt/config.toml`:
- **Database**: Local DuckDB file (`eo_pv_data.duckdb`)
- **Format**: Parquet for optimal geospatial performance
- **Logging**: INFO level for debugging

### 4. Available Datasets

Currently configured vector datasets (no image preprocessing required):

| Dataset | DOI | Repository | Format | Geometry | Labels | Description |
|---------|-----|------------|--------|----------|--------|-------------|
| `bradbury_2016_california` | [figshare.3385780.v4](https://doi.org/10.6084/m9.figshare.3385780.v4) | Figshare | GeoJSON | Polygon | 19,433 | USA USGS aerial imagery, 4 CA cities |
| `stowell_2020_uk` | [zenodo.4059881](https://zenodo.org/records/4059881) | Zenodo | GeoJSON | Point/Polygon | 265,418 | UK crowdsourced PV installations |
| `kruitwagen_2021_global` | [zenodo.5005867](https://doi.org/10.5281/zenodo.5005867) | Zenodo | GeoJSON | Polygon | 68,661 | Global PV inventory from Sentinel-2 |
| `global_harmonized_large_solar_farms_2020` | [figshare.11310269.v6](https://doi.org/10.6084/m9.figshare.11310269.v6) | Figshare | GeoPackage | Polygon | 35,272 | Harmonised global wind and solar farms |
| `chn_med_res_pv_2024` | [github.com/qingfengxitu/ChinaPV](https://github.com/qingfengxitu/ChinaPV/tree/main) | GitHub | Shapefile | Polygon | 3,356 | China PV installations 2015-2020 |
| `ind_pv_solar_farms_2022` | [github.com/microsoft/solar-farms-mapping](https://raw.githubusercontent.com/microsoft/solar-farms-mapping/refs/heads/main/data/solar_farms_india_2021_merged_simplified.geojson) | GitHub | GeoJSON | MultiPolygon | 117 | India solar farm locations |
| `global_pv_inventory_sent2_2024` | [github.com/yzyly1992/GloSoFarID](https://github.com/yzyly1992/GloSoFarID/tree/main/data_coordinates) | GitHub | JSON | Point | 6,793 | GloSoFarID global solar farm coordinates |

## Usage

### Basic Usage

Run the pipeline with sample datasets:
```bash
python doi_dataset_pipeline.py
```

### Advanced Usage

Load specific datasets:
```python
from doi_dataset_pipeline import load_doi_datasets

# Load specific datasets
load_doi_datasets(
    datasets=["bradbury_2016_california", "stowell_2020_uk"],
    max_mb=100,
    force=False,
    dataset_name="my_pv_data"
)
```

Load all available datasets:
```python
from doi_dataset_pipeline import load_all_datasets
load_all_datasets()
```

### Query the Data

After running the pipeline, query your data:
```python
import duckdb

# Connect to the database
conn = duckdb.connect('eo_pv_data.duckdb')

# List tables
conn.execute("SHOW TABLES").fetchall()

# Query dataset metadata
conn.execute("SELECT * FROM doi_datasets LIMIT 5").fetchall()
```

## Next Steps

### 1. MotherDuck Integration

To scale to cloud compute:

1. Sign up for [MotherDuck](https://motherduck.com/)
2. Get your access token
3. Update `.dlt/config.toml`:
   ```toml
   [destination.motherduck]
   credentials="md:///your_database_name?motherduck_token=your_token"
   ```
4. Run pipeline with MotherDuck destination

### 2. dbt Model Generation

Generate initial dbt models:
```bash
conda activate eo-pv-cv
dlt dbt generate
```

This will create:
- `models/` directory with SQL models
- `dbt_project.yml` configuration
- Initial staging, prepared, and curated models

### 3. Data Quality and Testing

Add data quality checks:
- Schema validation
- Geometry validation
- Duplicate detection
- Coordinate system verification

## File Structure

```
ice-mELT_ducklake/
├── doi_dataset_pipeline.py      # Main pipeline script
├── test_pipeline.py             # Test suite
├── requirements_dlt.txt         # Pipeline dependencies
├── .dlt/
│   ├── config.toml             # dlt configuration
│   └── secrets.toml            # API keys and secrets
├── utils/
│   └── fetch_and_preprocess.py # Utility functions
└── eo_pv_data.duckdb           # Generated database (after running)
```

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure conda environment is activated
2. **Download failures**: Check internet connection and DOI URLs
3. **Memory issues**: Reduce `max_mb` parameter
4. **Geometry errors**: Verify CRS and geometry validity

### Debug Mode

Enable verbose logging:
```toml
# In .dlt/config.toml
[runtime]
log_level="DEBUG"
```

## Contributing

To add new datasets:

1. Add metadata to `DATASET_METADATA` in `doi_dataset_pipeline.py`
2. Ensure the repository type is supported (`figshare`, `zenodo`, `github`, `sciencebase`)
3. Test with small file size limits first
4. Update this README with dataset information
