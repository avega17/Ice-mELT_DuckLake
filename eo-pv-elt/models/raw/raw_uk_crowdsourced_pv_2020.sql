{{
  config(
    materialized='table',
    description='UK Crowdsourced PV 2020 dataset from Hamilton raw ingestion GeoParquet export'
  )
}}

/*
  Raw model for UK Crowdsourced PV 2020 dataset.

  Sources from GeoParquet files exported by Hamilton raw ingestion pipeline.
  This approach allows the model to work in both local and cloud environments:

  Local: Reads from db/geoparquet/uk_crowdsourced_pv_2020.parquet
  Cloud: Reads from S3/R2 GeoParquet exports

  The Hamilton raw ingestion pipeline (dataflows/raw/doi_pv_locations.py)
  exports individual datasets to GeoParquet preserving original schemas.
*/

select
  -- Dataset identification
  'uk_crowdsourced_pv_2020' as dataset_name,
  'https://doi.org/10.5281/zenodo.4446968' as doi,
  'zenodo' as repository_type,

  -- Spatial data (from GeoParquet)
  geometry,
  centroid_lon,
  centroid_lat,
  area_m2,

  -- Ground-truth data preserved from original dataset
  installation_date,
  capacity_mw,
  building_type,

  -- Processing metadata
  processed_at,

  -- dbt metadata
  current_timestamp as dbt_loaded_at,
  'raw_uk_crowdsourced_pv_2020' as dbt_model_name

from read_parquet('{{ env_var("REPO_ROOT") }}/db/geoparquet/doi_uk_crowdsourced_pv_2020.parquet')
