{{
  config(
    materialized='table',
    description='China Medium Resolution PV 2024 dataset from Hamilton raw ingestion GeoParquet export'
  )
}}

/*
  Raw model for China Medium Resolution PV 2024 dataset.
  Sources from GeoParquet files exported by Hamilton raw ingestion pipeline.
*/

select 
  -- Dataset identification
  'chn_med_res_pv_2024' as dataset_name,
  'https://doi.org/10.5281/zenodo.10794575' as doi,
  'zenodo' as repository_type,
  
  -- Spatial data (from GeoParquet)
  geometry,
  centroid_lon,
  centroid_lat,
  area_m2,
  
  -- Ground-truth data (if available)
  capacity_mw,
  
  -- Processing metadata
  processed_at,
  
  -- dbt metadata
  current_timestamp as dbt_loaded_at,
  'raw_chn_med_res_pv_2024' as dbt_model_name

from read_parquet('{{ env_var("REPO_ROOT") }}/db/geoparquet/doi_chn_med_res_pv_2024.parquet')

-- Basic data quality filters
where centroid_lon between -180 and 180
  and centroid_lat between -90 and 90
  and area_m2 >= 0
