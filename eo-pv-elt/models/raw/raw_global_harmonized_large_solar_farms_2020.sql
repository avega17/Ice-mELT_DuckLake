{{
  config(
    materialized='table',
    description='Global Harmonized Large Solar Farms 2020 dataset from Hamilton raw ingestion GeoParquet export'
  )
}}

/*
  Raw model for Global Harmonized Large Solar Farms 2020 dataset.
  Sources from GeoParquet files exported by Hamilton raw ingestion pipeline.
*/

select 
  -- Dataset identification
  'global_harmonized_large_solar_farms_2020' as dataset_name,
  'https://doi.org/10.5281/zenodo.4297676' as doi,
  'zenodo' as repository_type,
  
  -- Spatial data (from GeoParquet)
  geometry,
  centroid_lon,
  centroid_lat,
  area_m2,
  
  -- Ground-truth data (if available)
  installation_date,
  
  -- Processing metadata
  processed_at,
  
  -- dbt metadata
  current_timestamp as dbt_loaded_at,
  'raw_global_harmonized_large_solar_farms_2020' as dbt_model_name

from read_parquet('{{ env_var("REPO_ROOT") }}/db/geoparquet/doi_global_harmonized_large_solar_farms_2020.parquet')

-- Basic data quality filters
where centroid_lon between -180 and 180
  and centroid_lat between -90 and 90
  and area_m2 >= 0
