{{
  config(
    materialized='table',
    description='India PV Solar Farms 2022 dataset from Hamilton raw ingestion GeoParquet export'
  )
}}

/*
  Raw model for India PV Solar Farms 2022 dataset.
  Sources from GeoParquet files exported by Hamilton raw ingestion pipeline.
*/

select 
  -- Dataset identification
  'ind_pv_solar_farms_2022' as dataset_name,
  'https://doi.org/10.5281/zenodo.7058502' as doi,
  'zenodo' as repository_type,
  
  -- Spatial data (from GeoParquet)
  geometry,
  centroid_lon,
  centroid_lat,
  area_m2,
  
  -- Processing metadata
  processed_at,
  
  -- dbt metadata
  current_timestamp as dbt_loaded_at,
  'raw_ind_pv_solar_farms_2022' as dbt_model_name

from read_parquet('{{ env_var("REPO_ROOT") }}/db/geoparquet/doi_ind_pv_solar_farms_2022.parquet')

-- Basic data quality filters
where centroid_lon between -180 and 180
  and centroid_lat between -90 and 90
  and area_m2 >= 0
