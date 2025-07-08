{{
  config(
    materialized='table',
    description='USA California USGS PV 2016 dataset from Hamilton raw ingestion GeoParquet export'
  )
}}

/*
  Raw model for USA California USGS PV 2016 dataset.
  Sources from GeoParquet files exported by Hamilton raw ingestion pipeline.
*/

select 
  -- Dataset identification
  'usa_cali_usgs_pv_2016' as dataset_name,
  'https://doi.org/10.5066/F7707ZTX' as doi,
  'usgs' as repository_type,
  
  -- Spatial data (from GeoParquet)
  geometry,
  centroid_lon,
  centroid_lat,
  area_m2,
  
  -- Processing metadata
  processed_at,
  
  -- dbt metadata
  current_timestamp as dbt_loaded_at,
  'raw_usa_cali_usgs_pv_2016' as dbt_model_name

from read_parquet('{{ env_var("REPO_ROOT") }}/db/geoparquet/doi_usa_cali_usgs_pv_2016.parquet')

-- Basic data quality filters
where centroid_lon between -180 and 180
  and centroid_lat between -90 and 90
  and area_m2 >= 0
