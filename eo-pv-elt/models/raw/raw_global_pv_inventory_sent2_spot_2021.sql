{{
  config(
    materialized='table',
    description='Global PV Inventory Sentinel-2 SPOT 2021 dataset from Hamilton raw ingestion GeoParquet export'
  )
}}

/*
  Raw model for Global PV Inventory Sentinel-2 SPOT 2021 dataset.
  Sources from GeoParquet files exported by Hamilton raw ingestion pipeline.
*/

select 
  -- Dataset identification
  'global_pv_inventory_sent2_spot_2021' as dataset_name,
  'https://doi.org/10.5281/zenodo.5171712' as doi,
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
  'raw_global_pv_inventory_sent2_spot_2021' as dbt_model_name

from read_parquet('{{ env_var("REPO_ROOT") }}/db/geoparquet/doi_global_pv_inventory_sent2_spot_2021.parquet')

-- Basic data quality filters
where centroid_lon between -180 and 180
  and centroid_lat between -90 and 90
  and area_m2 >= 0
