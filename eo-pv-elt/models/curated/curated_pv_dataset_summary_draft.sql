-- DRAFT: Curated model for PV dataset summary
-- This is a placeholder model to be refined later once we have substantial data to summarize

{{ config(
    materialized='view',
    description='DRAFT: Basic summary of PV datasets - to be refined later',
    enabled=false
) }}

with prepared_data as (
    -- Updated to use working staging model instead of disabled prep model
    select * from {{ ref('stg_pv_consolidated') }}
),

basic_summary as (
    select
        dataset_name,
        count(*) as installation_count,
        avg(area_m2) as avg_area_m2,
        sum(area_m2) as total_area_m2,
        -- Note: H3 indexing not available in staging, will be added in prepared layer
        -- count(distinct h3_index_res7) as unique_h3_cells_res7,
        min(processed_at) as first_processed,
        max(processed_at) as last_processed
    from prepared_data
    group by dataset_name
)

select 
    *,
    'DRAFT - TO BE REFINED' as status
from basic_summary
order by installation_count desc

-- TODO: Add more sophisticated metrics once we have:
-- 1. Administrative boundary data
-- 2. Land cover/land use data  
-- 3. Quality scoring framework
-- 4. Research priority classification
