-- Prepared model for country-level PV statistics
-- This model calculates PV statistics per country using admin-boundary-assigned data

{{ config(
    materialized='view',
    description='Country-level PV installation statistics with Overture Maps country info',
    enabled=false 
) }}

with pv_with_admin as (
    select * from {{ ref('prep_pv_with_admin_boundaries') }}
),

overture_countries as (
    select * from {{ ref('prep_overture_countries') }}
),

pv_country_stats as (
    -- Calculate PV statistics per country
    select
        admin_country,
        count(*) as country_pv_count,
        sum(area_m2) as country_pv_area_m2,
        avg(area_m2) as avg_pv_area_m2,
        min(area_m2) as min_pv_area_m2,
        max(area_m2) as max_pv_area_m2,
        count(distinct dataset_name) as datasets_count,
        sum(case when capacity_mw is not null then capacity_mw else 0 end) as total_capacity_mw
    from pv_with_admin
    where admin_country is not null
    group by admin_country
),

countries_with_pv_stats as (
    select
        oc.division_id,
        oc.country_iso,
        oc.division_name,
        oc.bbox,
        oc.geometry,
        coalesce(pvs.country_pv_count, 0) as country_pv_count,
        coalesce(pvs.country_pv_area_m2, 0.0) as country_pv_area_m2,
        coalesce(pvs.avg_pv_area_m2, 0.0) as avg_pv_area_m2,
        coalesce(pvs.min_pv_area_m2, 0.0) as min_pv_area_m2,
        coalesce(pvs.max_pv_area_m2, 0.0) as max_pv_area_m2,
        coalesce(pvs.datasets_count, 0) as datasets_count,
        coalesce(pvs.total_capacity_mw, 0.0) as total_capacity_mw,
        oc.processed_at
    from overture_countries oc
    left join pv_country_stats pvs
        on oc.country_iso = pvs.admin_country
)

select * from countries_with_pv_stats
order by country_pv_count desc, country_iso

-- This model provides country-level aggregations for:
-- 1. PV installation counts and area statistics
-- 2. Dataset coverage per country
-- 3. Capacity information where available
-- 4. Clean separation between Overture Maps data and PV statistics
