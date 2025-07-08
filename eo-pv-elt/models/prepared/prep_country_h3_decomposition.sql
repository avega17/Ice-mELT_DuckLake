-- Prepared model for country geometries with H3 cell decomposition
-- This model creates country-level aggregations with H3 grid cell breakdowns
-- IMPORTANT: Materialized as VIEW only to minimize storage costs for free tier usage

{{ config(
    materialized='view',
    description='Country geometries decomposed into H3 grid cells - view only to minimize storage',
    pre_hook="INSTALL h3 FROM community; LOAD h3;",
    enabled=false 
) }}

-- This model uses DuckDB H3 extension to decompose country geometries into H3 cells
-- It references the Overture countries view and applies H3 decomposition
-- View approach enables H3 spatial queries without persisting large decomposed datasets
-- Note: Will be refactored to Ibis dataframe functions later for multi-backend support

with countries_base as (
    select * from {{ ref('prep_country_pv_statistics') }}
),

countries_with_h3_arrays as (
    select
        division_id,
        country_iso,
        division_name,
        country_pv_count,
        country_pv_area_m2,
        avg_pv_area_m2,
        geometry,
        -- Use correct DuckDB H3 function to decompose country geometry into H3 cells
        h3_polygon_wkt_to_cells(ST_AsText(geometry), 5) as h3_cells_list,
        -- Calculate number of H3 cells per country
        array_length(h3_polygon_wkt_to_cells(ST_AsText(geometry), 5)) as h3_cell_count,
        5 as h3_resolution,
        processed_at
    from countries_base
    where geometry is not null
),

country_h3_base as (
    -- Normalize H3 cells - one row per H3 cell per country
    select
        division_id,
        country_iso,
        division_name,
        country_pv_count,
        country_pv_area_m2,
        avg_pv_area_m2,
        h3_resolution,
        h3_cell_count,
        unnest(h3_cells_list) as h3_cell,
        -- Get H3 cell geometry using correct DuckDB H3 function
        h3_cell_to_boundary_wkt(unnest(h3_cells_list)) as h3_cell_geometry_wkt,
        -- Get H3 cell center point using correct function name
        h3_cell_to_lat_lng(unnest(h3_cells_list)) as h3_cell_center,
        processed_at
    from countries_with_h3_arrays
),

country_h3_enriched as (
    select
        division_id,
        country_iso,
        division_name,
        country_pv_count,
        country_pv_area_m2,
        avg_pv_area_m2,
        h3_resolution,
        h3_cell_count,
        h3_cell,
        h3_cell_geometry_wkt,
        h3_cell_center,

        -- Calculate PV density metrics per country
        case
            when country_pv_count > 0 then country_pv_area_m2 / country_pv_count
            else 0
        end as calculated_avg_pv_area,

        -- Categorize countries by PV adoption
        case
            when country_pv_count = 0 then 'No PV'
            when country_pv_count < 100 then 'Low PV'
            when country_pv_count < 1000 then 'Medium PV'
            when country_pv_count < 10000 then 'High PV'
            else 'Very High PV'
        end as pv_adoption_category,

        -- Calculate area density (PV area per country area)
        country_pv_area_m2 / 1000000.0 as pv_area_km2,

        -- Calculate H3 cell density (PV installations per H3 cell)
        case
            when h3_cell_count > 0 then country_pv_count::float / h3_cell_count::float
            else 0
        end as pv_per_h3_cell,

        processed_at

    from country_h3_base
)

select * from country_h3_enriched
order by country_pv_count desc, country_iso, h3_cell

-- This table enables efficient spatial queries like:
-- 1. Find all H3 cells within a country: WHERE country_iso = 'USA'
-- 2. Aggregate PV installations by H3 cells across countries
-- 3. Perform spatial joins between PV data and country H3 grids using h3_cell
-- 4. Enable multi-resolution analysis using H3 hierarchy
-- 5. Country-level statistics: GROUP BY country_iso
-- 6. H3 cell-level analysis: GROUP BY h3_cell
