-- Prepared model for PV installations with administrative boundaries
-- This model efficiently assigns admin boundaries using H3 spatial indexing

{{ config(
    materialized='table',
    description='PV installations with administrative boundaries assigned via efficient H3-based spatial join',
    pre_hook="INSTALL h3 FROM community; LOAD h3; INSTALL spatial; LOAD spatial;"
) }}

-- This model uses H3 indexing for efficient spatial joins with Overture Maps data
-- Instead of joining individual geometries, we use coarsest H3 resolution polygons

with pv_base as (
    select * from {{ ref('prep_pv_datasets_unified') }}
),

-- Create H3 polygon boundaries for coarsest resolution (res 5) to reduce join complexity
pv_h3_polygons as (
    select distinct
        h3_index_res5,
        h3_cell_to_boundary_wkt(h3_index_res5) as h3_polygon_wkt,
        h3_cell_to_lat_lng(h3_index_res5) as h3_center
    from pv_base
    where h3_index_res5 is not null
),

-- Query Overture Maps countries directly from S3 (view approach)
overture_countries as (
    select
        id as division_id,
        country as country_iso,
        names.primary as country_name,
        subtype,
        geometry
    from read_parquet(
        's3://overturemaps-us-west-2/release/2024-11-13.0/theme=admins/type=*/*',
        hive_partitioning=1
    )
    where subtype = 'country'
      and geometry is not null
),

-- Efficient spatial join using H3 polygons instead of individual PV geometries
h3_admin_join as (
    select
        h3p.h3_index_res5,
        oc.division_id,
        oc.country_iso,
        oc.country_name
    from pv_h3_polygons h3p
    left join overture_countries oc
        on ST_Intersects(
            ST_GeomFromText(h3p.h3_polygon_wkt),
            oc.geometry
        )
),

-- Join back to individual PV installations
pv_with_admin as (
    select
        pv.*,
        haj.division_id,
        haj.country_iso as admin_country,
        haj.country_name as admin_country_name,
        -- Placeholder for region and locality (would require additional Overture queries)
        null as admin_region,
        null as admin_locality
    from pv_base pv
    left join h3_admin_join haj
        on pv.h3_index_res5 = haj.h3_index_res5
)

select 
    -- Core PV data
    dataset_name,
    installation_id,
    geometry_wkt,
    area_m2,
    centroid_lon,
    centroid_lat,
    installation_date,
    capacity_mw,
    
    -- H3 spatial indexing
    h3_index_res10,
    h3_index_res7,
    h3_index_res5,
    
    -- Spatial bounds
    bbox_xmin,
    bbox_ymin,
    bbox_xmax,
    bbox_ymax,
    hilbert_order,
    
    -- Administrative boundaries (efficiently assigned)
    division_id,
    admin_country,
    admin_country_name,
    admin_region,
    admin_locality,
    
    -- Metadata
    prepared_at,
    source_system,
    current_timestamp as admin_assigned_at

from pv_with_admin
order by admin_country, dataset_name, installation_id

-- This approach provides several benefits:
-- 1. Dramatically reduced join complexity (H3 cells vs individual geometries)
-- 2. Direct S3 querying without materialization
-- 3. Proper dbt model separation and lineage
-- 4. Efficient spatial indexing leveraging existing H3 infrastructure
-- 5. Scalable to additional admin levels (regions, localities) as needed
