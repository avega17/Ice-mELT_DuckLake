-- Staging model for Overture Maps administrative boundaries
-- Optimized for DuckDB 1.3.0 SPATIAL_JOIN operator with H3 spatial filtering
--
-- This model:
-- 1. Creates H3 grid from existing PV locations at resolution 6 (coarse, ~36km²)
-- 2. Queries Overture Maps S3 data directly with bbox pre-filtering
-- 3. Uses SPATIAL_JOIN operator for 100x performance improvement
-- 4. Filters to admin levels relevant for PV analysis
-- 5. Materializes as incremental table for efficient updates

{{ config(
    materialized='table',
    unique_key='division_id',
    description='Administrative boundaries from Overture Maps spatially filtered to PV installation areas'
) }}

-- Configuration from STAC manifest and environment
-- see update on public availability changing to max prev 60 days: https://docs.overturemaps.org/blog/2025/09/24/release-notes/
{% set overture_uri = "s3://overturemaps-us-west-2/release/" ~ var('overture_release') ~ "/theme=divisions/type=division_area/*" %}
{% set h3_overture_res = var('h3_overture_res', 6) %}
{# {% set admin_levels = ['dependency', 'country', 'region', 'county', 'locality'] %} #}
{# {% set s3_region = 'us-west-2' %}
{% set s3_endpoint = 'overturemaps-us-west-2.s3.us-west-2.amazonaws.com' %} #}

WITH pv_h3_grid AS (
    -- Create H3 grid from consolidated PV data at coarse resolution
    -- This dramatically reduces the search space for Overture Maps data
    SELECT DISTINCT
        h3_latlng_to_cell(centroid_lat, centroid_lon, {{ h3_overture_res }}) as h3_cell
    FROM {{ ref('stg_pv_consolidated') }}
),

pv_coverage_bbox AS (
    -- Calculate bounding box of all PV H3 cells for Overture data pre-filtering
    -- This enables efficient bbox filtering before expensive spatial operations
    SELECT
        MIN(h3_cell_to_lat(h3_cell)) - 0.25 as min_lat,
        MAX(h3_cell_to_lat(h3_cell)) + 0.25 as max_lat,
        MIN(h3_cell_to_lng(h3_cell)) - 0.25 as min_lng,
        MAX(h3_cell_to_lng(h3_cell)) + 0.25 as max_lng
    FROM pv_h3_grid
),

pv_h3_boundaries AS (
    -- Convert H3 cells to geometry boundaries for SPATIAL_JOIN
    -- Using H3 boundaries as the "smaller" dataset for R-Tree indexing
    SELECT 
        h3_cell,
        h3_cell_to_boundary_wkt(h3_cell) as h3_boundary_wkt
    FROM pv_h3_grid
),

-- SPATIAL_JOIN optimization: Single spatial predicate for DuckDB 1.3.0 SPATIAL_JOIN operator
-- This creates a temporary R-Tree index on pv_h3_boundaries (smaller dataset)
-- and efficiently filters overture_admin_raw to only features that intersect PV areas
spatially_filtered_admin AS (
    SELECT
        om_admin_raw.division_id,
        om_admin_raw.country_iso,
        om_admin_raw.region_code,
        om_admin_raw.division_name,
        om_admin_raw.division_common_names,
        om_admin_raw.admin_level,
        om_admin_raw.admin_class,
        om_admin_raw.geometry,
        om_admin_raw.bbox,
        om_admin_raw.sources,
        om_admin_raw.version,
        -- Add H3 indexing for the admin boundary centroid
        h3_latlng_to_cell(
            (om_admin_raw.bbox.ymin + om_admin_raw.bbox.ymax) / 2,
            (om_admin_raw.bbox.xmin + om_admin_raw.bbox.xmax) / 2,
            {{ h3_overture_res }}
        ) as admin_h3_cell,
        -- Track which PV H3 cells this admin boundary intersects
        ARRAY_AGG(DISTINCT p.h3_cell) as intersecting_pv_h3_cells
    FROM (
        -- Query Overture Maps administrative boundaries from S3
        -- Apply bbox pre-filtering to minimize data transfer
        SELECT
            id as division_id,
            country as country_iso,
            region as region_code,
            names.primary as division_name,
            names.common as division_common_names,
            subtype as admin_level,
            class as admin_class,
            geometry,
            bbox,
            sources,
            version
        -- NOTE: Latest error is "HTTP GET error reading 's3://overturemaps-us-west-2/release/2025-07-23.0/theme=divisions/type=division_area/overturemaps-us-west-2/?encoding-type=url&list-type=2&prefix=release%2F2025-07-23.0%2Ftheme%3Ddivisions%2Ftype%3Ddivision_area%2F'"
        -- where it seems to be incorrectly expanding and encoding the s3 URI
        FROM read_parquet( "{{ overture_uri }}", hive_partitioning=1)
        WHERE 1=1 -- SQL pattern: allows all conditions to use AND consistently for maintainability; easier to comment out or reorder filters
            -- Filter to relevant admin levels from STAC manifest
            AND subtype IN ({{ "'" + admin_levels | join("', '") + "'" }})
            -- Bbox pre-filtering using PV coverage area
            AND bbox.ymin <= (SELECT max_lat FROM pv_coverage_bbox)
            AND bbox.ymax >= (SELECT min_lat FROM pv_coverage_bbox)
            AND bbox.xmin <= (SELECT max_lng FROM pv_coverage_bbox)
            AND bbox.xmax >= (SELECT min_lng FROM pv_coverage_bbox)
    ) as om_admin_raw
    -- CRITICAL: Single spatial predicate to trigger SPATIAL_JOIN operator
    JOIN pv_h3_boundaries p ON ST_Intersects(om_admin_raw.geometry, ST_GeomFromText(p.h3_boundary_wkt))
    GROUP BY
        division_id, country_iso, region_code, division_name,
        division_common_names, admin_level, admin_class,
        om_admin_raw.geometry, bbox, sources, version
),

final AS (
    SELECT
        division_id,
        country_iso,
        region_code,
        division_name,
        division_common_names,
        admin_level,
        admin_class,
        geometry,
        bbox,
        admin_h3_cell,
        intersecting_pv_h3_cells,
        ARRAY_LENGTH(intersecting_pv_h3_cells) as pv_h3_cell_count,
        sources,
        version,
        -- Geometry statistics for analysis
        ST_Area_Spheroid(geometry) as area_m2,
        (bbox.xmin + bbox.xmax) / 2 as centroid_lon,
        (bbox.ymin + bbox.ymax) / 2 as centroid_lat,
        -- Processing metadata
        CURRENT_TIMESTAMP as processed_at,
        '{{ overture_release }}' as overture_release,
        {{ h3_overture_res }} as h3_fetch_res
    FROM spatially_filtered_admin
)

SELECT * FROM final

-- Performance monitoring: Check if SPATIAL_JOIN operator was used
-- Run: EXPLAIN SELECT * FROM {{ this }} LIMIT 1;
-- Should show "SPATIAL_JOIN" in the execution plan
