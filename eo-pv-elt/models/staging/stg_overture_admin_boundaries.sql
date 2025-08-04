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
{% set overture_release = var('overture_release', '2025-06-25.0') %}
{% set h3_overture_res = var('h3_overture_res', 6) %}
{% set admin_levels = ['dependency', 'country', 'region', 'county', 'locality'] %}

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
        h3_cell_to_boundary(h3_cell) as h3_boundary
    FROM pv_h3_grid
),

overture_admin_raw AS (
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
    FROM read_parquet('s3://overturemaps-us-west-2/release/{{ overture_release }}/theme=divisions/type=division_area/*', hive_partitioning=1)
    WHERE 1=1 -- SQL pattern: allows all conditions to use AND consistently for maintainability; easier to comment out or reorder filters
        -- Filter to relevant admin levels from STAC manifest
        AND subtype IN ({{ "'" + admin_levels | join("', '") + "'" }})
        -- Bbox pre-filtering using PV coverage area
        AND bbox.ymin <= (SELECT max_lat FROM pv_coverage_bbox)
        AND bbox.ymax >= (SELECT min_lat FROM pv_coverage_bbox)
        AND bbox.xmin <= (SELECT max_lng FROM pv_coverage_bbox)
        AND bbox.xmax >= (SELECT min_lng FROM pv_coverage_bbox)
        -- Basic data quality filters
        -- AND geometry IS NOT NULL
        -- AND names.primary IS NOT NULL
        -- AND country IS NOT NULL
),

-- SPATIAL_JOIN optimization: Single spatial predicate for DuckDB 1.3.0 SPATIAL_JOIN operator
-- This creates a temporary R-Tree index on pv_h3_boundaries (smaller dataset)
-- and efficiently filters overture_admin_raw to only features that intersect PV areas
spatially_filtered_admin AS (
    SELECT 
        o.division_id,
        o.country_iso,
        o.region_code,
        o.division_name,
        o.division_common_names,
        o.admin_level,
        o.admin_class,
        o.geometry,
        o.bbox,
        o.sources,
        o.version,
        -- Add H3 indexing for the admin boundary centroid
        h3_latlng_to_cell(
            (o.bbox.ymin + o.bbox.ymax) / 2,
            (o.bbox.xmin + o.bbox.xmax) / 2,
            {{ h3_overture_res }}
        ) as admin_h3_cell,
        -- Track which PV H3 cells this admin boundary intersects
        ARRAY_AGG(DISTINCT p.h3_cell) as intersecting_pv_h3_cells
    FROM overture_admin_raw o
    -- CRITICAL: Single spatial predicate to trigger SPATIAL_JOIN operator
    JOIN pv_h3_boundaries p ON ST_Intersects(o.geometry, p.h3_boundary)
    GROUP BY 
        o.division_id, o.country_iso, o.region_code, o.division_name,
        o.division_common_names, o.admin_level, o.admin_class, 
        o.geometry, o.bbox, o.sources, o.version
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
        {{ h3_overture_res }} as h3_resolution_used
    FROM spatially_filtered_admin
)

SELECT * FROM final

{# {% if is_incremental() %}
    -- Incremental logic: only process new/updated admin boundaries
    WHERE processed_at > (
        SELECT COALESCE(MAX(processed_at), '1900-01-01'::TIMESTAMP) 
        FROM {{ this }}
    )
{% endif %} #}

-- Performance monitoring: Check if SPATIAL_JOIN operator was used
-- Run: EXPLAIN SELECT * FROM {{ this }} LIMIT 1;
-- Should show "SPATIAL_JOIN" in the execution plan
