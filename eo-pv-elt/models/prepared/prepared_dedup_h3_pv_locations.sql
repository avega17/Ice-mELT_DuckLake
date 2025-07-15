/*
H3-based spatial deduplication model for consolidated PV installations.

This model takes the consolidated PV table (with basic exact duplicate removal)
and applies H3-based spatial deduplication to remove overlapping installations
from different datasets.

Architecture:
1. Load consolidated table with H3 indices preserved
2. Group by H3 index for efficient spatial operations  
3. Apply overlap detection within H3 groups only
4. Return deduplicated table with spatial overlaps removed

This approach leverages H3 spatial indexing for performance while maintaining
clean separation between union operations and complex spatial deduplication.

References:
- https://ibis-project.org/reference/expression-geospatial
- https://geopandas.org/en/latest/docs/user_guide/how_to.html
- https://blog.rtwilson.com/how-to-fix-geopandas-drop_duplicates-on-geometry-column-not-getting-rid-of-all-duplicates/
- https://medium.com/@jesse.b.nestler/how-to-find-geometry-intersections-within-the-same-dataset-using-geopandas-59cd1a5f30f9
*/

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['dataset_name'], 'type': 'btree'},
        {'columns': ['h3_index_12'], 'type': 'btree'},
        {'columns': ['area_m2'], 'type': 'btree'}
    ]
) }}

-- Note: H3 indices are stored as UBIGINT (uint64) for optimal performance
-- with DuckDB H3 extension functions like h3_latlng_to_cell()

-- Load required extensions for spatial operations
{% set load_extensions %}
    INSTALL spatial;
    LOAD spatial;
    INSTALL h3 FROM community;
    LOAD h3;
{% endset %}

{% if execute %}
    {% do run_query(load_extensions) %}
{% endif %}

-- Get configuration from environment variables
{% set h3_dedup_res = env_var('H3_DEDUP_RES', '12') | int %}
{% set overlap_threshold = env_var('OVERLAP_THRESHOLD', '0.5') | float %}
{% set h3_column_name = 'h3_index_' ~ h3_dedup_res %}

WITH consolidated_with_h3 AS (
    SELECT *
    FROM {{ ref('stg_pv_consolidated') }}
    WHERE {{ h3_column_name }} IS NOT NULL 
      AND geometry IS NOT NULL
),

-- Group by H3 index and assign priority ranking within each H3 cell
grouped_by_h3 AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY {{ h3_column_name }}
               ORDER BY area_m2 DESC, dataset_name
           ) as priority_rank
    FROM consolidated_with_h3
),

-- Find overlap candidates within same H3 cells only
-- This dramatically reduces the search space for spatial operations
overlap_candidates AS (
    SELECT DISTINCT 
           a.dataset_name as dataset_a,
           a.unified_id as id_a,
           b.dataset_name as dataset_b,
           b.unified_id as id_b,
           a.{{ h3_column_name }} as h3_cell
    FROM grouped_by_h3 a
    JOIN grouped_by_h3 b
      ON a.{{ h3_column_name }} = b.{{ h3_column_name }}  -- Same H3 cell only
     AND a.priority_rank > b.priority_rank  -- Lower priority (smaller area) wins 
     AND a.dataset_name != b.dataset_name   -- Different datasets only
    WHERE ST_GeomFromText(a.geometry) IS NOT NULL
      AND ST_GeomFromText(b.geometry) IS NOT NULL
      -- Check for actual spatial overlap
      AND ST_Overlaps(
            ST_GeomFromText(a.geometry),
            ST_GeomFromText(b.geometry)
          )
      -- Check if overlap exceeds threshold (as fraction of smaller geometry)
      AND ST_Area(ST_Intersection(
            ST_GeomFromText(a.geometry),
            ST_GeomFromText(b.geometry)
          )) / LEAST(
            ST_Area(ST_GeomFromText(a.geometry)),
            ST_Area(ST_GeomFromText(b.geometry))
          ) > {{ overlap_threshold }}
),

-- Remove overlapping installations (keep higher priority ones)
deduplicated AS (
    SELECT g.*
    FROM grouped_by_h3 g
    LEFT JOIN overlap_candidates oc
      ON g.dataset_name = oc.dataset_b
     AND g.unified_id = oc.id_b
    WHERE oc.id_b IS NULL  -- Keep records that don't overlap significantly
)

SELECT
    dataset_name,
    geometry,
    area_m2,
    centroid_lat,
    centroid_lon,
    {{ h3_column_name }},
    processed_at,
    unified_id,
    source_area_m2,
    capacity_mw,
    install_date
FROM deduplicated
ORDER BY dataset_name, unified_id
