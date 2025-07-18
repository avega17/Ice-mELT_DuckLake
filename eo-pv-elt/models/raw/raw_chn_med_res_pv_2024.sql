-- dbt SQL raw model for China Medium Resolution PV 2024 dataset.
-- Converted from Python to SQL using DuckDB's native JSON functions and dbt variables.

-- Set dataset name for this model
{% set dataset_name = 'chn_med_res_pv_2024' %}

-- Determine file path based on target
{% if target.name == 'prod' %}
    {% set source_path = env_var('GEOPARQUET_SOURCE_PATH', 's3://eo-pv-lakehouse/geoparquet') %}
    {% set file_path = source_path ~ '/raw_' ~ dataset_name ~ '.parquet' %}
    {% set geometry_parse = 'ST_GeomFromWKB(geometry)' %}
{% else %}
    {% set source_path = env_var('GEOPARQUET_SOURCE_PATH', 'db/geoparquet') %}
    {% set file_path = source_path ~ '/raw_' ~ dataset_name ~ '.parquet' %}
    {% set geometry_parse = 'geometry' %}
{% endif %}

-- Load DOI manifest JSON and extract metadata for this dataset
WITH doi_manifest_raw AS (
    -- Read JSON file preserving the nested structure
    {% if target.name == 'prod' %}
        SELECT * FROM read_json_objects_auto('{{ env_var("DOI_MANIFEST_PROD", "s3://eo-pv-lakehouse/pv_metadata/doi_manifest.json") }}')
    {% else %}
        SELECT * FROM read_json_objects_auto('{{ env_var("REPO_ROOT", ".") }}/data_loaders/doi_manifest.json')
    {% endif %}
),
doi_manifest_expanded AS (
    -- Use json_each to expand the top-level object into key-value pairs
    SELECT
        je.key as dataset_name,
        je.value as metadata_json
    FROM doi_manifest_raw AS dmr,
         json_each(dmr.json) AS je
),
dataset_metadata AS (
    -- Extract specific fields from the metadata JSON for our target dataset
    SELECT
        dataset_name,
        json_extract_string(metadata_json, '$.doi') as doi,
        json_extract_string(metadata_json, '$.repo') as repo,
        json_extract_string(metadata_json, '$.paper_doi') as paper_doi,
        json_extract_string(metadata_json, '$.paper_title') as paper_title
    FROM doi_manifest_expanded
    WHERE dataset_name = '{{ dataset_name }}'
)

-- Main query: read parquet data and join with metadata
SELECT
    -- dataset name is already included in parquet file
    * EXCLUDE (geometry),
    ST_AsText({{ geometry_parse }}) as geometry_wkt,
    ST_AsWKB({{ geometry_parse }}) as geometry_wkb,
    dm.doi,
    dm.repo,
    dm.paper_doi,
    dm.paper_title,
    CURRENT_TIMESTAMP as dbt_loaded_at
FROM read_parquet('{{ file_path }}')
CROSS JOIN dataset_metadata as dm
WHERE ST_IsValid({{ geometry_parse }}) = true  -- Filter out invalid geometries
