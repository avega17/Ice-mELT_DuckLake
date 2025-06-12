-- Prepared model for Overture Maps country geometries
-- This model fetches country boundaries from Overture Maps S3 data
-- IMPORTANT: Materialized as VIEW only to minimize storage costs for free tier usage

{{ config(
    materialized='view',
    description='Country geometries from Overture Maps with PV installation statistics (view only - no storage)'
) }}

-- This model uses DuckDB's ability to query S3 parquet files directly
-- We filter for countries only to reduce data size (~2GB+ if materialized as table)
-- The view approach enables efficient joins without persisting large Overture datasets

with overture_countries as (
    select
        id as division_id,
        country as country_iso,
        names.primary as division_name,
        bbox,
        geometry,
        subtype
    from read_parquet(
        's3://overturemaps-us-west-2/release/2024-11-13.0/theme=admins/type=*/*',
        hive_partitioning=1
    )
    where subtype = 'country'
      and geometry is not null
),

-- Note: PV statistics will be calculated in downstream models
-- This model provides clean Overture Maps country geometries only

clean_countries as (
    select
        division_id,
        country_iso,
        division_name,
        bbox,
        geometry,
        current_timestamp as processed_at
    from overture_countries
)

select * from clean_countries
order by country_iso

-- Note: This model requires DuckDB with httpfs extension for S3 access
-- If S3 access fails, this model will need to be modified to use local data
