-- DuckDB spatial utility macros leveraging dbt-duckdb plugins

{% macro h3_polygon_wkt_to_cells(geometry_column, resolution=8) %}
  -- Convert WKT polygon to H3 cells using DuckDB H3 extension
  -- Uses h3_polygon_wkt_to_cells function from DuckDB H3 extension
  h3_polygon_wkt_to_cells({{ geometry_column }}, {{ resolution }})
{% endmacro %}

{% macro calculate_geometry_area_m2(geometry_column) %}
  -- Calculate area in square meters using DuckDB spatial extension
  -- Converts to Web Mercator (EPSG:3857) for accurate area calculation
  ST_Area(ST_Transform(ST_GeomFromText({{ geometry_column }}), 'EPSG:4326', 'EPSG:3857'))
{% endmacro %}

{% macro calculate_geometry_centroid(geometry_column) %}
  -- Calculate centroid coordinates using DuckDB spatial extension
  ST_Centroid(ST_GeomFromText({{ geometry_column }}))
{% endmacro %}

{% macro extract_centroid_coords(geometry_column) %}
  -- Extract longitude and latitude from centroid
  ST_X(ST_Centroid(ST_GeomFromText({{ geometry_column }}))) as centroid_lon,
  ST_Y(ST_Centroid(ST_GeomFromText({{ geometry_column }}))) as centroid_lat
{% endmacro %}

{% macro spatial_join_within_distance(left_table, right_table, left_geom, right_geom, distance_m=1000) %}
  -- Spatial join using DuckDB spatial extension with distance buffer
  -- Uses ST_DWithin for efficient spatial joins
  SELECT *
  FROM {{ left_table }} l
  JOIN {{ right_table }} r
  ON ST_DWithin(
    ST_GeomFromText(l.{{ left_geom }}),
    ST_GeomFromText(r.{{ right_geom }}),
    {{ distance_m }}
  )
{% endmacro %}

{% macro validate_geometries(geometry_column) %}
  -- Validate geometries using DuckDB spatial extension
  ST_IsValid(ST_GeomFromText({{ geometry_column }}))
{% endmacro %}

{% macro create_spatial_index(table_name, geometry_column) %}
  -- Create spatial index for better performance
  -- Note: This is a placeholder - DuckDB handles spatial indexing automatically
  -- but we can add explicit hints for query optimization
  {% if target.name == 'prod' %}
    -- In production, we might want to create explicit spatial indexes
    CREATE INDEX IF NOT EXISTS idx_{{ table_name }}_{{ geometry_column }}_spatial
    ON {{ table_name }} USING GIST (ST_GeomFromText({{ geometry_column }}))
  {% endif %}
{% endmacro %}

{% macro motherduck_optimize_table(table_name) %}
  -- Optimize table for MotherDuck cloud performance
  {% if 'motherduck' in target.get('plugins', []) %}
    OPTIMIZE {{ table_name }}
  {% endif %}
{% endmacro %}

{% macro iceberg_create_table_as(table_name, select_sql, partition_cols=[]) %}
  -- Create Iceberg table using dbt-duckdb iceberg plugin
  {% if 'iceberg' in target.get('plugins', []) %}
    CREATE TABLE {{ table_name }}
    {% if partition_cols %}
    PARTITIONED BY ({{ partition_cols | join(', ') }})
    {% endif %}
    AS {{ select_sql }}
  {% else %}
    -- Fallback to regular table
    CREATE TABLE {{ table_name }} AS {{ select_sql }}
  {% endif %}
{% endmacro %}
