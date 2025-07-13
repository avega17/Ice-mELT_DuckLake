{% macro get_ducklake_connection_string() %}
  {#- Return the appropriate DuckLake connection string based on target -#}
  {% if target.name == 'prod' %}
    {{ env_var('DUCKLAKE_CONNECTION_STRING_PROD') }}
  {% else %}
    {{ env_var('DUCKLAKE_CONNECTION_STRING_DEV') }}
  {% endif %}
{% endmacro %}

{% macro get_geoparquet_source_path() %}
  {#- Return the appropriate GeoParquet source path based on target -#}
  {% if target.name == 'prod' %}
    {{ env_var('GEOPARQUET_SOURCE_PATH_PROD') }}
  {% else %}
    {{ env_var('GEOPARQUET_SOURCE_PATH_DEV') }}
  {% endif %}
{% endmacro %}
