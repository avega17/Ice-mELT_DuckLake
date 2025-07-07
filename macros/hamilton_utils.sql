-- dbt macros for Hamilton integration utilities

{% macro check_table_exists(schema_name, table_name) %}
  {#
    Check if a table exists in the database
    
    Args:
      schema_name: Schema name to check
      table_name: Table name to check
    
    Returns:
      Boolean indicating if table exists
  #}
  
  {% set check_query %}
    SELECT COUNT(*) as table_count
    FROM information_schema.tables 
    WHERE table_schema = '{{ schema_name }}'
    AND table_name = '{{ table_name }}'
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(check_query) %}
    {% if results %}
      {% set table_exists = results.columns[0].values()[0] > 0 %}
      {% return table_exists %}
    {% endif %}
  {% endif %}
  
  {% return false %}
  
{% endmacro %}


{% macro get_table_row_count(schema_name, table_name) %}
  {#
    Get row count for a table if it exists
    
    Args:
      schema_name: Schema name
      table_name: Table name
    
    Returns:
      Row count or 0 if table doesn't exist
  #}
  
  {% if check_table_exists(schema_name, table_name) %}
    {% set count_query %}
      SELECT COUNT(*) as row_count FROM {{ schema_name }}.{{ table_name }}
    {% endset %}
    
    {% if execute %}
      {% set results = run_query(count_query) %}
      {% if results %}
        {% return results.columns[0].values()[0] %}
      {% endif %}
    {% endif %}
  {% endif %}
  
  {% return 0 %}
  
{% endmacro %}


{% macro log_hamilton_execution(model_name, execution_status, record_count=0, execution_time=null) %}
  {#
    Log Hamilton DAG execution for monitoring and debugging
    
    Args:
      model_name: Name of the dbt model
      execution_status: Status (success, failed, skipped)
      record_count: Number of records processed
      execution_time: Execution time in seconds
  #}
  
  {% set log_message = "Hamilton execution for " ~ model_name ~ ": " ~ execution_status %}
  
  {% if record_count > 0 %}
    {% set log_message = log_message ~ " (" ~ record_count ~ " records)" %}
  {% endif %}
  
  {% if execution_time %}
    {% set log_message = log_message ~ " in " ~ execution_time ~ "s" %}
  {% endif %}
  
  {{ log(log_message, info=true) }}
  
{% endmacro %}


{% macro validate_hamilton_dependencies() %}
  {#
    Validate that Hamilton dependencies are available
    
    Returns:
      Boolean indicating if dependencies are available
  #}
  
  {% if execute %}
    {% set python_check %}
import sys
try:
    import hamilton
    import ibis
    import geopandas
    import duckdb
    print("SUCCESS: All Hamilton dependencies available")
    return True
except ImportError as e:
    print(f"ERROR: Missing Hamilton dependency: {e}")
    return False
    {% endset %}
    
    {% set result = run_query(python_check) %}
    {{ log("Hamilton dependency check completed", info=true) }}
  {% endif %}
  
{% endmacro %}


{% macro get_hamilton_config_defaults() %}
  {#
    Get default configuration for Hamilton DAGs
    
    Returns:
      Dictionary with default configuration
  #}
  
  {% set config = {
    'execution_mode': 'sequential',
    'use_cache': true,
    'max_mb': 300,
    'parallel_downloads': 4,
    'target_sensor': 'sentinel-2-l2a',
    'target_use_case': 'detection_model'
  } %}
  
  {% return config %}
  
{% endmacro %}


{% macro format_spatial_summary(optimization_summary) %}
  {#
    Format spatial optimization summary for logging
    
    Args:
      optimization_summary: Dictionary with optimization results
    
    Returns:
      Formatted string for logging
  #}
  
  {% if optimization_summary %}
    {% set total_records = optimization_summary.get('total_records', 0) %}
    {% set optimizations = optimization_summary.get('optimizations_applied', []) %}
    {% set indexes = optimization_summary.get('spatial_indexes', {}) %}
    
    {% set summary = "Spatial optimization complete: " ~ total_records ~ " records" %}
    
    {% if optimizations %}
      {% set summary = summary ~ ", optimizations: " ~ optimizations | join(", ") %}
    {% endif %}
    
    {% if indexes %}
      {% set summary = summary ~ ", indexes: " ~ indexes.keys() | join(", ") %}
    {% endif %}
    
    {% return summary %}
  {% endif %}
  
  {% return "No spatial optimization summary available" %}
  
{% endmacro %}


{% macro ensure_duckdb_extensions() %}
  {#
    Ensure required DuckDB extensions are installed and loaded
    
    This macro can be called in pre-hooks to ensure spatial extensions are available
  #}
  
  {% if execute %}
    {% set extension_sql %}
      INSTALL spatial;
      LOAD spatial;
      INSTALL h3;
      LOAD h3;
    {% endset %}
    
    {% do run_query(extension_sql) %}
    {{ log("DuckDB spatial extensions loaded", info=true) }}
  {% endif %}
  
{% endmacro %}


{% macro create_spatial_index(table_name, geometry_column='geometry_wkb', index_type='rtree') %}
  {#
    Create spatial index on a geometry column
    
    Args:
      table_name: Full table name (schema.table)
      geometry_column: Name of geometry column
      index_type: Type of spatial index (rtree, hilbert)
  #}
  
  {% if execute %}
    {% if index_type == 'rtree' %}
      {% set index_sql %}
        CREATE INDEX IF NOT EXISTS idx_{{ table_name.split('.')[-1] }}_{{ geometry_column }}_spatial 
        ON {{ table_name }} 
        USING RTREE ({{ geometry_column }})
      {% endset %}
    {% elif index_type == 'hilbert' %}
      {% set index_sql %}
        CREATE INDEX IF NOT EXISTS idx_{{ table_name.split('.')[-1] }}_{{ geometry_column }}_hilbert 
        ON {{ table_name }} (ST_Hilbert({{ geometry_column }}))
      {% endset %}
    {% endif %}
    
    {% if index_sql %}
      {% do run_query(index_sql) %}
      {{ log("Created " ~ index_type ~ " spatial index on " ~ table_name ~ "." ~ geometry_column, info=true) %}
    {% endif %}
  {% endif %}
  
{% endmacro %}
