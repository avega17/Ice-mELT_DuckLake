-- dbt macro for running Hamilton DAGs
-- This macro provides integration between dbt and Hamilton dataflows

{% macro run_hamilton_dag(dag_name, config_overrides={}, force_refresh=false) %}
  {#
    Run a Hamilton DAG from within dbt
    
    Args:
      dag_name: Name of the Hamilton DAG to run (e.g., 'doi_pv_locations')
      config_overrides: Dictionary of configuration overrides
      force_refresh: Whether to force refresh even if data exists
    
    Returns:
      Status message about DAG execution
  #}
  
  {% set available_dags = [
    'doi_pv_locations',
    'stg_doi_pv_dedup',
    'stg_doi_pv_geom_stats', 
    'stg_doi_pv_h3_res',
    'stg_doi_pv_std_schema',
    'stg_doi_pv_overture_admin',
    'stg_doi_pv_overture_buildings',
    'stg_doi_pv_overture_land_cover',
    'stg_doi_pv_overture_land_use',
    'stg_doi_pv_overture_unified',
    'stg_doi_pv_spatial_optimization',
    'stg_doi_pv_hive_partitioning',
    'stg_doi_pv_database_export'
  ] %}
  
  {% if dag_name not in available_dags %}
    {{ exceptions.raise_compiler_error("Unknown Hamilton DAG: " ~ dag_name ~ ". Available DAGs: " ~ available_dags | join(", ")) }}
  {% endif %}
  
  {% if execute %}
    {% set python_code %}
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path("{{ project_root }}").resolve()
sys.path.insert(0, str(project_root))

try:
    from dataflows import {{ dag_name }}
    from hamilton import driver
    import json
    
    # Configuration
    config = {
        "execution_mode": "{{ config_overrides.get('execution_mode', 'sequential') }}",
        "use_cache": {{ config_overrides.get('use_cache', true) | lower }},
        "database_path": "{{ config_overrides.get('database_path', 'db/eo_pv_data.duckdb') }}",
        "force_refresh": {{ force_refresh | lower }}
    }
    
    # Add any additional config overrides
    {% for key, value in config_overrides.items() %}
    config["{{ key }}"] = {{ value if value is string else value | tojson }}
    {% endfor %}
    
    print(f"🚀 Running Hamilton DAG: {{ dag_name }}")
    print(f"   Configuration: {json.dumps(config, indent=2)}")
    
    # Build Hamilton driver
    dr = driver.Builder().with_modules({{ dag_name }}).with_config(config).build()
    
    # Determine what to execute based on DAG type
    {% if dag_name == 'doi_pv_locations' %}
    final_vars = ["storage_result"]
    {% elif dag_name.startswith('stg_doi_pv_overture') %}
    final_vars = ["collected_pv_with_overture_context"]
    {% elif dag_name == 'stg_doi_pv_spatial_optimization' %}
    final_vars = ["spatial_optimization_summary"]
    {% elif dag_name == 'stg_doi_pv_hive_partitioning' %}
    final_vars = ["optimized_partitioned_table"]
    {% elif dag_name == 'stg_doi_pv_database_export' %}
    final_vars = ["database_export_summary"]
    {% else %}
    final_vars = ["standardized_geodataframe"]
    {% endif %}
    
    # Execute Hamilton DAG
    results = dr.execute(final_vars)
    
    print(f"✅ Hamilton DAG {{ dag_name }} completed successfully")
    
    # Return summary information
    if isinstance(results, dict):
        for var_name, result in results.items():
            if hasattr(result, '__len__'):
                print(f"   {var_name}: {len(result)} records")
            else:
                print(f"   {var_name}: {type(result).__name__}")
    
    return "SUCCESS"
    
except ImportError as e:
    print(f"❌ Failed to import Hamilton DAG {{ dag_name }}: {e}")
    return "IMPORT_ERROR"
    
except Exception as e:
    print(f"❌ Hamilton DAG {{ dag_name }} failed: {e}")
    return "EXECUTION_ERROR"
    {% endset %}
    
    {% set result = run_query(python_code) %}
    {{ log("Hamilton DAG " ~ dag_name ~ " execution result: " ~ result, info=true) }}
    
  {% endif %}
  
{% endmacro %}


{% macro check_hamilton_data_exists(table_name, schema_name='raw_data') %}
  {#
    Check if Hamilton-generated data exists in the database
    
    Args:
      table_name: Name of the table to check
      schema_name: Schema name (default: raw_data)
    
    Returns:
      Boolean indicating if data exists
  #}
  
  {% set check_query %}
    SELECT COUNT(*) as record_count
    FROM information_schema.tables 
    WHERE table_schema = '{{ schema_name }}'
    AND table_name = '{{ table_name }}'
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(check_query) %}
    {% if results %}
      {% set table_exists = results.columns[0].values()[0] > 0 %}
      {% if table_exists %}
        {% set count_query %}
          SELECT COUNT(*) as record_count FROM {{ schema_name }}.{{ table_name }}
        {% endset %}
        {% set count_results = run_query(count_query) %}
        {% set record_count = count_results.columns[0].values()[0] if count_results else 0 %}
        {{ log("Table " ~ schema_name ~ "." ~ table_name ~ " exists with " ~ record_count ~ " records", info=true) }}
        {% return record_count > 0 %}
      {% else %}
        {{ log("Table " ~ schema_name ~ "." ~ table_name ~ " does not exist", info=true) }}
        {% return false %}
      {% endif %}
    {% endif %}
  {% endif %}
  
  {% return false %}
  
{% endmacro %}


{% macro ensure_hamilton_data(dag_name, table_name, schema_name='raw_data', config_overrides={}) %}
  {#
    Ensure Hamilton data exists, running the DAG if necessary
    
    Args:
      dag_name: Hamilton DAG to run if data doesn't exist
      table_name: Table name to check for existing data
      schema_name: Schema name (default: raw_data)
      config_overrides: Configuration overrides for Hamilton DAG
    
    Returns:
      Status message
  #}
  
  {% set data_exists = check_hamilton_data_exists(table_name, schema_name) %}
  
  {% if not data_exists %}
    {{ log("Data not found for " ~ schema_name ~ "." ~ table_name ~ ", running Hamilton DAG: " ~ dag_name, info=true) }}
    {{ run_hamilton_dag(dag_name, config_overrides) }}
  {% else %}
    {{ log("Data already exists for " ~ schema_name ~ "." ~ table_name ~ ", skipping Hamilton DAG execution", info=true) }}
  {% endif %}
  
{% endmacro %}


{% macro get_hamilton_lineage_info(table_name, schema_name='raw_data') %}
  {#
    Get lineage information for Hamilton-generated data
    
    Args:
      table_name: Name of the table
      schema_name: Schema name (default: raw_data)
    
    Returns:
      Lineage information if available
  #}
  
  {% set lineage_query %}
    SELECT 
      processed_at,
      source_system,
      optimization_version
    FROM {{ schema_name }}.{{ table_name }}
    WHERE processed_at IS NOT NULL
    ORDER BY processed_at DESC
    LIMIT 1
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(lineage_query) %}
    {% if results and results.columns[0].values() %}
      {% set latest_processed = results.columns[0].values()[0] %}
      {% set source_system = results.columns[1].values()[0] if results.columns|length > 1 else 'unknown' %}
      {% set optimization_version = results.columns[2].values()[0] if results.columns|length > 2 else 'unknown' %}
      
      {{ log("Latest data processed at: " ~ latest_processed ~ " by " ~ source_system ~ " (v" ~ optimization_version ~ ")", info=true) }}
      
      {% return {
        'latest_processed': latest_processed,
        'source_system': source_system,
        'optimization_version': optimization_version
      } %}
    {% endif %}
  {% endif %}
  
  {% return {} %}
  
{% endmacro %}
