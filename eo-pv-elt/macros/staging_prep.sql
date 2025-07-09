{% macro prepare_staging_tables() %}
  {#- 
    Simple macro to prepare staging tables by running Python script.
    This can be used as a pre-hook in staging models.
  -#}
  
  {% if execute %}
    {#- Run the staging preparation script with current target -#}
    {% set prep_command = 'python scripts/build_with_staging.py ' ~ target.name %}
    
    {{ log("🔄 Preparing staging tables for target: " ~ target.name, info=True) }}
    
    {#- Execute the preparation script -#}
    {% set result = run_query("SELECT system('" ~ prep_command ~ "')") %}
    
    {{ log("✅ Staging preparation completed", info=True) }}
  {% endif %}
{% endmacro %}
