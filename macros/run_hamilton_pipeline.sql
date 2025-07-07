{% macro run_hamilton_pipeline(pipeline_name) %}
  {#
    Execute a Hamilton pipeline and update dbt sources automatically.
    
    This macro provides a seamless integration between Hamilton data loading
    pipelines and dbt transformations by:
    1. Executing the specified Hamilton pipeline
    2. Automatically updating dbt source definitions
    3. Providing execution feedback and error handling
    
    Args:
        pipeline_name (str): Name of the Hamilton pipeline to execute
                           Options: 'doi_datasets', 'overture_maps', 'solar_irradiance'
    
    Usage:
        {{ run_hamilton_pipeline('doi_datasets') }}
        
    Example in model pre-hook:
        {{ config(pre_hook=run_hamilton_pipeline('doi_datasets')) }}
  #}
  
  {% if execute %}
    {% set start_time = modules.datetime.datetime.now() %}
    {{ log("Starting Hamilton pipeline: " ~ pipeline_name, info=True) }}
    
    {# Execute the Hamilton pipeline and update sources #}
    {% set result = run_operation('execute_hamilton_pipeline', {'pipeline': pipeline_name}) %}
    
    {% set end_time = modules.datetime.datetime.now() %}
    {% set duration = (end_time - start_time).total_seconds() %}
    
    {{ log("Hamilton pipeline '" ~ pipeline_name ~ "' completed in " ~ duration ~ " seconds", info=True) }}
    
    {# Update dbt sources from Hamilton results #}
    {% set sources_result = run_operation('update_hamilton_sources', {'pipeline_name': pipeline_name}) %}
    
    {{ log("dbt sources updated for pipeline: " ~ pipeline_name, info=True) }}
  {% else %}
    {{ log("Skipping Hamilton pipeline execution (not in execute mode)", info=True) }}
  {% endif %}
{% endmacro %}


{% macro execute_hamilton_pipeline(pipeline) %}
  {#
    Execute a specific Hamilton pipeline using Python subprocess.
    
    This macro handles the actual execution of Hamilton pipelines by calling
    the appropriate Python scripts and handling errors gracefully.
    
    Args:
        pipeline (str): Name of the pipeline to execute
        
    Returns:
        str: Execution result message
  #}
  
  {% if pipeline == 'doi_datasets' %}
    {{ log("Executing DOI datasets Hamilton pipeline...", info=True) }}
    {% set python_script %}
import sys
import os
from pathlib import Path

# Add data_loaders to Python path
project_root = Path.cwd()
data_loaders_path = project_root / "data_loaders"
if data_loaders_path.exists():
    sys.path.insert(0, str(project_root))

try:
    from data_loaders.raw_pv_doi_ingest import run_doi_pipeline
    
    # Execute with production settings
    result = run_doi_pipeline(
        database_path="../db/eo_pv_data.duckdb",
        export_geoparquet=True,
        use_parallel=True,
        use_cache=True,
        force_download=False,
        max_mb=300
    )
    
    print(f"SUCCESS: Hamilton DOI pipeline completed. Processed {len(result)} datasets.")
    
except ImportError as e:
    print(f"ERROR: Cannot import Hamilton pipeline: {e}")
    sys.exit(1)
except Exception as e:
    print(f"ERROR: Hamilton pipeline execution failed: {e}")
    sys.exit(1)
    {% endset %}
    
    {% set result = run_operation('run_python_code', {'code': python_script}) %}
    {{ return(result) }}
    
  {% elif pipeline == 'overture_maps' %}
    {{ log("Overture Maps pipeline not yet implemented", info=True) }}
    {{ return("SKIPPED: Overture Maps pipeline not implemented") }}
    
  {% elif pipeline == 'solar_irradiance' %}
    {{ log("Solar Irradiance pipeline not yet implemented", info=True) }}
    {{ return("SKIPPED: Solar Irradiance pipeline not implemented") }}
    
  {% else %}
    {{ log("Unknown pipeline: " ~ pipeline, info=True) }}
    {{ return("ERROR: Unknown pipeline " ~ pipeline) }}
  {% endif %}
{% endmacro %}


{% macro update_hamilton_sources(pipeline_name) %}
  {#
    Update dbt sources.yml files from Hamilton pipeline results.
    
    This macro uses the Hamilton-dbt bridge to automatically generate
    and update dbt source definitions based on Hamilton pipeline execution.
    
    Args:
        pipeline_name (str): Name of the Hamilton pipeline
        
    Returns:
        str: Update result message
  #}
  
  {% if execute %}
    {{ log("Updating dbt sources from Hamilton pipeline: " ~ pipeline_name, info=True) }}
    
    {% set python_script %}
import sys
from pathlib import Path

# Add data_loaders to Python path
project_root = Path.cwd()
data_loaders_path = project_root / "data_loaders"
if data_loaders_path.exists():
    sys.path.insert(0, str(project_root))

try:
    from data_loaders.utils.hamilton_dbt_bridge import HamiltonDbtBridge
    
    # Initialize bridge and update sources
    bridge = HamiltonDbtBridge()
    result = bridge.update_sources_from_hamilton('{{ pipeline_name }}')
    
    # Extract summary information
    source_info = result['sources'][0]
    table_count = len(source_info['tables'])
    
    print(f"SUCCESS: Updated dbt sources for {pipeline_name}")
    print(f"Generated {table_count} table definitions in sources/{pipeline_name}_sources.yml")
    
    # Validate the generated sources
    is_valid = bridge.validate_sources('{{ pipeline_name }}')
    if is_valid:
        print("Sources file validation passed")
    else:
        print("WARNING: Sources file validation failed")
    
except ImportError as e:
    print(f"ERROR: Cannot import Hamilton-dbt bridge: {e}")
    sys.exit(1)
except Exception as e:
    print(f"ERROR: Failed to update sources: {e}")
    sys.exit(1)
    {% endset %}
    
    {% set result = run_operation('run_python_code', {'code': python_script}) %}
    {{ return(result) }}
  {% else %}
    {{ return("SKIPPED: Not in execute mode") }}
  {% endif %}
{% endmacro %}


{% macro run_python_code(code) %}
  {#
    Execute Python code and return the result.
    
    This is a utility macro for running Python scripts from within dbt.
    It handles subprocess execution and error reporting.
    
    Args:
        code (str): Python code to execute
        
    Returns:
        str: Execution result
  #}
  
  {% if execute %}
    {% set python_executable = "python" %}
    {% set temp_script = "/tmp/dbt_hamilton_" ~ modules.uuid.uuid4().hex ~ ".py" %}
    
    {# Write Python code to temporary file #}
    {% set write_result = modules.subprocess.run([
        "bash", "-c", 
        "cat > " ~ temp_script ~ " << 'EOF'\n" ~ code ~ "\nEOF"
    ], capture_output=True, text=True) %}
    
    {# Execute the Python script #}
    {% set exec_result = modules.subprocess.run([
        python_executable, temp_script
    ], capture_output=True, text=True, cwd=project_root) %}
    
    {# Clean up temporary file #}
    {% set cleanup_result = modules.subprocess.run([
        "rm", "-f", temp_script
    ], capture_output=True, text=True) %}
    
    {# Handle execution results #}
    {% if exec_result.returncode == 0 %}
      {{ log("Python execution successful", info=True) }}
      {% if exec_result.stdout %}
        {{ log("Output: " ~ exec_result.stdout, info=True) }}
      {% endif %}
      {{ return("SUCCESS") }}
    {% else %}
      {{ log("Python execution failed with return code: " ~ exec_result.returncode, info=True) }}
      {% if exec_result.stderr %}
        {{ log("Error: " ~ exec_result.stderr, info=True) }}
      {% endif %}
      {% if exec_result.stdout %}
        {{ log("Output: " ~ exec_result.stdout, info=True) }}
      {% endif %}
      {{ return("FAILED") }}
    {% endif %}
  {% else %}
    {{ return("SKIPPED") }}
  {% endif %}
{% endmacro %}