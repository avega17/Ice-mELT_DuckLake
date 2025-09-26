"""
Data Loading Pipelines using Hamilton Framework

This package contains Hamilton-based data loading pipelines for the EO PV project.
Each pipeline handles a specific data source and follows Hamilton best practices
for dependency management, function naming, and lineage tracking.

Available Pipelines:
- overture_maps_loader: Global administrative boundaries and building footprints (planned)
- solar_irradiance_loader: NREL and Google Solar API irradiance data (planned)

Architecture:
- Each pipeline is a self-contained Hamilton module with proper dependency injection
- All pipelines output to DuckDB with standardized schemas in the raw_data schema
- Integration with dbt is handled via the hamilton_dbt_bridge utility
- Pipelines support both parallel and sequential execution modes

Usage:
    from ingest import run_doi_pipeline
    
    # Execute DOI datasets pipeline
    result = run_doi_pipeline(
        database_path="../db/eo_pv_data.duckdb",
        use_parallel=True,
        use_cache=True
    )
    
    # Result is Dict[str, str] mapping dataset names to table information
    print(f"Loaded {len(result)} datasets")

Integration with dbt:
    # Manual execution
    dbt run-operation execute_hamilton_pipeline --args '{pipeline: doi_datasets}'
    
    # Automatic execution via pre-hooks
    {{ config(pre_hook=run_hamilton_pipeline('doi_datasets')) }}

Directory Structure:
    ingest/
    ├── __init__.py                    # This file
    ├── overture_maps_loader.py       # Overture Maps pipeline (planned)
    ├── solar_irradiance_loader.py    # Solar irradiance pipeline (planned)
    └── utils/
        ├── __init__.py
        ├── ingestion_utils.py         # Shared utilities
        └── hamilton_dbt_bridge.py     # dbt integration bridge

Best Practices:
1. Each pipeline follows Hamilton dependency management principles
2. Function names describe outputs (nouns) rather than actions (verbs)
3. Parameter names match function names they depend on
4. Comprehensive @tag decorators for lineage and metadata
5. Proper Builder patterns for driver creation
6. Error handling and logging throughout
7. Configuration-driven execution with sensible defaults
"""

# Future pipeline imports (when implemented)
# from .overture_maps_loader import run_overture_pipeline
# from .solar_irradiance_loader import run_solar_pipeline
# __all__.extend(['run_overture_pipeline', 'run_solar_pipeline'])

# Version information
__version__ = "0.1.0"
__author__ = "EO PV Pipeline Team"
__description__ = "Hamilton-based data loading pipelines for Earth Observation PV analysis"

# Pipeline registry for dynamic discovery
AVAILABLE_PIPELINES = {
    'overture_maps': {
        'module': 'overture_maps_loader',
        'function': 'run_overture_pipeline',
        'description': 'Global administrative boundaries and building footprints',
        'status': 'planned',
        'data_sources': ['overture_maps_s3'],
        'output_format': 'geoparquet',
        'estimated_runtime': '10-30 minutes'
    },
    'solar_irradiance': {
        'module': 'solar_irradiance_loader',
        'function': 'run_solar_pipeline',
        'description': 'Solar irradiance data from NREL and Google Solar API',
        'status': 'planned',
        'data_sources': ['nrel_api', 'google_solar_api'],
        'output_format': 'zarr_arrays',
        'estimated_runtime': '20-60 minutes'
    }
}


def list_pipelines():
    """
    List all available Hamilton pipelines with their metadata.
    
    Returns:
        dict: Pipeline registry with metadata for each pipeline
    """
    return AVAILABLE_PIPELINES


def get_pipeline_info(pipeline_name: str):
    """
    Get detailed information about a specific pipeline.
    
    Args:
        pipeline_name: Name of the pipeline
        
    Returns:
        dict: Pipeline metadata
        
    Raises:
        KeyError: If pipeline_name is not found
    """
    if pipeline_name not in AVAILABLE_PIPELINES:
        available = list(AVAILABLE_PIPELINES.keys())
        raise KeyError(f"Pipeline '{pipeline_name}' not found. Available: {available}")
    
    return AVAILABLE_PIPELINES[pipeline_name]


def run_pipeline(pipeline_name: str, **kwargs):
    """
    Dynamically run a Hamilton pipeline by name.
    
    Args:
        pipeline_name: Name of the pipeline to run
        **kwargs: Pipeline-specific configuration parameters
        
    Returns:
        Pipeline execution result
        
    Raises:
        KeyError: If pipeline_name is not found
        ImportError: If pipeline module cannot be imported
        NotImplementedError: If pipeline is not yet implemented
    """
    pipeline_info = get_pipeline_info(pipeline_name)
    
    if pipeline_info['status'] != 'active':
        raise NotImplementedError(f"Pipeline '{pipeline_name}' is {pipeline_info['status']}")
    
    if pipeline_name == 'doi_datasets':
        return run_doi_pipeline(**kwargs)
    else:
        # Future pipeline implementations
        raise NotImplementedError(f"Pipeline '{pipeline_name}' not yet implemented")


# Utility functions for dbt integration
def validate_pipeline_dependencies():
    """
    Validate that all required dependencies are installed for active pipelines.
    
    Returns:
        dict: Validation results for each pipeline
    """
    results = {}
    
    for pipeline_name, info in AVAILABLE_PIPELINES.items():
        if info['status'] != 'active':
            results[pipeline_name] = {'status': 'skipped', 'reason': f"Pipeline is {info['status']}"}
            continue
        
        try:
            if pipeline_name == 'doi_datasets':
                # Test imports for DOI pipeline
                import hamilton
                import pyarrow
                import geopandas
                import duckdb
                import datahugger
                results[pipeline_name] = {'status': 'valid', 'dependencies': 'all_found'}
            else:
                results[pipeline_name] = {'status': 'unknown', 'reason': 'not_implemented'}
                
        except ImportError as e:
            results[pipeline_name] = {'status': 'invalid', 'missing_dependency': str(e)}
    
    return results


if __name__ == "__main__":
    # CLI interface for pipeline management
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Hamilton Data Loaders CLI")
    parser.add_argument("command", choices=["list", "info", "run", "validate"])
    parser.add_argument("--pipeline", help="Pipeline name (for info and run commands)")
    parser.add_argument("--config", help="JSON configuration for run command")
    
    args = parser.parse_args()
    
    if args.command == "list":
        pipelines = list_pipelines()
        print("Available Hamilton Pipelines:")
        for name, info in pipelines.items():
            print(f"  {name}: {info['description']} ({info['status']})")
    
    elif args.command == "info":
        if not args.pipeline:
            print("Error: --pipeline required for info command")
            exit(1)
        info = get_pipeline_info(args.pipeline)
        print(json.dumps(info, indent=2))
    
    elif args.command == "run":
        if not args.pipeline:
            print("Error: --pipeline required for run command")
            exit(1)
        
        config = {}
        if args.config:
            config = json.loads(args.config)
        
        try:
            result = run_pipeline(args.pipeline, **config)
            print(f"Pipeline {args.pipeline} completed successfully")
            if isinstance(result, dict):
                print(f"Processed {len(result)} items")
        except Exception as e:
            print(f"Pipeline {args.pipeline} failed: {e}")
            exit(1)
    
    elif args.command == "validate":
        results = validate_pipeline_dependencies()
        print("Pipeline Dependency Validation:")
        for pipeline, result in results.items():
            status = result['status']
            if status == 'valid':
                print(f"  ✅ {pipeline}: All dependencies found")
            elif status == 'invalid':
                print(f"  ❌ {pipeline}: Missing {result['missing_dependency']}")
            elif status == 'skipped':
                print(f"  ⏭️  {pipeline}: {result['reason']}")
            else:
                print(f"  ❓ {pipeline}: {result.get('reason', 'Unknown status')}")
