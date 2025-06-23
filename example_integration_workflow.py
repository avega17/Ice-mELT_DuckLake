#!/usr/bin/env python3
"""
Example Integration Workflow: Hamilton + dbt

This script demonstrates the complete integration workflow between our refactored
Hamilton DOI pipeline and dbt transformations, showcasing the enhanced dependency
management and automatic source registration capabilities.

Usage:
    python example_integration_workflow.py --mode [standalone|dbt-integration|full-workflow]
"""

import argparse
import sys
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_standalone_hamilton():
    """
    Demonstrate standalone Hamilton pipeline execution with the refactored code.
    
    This shows how to run the Hamilton pipeline independently of dbt,
    useful for testing and development.
    """
    logger.info("=== Standalone Hamilton Pipeline Execution ===")
    
    try:
        # Import the refactored Hamilton pipeline
        from data_loaders.raw_pv_doi_ingest import run_doi_pipeline
        
        logger.info("Executing Hamilton DOI pipeline with enhanced dependency management...")
        
        # Run with configuration that demonstrates the new features
        result = run_doi_pipeline(
            database_path="../db/example_eo_pv_data.duckdb",
            export_geoparquet=True,
            use_parallel=True,  # Demonstrates Parallelizable/Collect usage
            use_cache=True,
            force_download=False,
            max_mb=100  # Smaller limit for example
        )
        
        logger.info(f"✅ Hamilton pipeline completed successfully!")
        logger.info(f"📊 Processed {len(result)} datasets:")
        
        for dataset_name, table_info in result.items():
            logger.info(f"   • {dataset_name}: {table_info}")
        
        return result
        
    except ImportError as e:
        logger.error(f"❌ Cannot import Hamilton pipeline: {e}")
        logger.error("Ensure you're in the correct directory and dependencies are installed")
        return None
    except Exception as e:
        logger.error(f"❌ Hamilton pipeline execution failed: {e}")
        return None


def demonstrate_dbt_integration(hamilton_result):
    """
    Demonstrate the Hamilton-dbt integration using the bridge utility.
    
    Args:
        hamilton_result: Result from Hamilton pipeline execution
    """
    logger.info("=== Hamilton-dbt Integration Demonstration ===")
    
    if not hamilton_result:
        logger.error("❌ No Hamilton result to integrate with dbt")
        return False
    
    try:
        # Import the Hamilton-dbt bridge
        from data_loaders.utils.hamilton_dbt_bridge import HamiltonDbtBridge
        
        logger.info("Initializing Hamilton-dbt bridge...")
        bridge = HamiltonDbtBridge()
        
        # Generate dbt sources from Hamilton results
        logger.info("Generating dbt sources from Hamilton pipeline results...")
        source_config = bridge.update_sources_from_hamilton('doi_datasets')
        
        # Validate the generated sources
        logger.info("Validating generated dbt sources...")
        is_valid = bridge.validate_sources('doi_datasets')
        
        if is_valid:
            logger.info("✅ dbt sources generated and validated successfully!")
            
            # Show what was generated
            source_info = source_config['sources'][0]
            table_count = len(source_info['tables'])
            logger.info(f"📋 Generated {table_count} table definitions in sources/doi_datasets_sources.yml")
            
            # Show example of generated source
            if source_info['tables']:
                example_table = source_info['tables'][0]
                logger.info(f"📄 Example table definition: {example_table['name']}")
                logger.info(f"   Description: {example_table['description']}")
                logger.info(f"   Hamilton tags: {example_table['meta']['hamilton_tags']}")
            
            return True
        else:
            logger.error("❌ dbt sources validation failed")
            return False
            
    except ImportError as e:
        logger.error(f"❌ Cannot import Hamilton-dbt bridge: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ dbt integration failed: {e}")
        return False


def demonstrate_full_workflow():
    """
    Demonstrate the complete workflow from Hamilton execution to dbt integration.
    
    This shows the end-to-end process that would typically be automated
    in a production environment.
    """
    logger.info("=== Complete Hamilton → dbt Workflow ===")
    
    # Step 1: Execute Hamilton pipeline
    logger.info("Step 1: Executing Hamilton pipeline...")
    hamilton_result = run_standalone_hamilton()
    
    if not hamilton_result:
        logger.error("❌ Workflow failed at Hamilton execution step")
        return False
    
    # Step 2: Integrate with dbt
    logger.info("Step 2: Integrating with dbt...")
    dbt_success = demonstrate_dbt_integration(hamilton_result)
    
    if not dbt_success:
        logger.error("❌ Workflow failed at dbt integration step")
        return False
    
    # Step 3: Show next steps for dbt
    logger.info("Step 3: Next steps for dbt workflow...")
    logger.info("🔄 You can now run the following dbt commands:")
    logger.info("   dbt source freshness --select source:raw_data")
    logger.info("   dbt run --models staging+")
    logger.info("   dbt test")
    
    logger.info("✅ Complete workflow demonstration successful!")
    return True


def show_dag_visualization():
    """
    Generate and display information about Hamilton DAG visualizations.
    
    This demonstrates the improved DAG visualizations from our refactoring.
    """
    logger.info("=== Hamilton DAG Visualization ===")
    
    try:
        from hamilton import driver
        import data_loaders.raw_pv_doi_ingest as pipeline_module
        
        logger.info("Creating Hamilton driver with refactored pipeline...")
        dr = driver.Builder().with_modules(pipeline_module).build()
        
        # Get function information
        functions = dr.list_available_variables()
        function_names = [f.name for f in functions]
        
        logger.info(f"📊 Hamilton pipeline contains {len(function_names)} functions:")
        logger.info("   Key functions demonstrating proper dependency management:")
        
        key_functions = [
            'dataset_metadata',
            'target_datasets', 
            'dataset_download_path',
            'dataset_geospatial_files',
            'dataset_processed_gdf',
            'dataset_arrow_table',
            'collected_arrow_tables',
            'store_individual_datasets'
        ]
        
        for func_name in key_functions:
            if func_name in function_names:
                logger.info(f"   ✅ {func_name}")
            else:
                logger.info(f"   ❌ {func_name} (missing)")
        
        # Generate DAG visualization
        logger.info("Generating DAG visualization...")
        output_path = "./dag_visualizations/refactored_hamilton_dag.png"
        Path("./dag_visualizations").mkdir(exist_ok=True)
        
        try:
            dr.display_all_functions(output_path)
            logger.info(f"✅ DAG visualization saved to: {output_path}")
            logger.info("🎨 The visualization should show rich, multi-dimensional dependency relationships")
        except Exception as viz_error:
            logger.warning(f"⚠️  DAG visualization failed (this is optional): {viz_error}")
            logger.info("💡 Install graphviz for better visualizations: conda install graphviz")
        
        return True
        
    except ImportError as e:
        logger.error(f"❌ Cannot import Hamilton components: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ DAG visualization demonstration failed: {e}")
        return False


def validate_environment():
    """
    Validate that the environment is properly set up for the integration.
    
    Returns:
        bool: True if environment is valid, False otherwise
    """
    logger.info("=== Environment Validation ===")
    
    # Check required directories
    required_dirs = [
        "data_loaders",
        "data_loaders/utils",
        "macros"
    ]
    
    for dir_path in required_dirs:
        if not Path(dir_path).exists():
            logger.error(f"❌ Required directory missing: {dir_path}")
            return False
        else:
            logger.info(f"✅ Directory found: {dir_path}")
    
    # Check required files
    required_files = [
        "data_loaders/__init__.py",
        "data_loaders/utils/__init__.py",
        "data_loaders/utils/hamilton_dbt_bridge.py",
        "macros/run_hamilton_pipeline.sql",
        "raw_pv_doi_ingest.py"  # Should be moved to data_loaders/
    ]
    
    for file_path in required_files:
        if not Path(file_path).exists():
            logger.warning(f"⚠️  File missing: {file_path}")
        else:
            logger.info(f"✅ File found: {file_path}")
    
    # Check Python dependencies
    required_packages = [
        'hamilton',
        'pyarrow', 
        'geopandas',
        'duckdb',
        'datahugger',
        'yaml'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"✅ Package available: {package}")
        except ImportError:
            missing_packages.append(package)
            logger.error(f"❌ Package missing: {package}")
    
    if missing_packages:
        logger.error(f"❌ Missing packages: {missing_packages}")
        logger.info("💡 Install with: pip install " + " ".join(missing_packages))
        return False
    
    logger.info("✅ Environment validation passed!")
    return True


def main():
    """Main CLI interface for the integration workflow demonstration."""
    parser = argparse.ArgumentParser(
        description="Hamilton + dbt Integration Workflow Demonstration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python example_integration_workflow.py --mode standalone
    python example_integration_workflow.py --mode dbt-integration  
    python example_integration_workflow.py --mode full-workflow
    python example_integration_workflow.py --mode validate
    python example_integration_workflow.py --mode visualize
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["standalone", "dbt-integration", "full-workflow", "validate", "visualize"],
        default="full-workflow",
        help="Demonstration mode to run"
    )
    
    args = parser.parse_args()
    
    logger.info("🚀 Hamilton + dbt Integration Workflow Demonstration")
    logger.info("=" * 60)
    
    # Always validate environment first
    if not validate_environment():
        logger.error("❌ Environment validation failed. Please fix issues and try again.")
        sys.exit(1)
    
    success = False
    
    if args.mode == "standalone":
        success = run_standalone_hamilton() is not None
    elif args.mode == "dbt-integration":
        # Run Hamilton first, then demonstrate dbt integration
        hamilton_result = run_standalone_hamilton()
        success = demonstrate_dbt_integration(hamilton_result)
    elif args.mode == "full-workflow":
        success = demonstrate_full_workflow()
    elif args.mode == "validate":
        success = True  # Already validated above
    elif args.mode == "visualize":
        success = show_dag_visualization()
    
    logger.info("=" * 60)
    if success:
        logger.info("✅ Demonstration completed successfully!")
        logger.info("🎯 The refactored Hamilton pipeline demonstrates:")
        logger.info("   • Proper dependency management (parameter names match function names)")
        logger.info("   • Enhanced function modifiers and tags for lineage tracking")
        logger.info("   • Builder patterns for better dbt integration")
        logger.info("   • Automatic dbt source generation from Hamilton results")
        logger.info("   • Rich, multi-dimensional DAG visualizations")
    else:
        logger.error("❌ Demonstration failed. Check the logs above for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
