#!/usr/bin/env python3
"""
Runner script for the Hamilton-based DOI dataset pipeline.

This script demonstrates how to use the Hamilton pipeline to:
1. Download and process DOI datasets
2. Query Overture Maps for administrative boundaries
3. Load everything into DuckDB for analysis

Usage:
    python run_hamilton_pipeline.py --datasets bradbury stowell --max-mb 50
    python run_hamilton_pipeline.py --all --max-mb 100
    python run_hamilton_pipeline.py --test-only
"""

import argparse
import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent))

from hamilton_doi_pipeline import DATASET_METADATA, run_doi_pipeline


def main():
    """Main runner function."""
    parser = argparse.ArgumentParser(
        description="Run Hamilton-based DOI dataset pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process specific datasets
  python run_hamilton_pipeline.py --datasets bradbury stowell --max-mb 50
  
  # Process all datasets
  python run_hamilton_pipeline.py --all --max-mb 100
  
  # Test pipeline without downloads
  python run_hamilton_pipeline.py --test-only
  
  # Process with custom database path
  python run_hamilton_pipeline.py --datasets china --db-path ./custom.duckdb
        """
    )
    
    # Dataset selection
    dataset_group = parser.add_mutually_exclusive_group(required=True)
    dataset_group.add_argument(
        "--datasets",
        nargs="+",
        choices=list(DATASET_METADATA.keys()) + ["bradbury", "stowell", "china", "global_large", "india", "global_inventory"],
        help="Specific datasets to process"
    )
    dataset_group.add_argument(
        "--all",
        action="store_true",
        help="Process all available datasets"
    )
    dataset_group.add_argument(
        "--test-only",
        action="store_true",
        help="Run tests only, no data processing"
    )
    
    # Processing options
    parser.add_argument(
        "--max-mb",
        type=int,
        default=100,
        help="Maximum file size in MB (default: 100)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if files exist"
    )
    parser.add_argument(
        "--db-path",
        default="./eo_pv_data.duckdb",
        help="Path to DuckDB database (default: ./eo_pv_data.duckdb)"
    )
    
    # Visualization options
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Generate Hamilton DAG visualization"
    )
    parser.add_argument(
        "--lineage",
        action="store_true",
        help="Show data lineage information"
    )
    
    args = parser.parse_args()
    
    # Handle test-only mode
    if args.test_only:
        print("🧪 Running pipeline tests...")
        from test_hamilton_pipeline import main as test_main
        success = test_main()
        return 0 if success else 1
    
    # Handle visualization
    if args.visualize:
        visualize_pipeline()
        return 0
    
    # Handle lineage
    if args.lineage:
        show_lineage()
        return 0
    
    # Determine datasets to process
    if args.all:
        datasets = list(DATASET_METADATA.keys())
        print(f"📊 Processing all {len(datasets)} datasets")
    else:
        # Map short names to full names
        name_mapping = {
            "bradbury": "bradbury_2016_california",
            "stowell": "stowell_2020_uk",
            "china": "chn_med_res_pv_2024",
            "global_large": "global_harmonized_large_solar_farms_2020",
            "india": "ind_pv_solar_farms_2022",
            "global_inventory": "global_pv_inventory_sent2_2024"
        }
        
        datasets = []
        for dataset in args.datasets:
            if dataset in name_mapping:
                datasets.append(name_mapping[dataset])
            else:
                datasets.append(dataset)
        
        print(f"📊 Processing {len(datasets)} datasets: {', '.join(datasets)}")
    
    # Show dataset information
    print("\n📋 Dataset Information:")
    for dataset in datasets:
        if dataset in DATASET_METADATA:
            meta = DATASET_METADATA[dataset]
            print(f"  • {dataset}")
            print(f"    {meta['description']}")
            print(f"    Format: {meta['label_fmt']}, Count: {meta['label_count']:,}")
        else:
            print(f"  ⚠️  Unknown dataset: {dataset}")
    
    print(f"\n⚙️  Configuration:")
    print(f"  Max file size: {args.max_mb} MB")
    print(f"  Force download: {args.force}")
    print(f"  Database path: {args.db_path}")
    
    # Confirm before processing
    if not args.force:
        response = input("\n🚀 Proceed with pipeline execution? [y/N]: ")
        if response.lower() not in ['y', 'yes']:
            print("❌ Pipeline execution cancelled")
            return 0
    
    # Run the pipeline
    try:
        print("\n🔄 Starting Hamilton pipeline...")
        result = run_doi_pipeline(
            datasets=datasets,
            max_mb=args.max_mb,
            force=args.force,
            database_path=args.db_path
        )
        
        print(f"\n✅ Pipeline completed successfully!")
        print(f"📊 {result}")
        
        # Show next steps
        print(f"\n🔍 Next steps:")
        print(f"  1. Verify raw data: duckdb {args.db_path} -c \"SELECT COUNT(*) FROM raw_data.doi_pv_features\"")
        print(f"  2. Run dbt staging models: dbt run --select staging")
        print(f"  3. Run full dbt pipeline: dbt run")
        print(f"  4. Generate documentation: dbt docs generate")
        print(f"\n📊 Hamilton has loaded raw data into 'raw_data' schema for dbt consumption")
        print(f"💡 dbt models can reference: source('raw_data', 'doi_pv_features')")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        return 1


def visualize_pipeline():
    """Generate Hamilton DAG visualization."""
    try:
        from hamilton import driver
        import hamilton_doi_pipeline as pipeline_module
        
        config = {"max_mb": 100, "force": False, "database_path": "./eo_pv_data.duckdb"}
        dr = driver.Driver(config, pipeline_module)
        
        # Generate visualization
        dr.visualize_execution(
            ["dataset_pipeline__bradbury"],
            output_file_path="hamilton_pipeline_dag.png",
            render_kwargs={"format": "png"}
        )
        
        print("📊 Pipeline DAG visualization saved to: hamilton_pipeline_dag.png")
        
    except Exception as e:
        print(f"❌ Visualization failed: {e}")
        print("💡 Try: pip install graphviz")


def show_lineage():
    """Show data lineage information."""
    try:
        from hamilton import driver
        import hamilton_doi_pipeline as pipeline_module
        
        config = {"max_mb": 100, "force": False, "database_path": "./eo_pv_data.duckdb"}
        dr = driver.Driver(config, pipeline_module)
        
        print("📈 Hamilton Pipeline Data Lineage:")
        print("=" * 50)
        
        # Show available variables (functions)
        variables = dr.list_available_variables()
        print(f"Available pipeline functions: {len(variables)}")
        
        for var in sorted(variables):
            print(f"  • {var}")
        
        print("\n🔗 Pipeline Dependencies:")
        print("  dataset_metadata → download_doi_dataset → extract_geospatial_files")
        print("  → process_geospatial_data → export_geoparquet → load_to_duckdb")
        
        print("\n📊 Parameterized Datasets:")
        for dataset in DATASET_METADATA.keys():
            print(f"  • dataset_pipeline__{dataset}")
        
    except Exception as e:
        print(f"❌ Lineage display failed: {e}")


if __name__ == "__main__":
    sys.exit(main())
