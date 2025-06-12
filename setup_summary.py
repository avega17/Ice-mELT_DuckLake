#!/usr/bin/env python3
"""
Setup Summary for EO PV Data Ingestion Pipeline
This script provides a complete overview of the pipeline setup and next steps.
"""

import os
from pathlib import Path

def print_header(title):
    """Print a formatted header."""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)

def check_file_exists(filepath):
    """Check if a file exists and return status."""
    return "✓" if Path(filepath).exists() else "✗"

def main():
    print_header("EO PV Data Ingestion Pipeline Setup Summary")
    
    print("\n📁 PROJECT STRUCTURE:")
    files_to_check = [
        ("doi_dataset_pipeline.py", "Main pipeline script"),
        ("test_pipeline.py", "Test suite"),
        ("requirements_dlt.txt", "Pipeline dependencies"),
        (".dlt/config.toml", "dlt configuration"),
        (".dlt/secrets.toml", "API secrets"),
        ("dbt_project.yml", "dbt project configuration"),
        ("profiles_template.yml", "dbt profiles template"),
        ("models/staging/stg_doi_datasets.sql", "dbt staging model"),
        ("models/prepared/prep_pv_datasets_unified.sql", "dbt prepared model"),
        ("models/curated/curated_pv_dataset_summary.sql", "dbt curated summary model"),
        ("models/curated/curated_pv_geographic_coverage.sql", "dbt curated geographic model"),
        ("models/sources.yml", "dbt source definitions"),
        ("models/schema.yml", "dbt model documentation"),
        ("README_pipeline.md", "Documentation"),
    ]
    
    for filepath, description in files_to_check:
        status = check_file_exists(filepath)
        print(f"  {status} {filepath:<35} - {description}")
    
    print_header("NEXT STEPS")
    
    print("\n🔧 1. ACTIVATE ENVIRONMENT & TEST:")
    print("   conda activate eo-pv-cv")
    print("   python test_pipeline.py")
    
    print("\n📊 2. RUN SAMPLE PIPELINE:")
    print("   python doi_dataset_pipeline.py")
    
    print("\n🔍 3. VERIFY DATA LOADING:")
    print("   python -c \"import duckdb; conn = duckdb.connect('eo_pv_data.duckdb'); print(conn.execute('SHOW TABLES').fetchall())\"")
    
    print("\n🏗️  4. GENERATE DBT MODELS:")
    print("   dlt dbt generate")
    print("   # This will create additional dbt models based on your data schema")
    
    print("\n☁️  5. SETUP MOTHERDUCK (OPTIONAL):")
    print("   # Sign up at https://motherduck.com/")
    print("   # Get your access token")
    print("   # Update .dlt/config.toml with MotherDuck credentials")
    
    print("\n📈 6. DEVELOP DBT MODELS:")
    print("   # Copy profiles_template.yml to ~/.dbt/profiles.yml")
    print("   # Customize for your environment")
    print("   # Run: dbt run")
    print("   # Run: dbt test")
    
    print_header("AVAILABLE VECTOR DATASETS")

    datasets = [
        ("bradbury_2016_california", "19,433 PV modules in 4 CA cities", "Polygon", "Figshare"),
        ("stowell_2020_uk", "265,418 UK PV installations", "Point/Polygon", "Zenodo"),
        ("kruitwagen_2021_global", "68,661 global PV labels", "Polygon", "Zenodo"),
        ("global_harmonized_large_solar_farms_2020", "35,272 global wind/solar farms", "Polygon", "Figshare"),
        ("chn_med_res_pv_2024", "3,356 China PV installations", "Polygon", "GitHub"),
        ("ind_pv_solar_farms_2022", "117 India solar farm locations", "MultiPolygon", "GitHub"),
        ("global_pv_inventory_sent2_2024", "6,793 global solar farm coordinates", "Point", "GitHub"),
    ]

    for name, description, format_type, repo in datasets:
        print(f"  📍 {name}")
        print(f"     {description} ({format_type}) - {repo}")
    
    print_header("PIPELINE FEATURES")
    
    features = [
        "✓ Automated DOI dataset fetching via datahugger",
        "✓ Support for multiple repositories (Zenodo, Figshare, GitHub, ScienceBase)",
        "✓ DuckDB local storage with Parquet optimization",
        "✓ MotherDuck cloud scaling capability",
        "✓ dbt integration for data modeling",
        "✓ Geospatial data preservation and validation",
        "✓ Configurable file size limits and download options",
        "✓ Comprehensive logging and error handling",
    ]
    
    for feature in features:
        print(f"  {feature}")
    
    print_header("TROUBLESHOOTING")
    
    print("\n🐛 Common Issues:")
    print("   • Import errors → Ensure conda environment is activated")
    print("   • Download failures → Check internet connection and DOI URLs")
    print("   • Memory issues → Reduce max_mb parameter in config")
    print("   • dlt command not found → Verify dlt installation in conda env")
    
    print("\n📚 Documentation:")
    print("   • dlt docs: https://dlthub.com/docs/")
    print("   • dbt docs: https://docs.getdbt.com/")
    print("   • MotherDuck: https://motherduck.com/docs/")
    print("   • datahugger: https://github.com/J535D165/datahugger")
    
    print("\n" + "=" * 60)
    print(" Setup Complete! Ready to ingest EO PV data 🚀")
    print("=" * 60)

if __name__ == "__main__":
    main()
