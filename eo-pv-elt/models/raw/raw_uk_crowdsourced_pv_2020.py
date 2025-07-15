"""
dbt Python raw model for UK Crowdsourced PV 2020 dataset.
"""

def model(dbt, session):
    import os
    import json
    import pandas as pd
    from dotenv import load_dotenv
    
    load_dotenv()
    print("🚀 dbt Python raw: Loading UK Crowdsourced PV 2020...")

    dbt.config(materialized='table', indexes=[{'columns': ['dataset_name'], 'type': 'btree'}])

    dataset_name = "uk_crowdsourced_pv_2020"
    repo_root = os.getenv('REPO_ROOT', '.')
    
    with open(f"{repo_root}/data_loaders/doi_manifest.json", 'r') as f:
        manifest = json.load(f)
    
    dataset_metadata = manifest[dataset_name]

    # Multiple methods to detect target
    target_name = os.getenv('DBT_TARGET', 'dev')

    # Alternative: Check if we're connected to MotherDuck
    db_name = str(session.execute("SELECT current_database()").fetchone()[0])
    print(f"   🌱 Connected to database: {db_name}")
    if db_name.startswith('md:') or 'motherduck' in db_name.lower():
        target_name = 'prod'
        print(f"   🎯 Target detected via MotherDuck connection: {target_name}")
    else:
        print(f"   🎯 Target from DBT_TARGET env var: {target_name}")

    # Use GEOPARQUET_SOURCE_PATH which is set by target scripts
    # For prod: Use r2:// syntax for Cloudflare R2; For dev: /Users/.../db/geoparquet
    source_path = os.getenv('GEOPARQUET_SOURCE_PATH')
    file_path = f"{source_path}/raw_{dataset_name}.parquet"
    is_prod_target = target_name == 'prod' or file_path.startswith('r2://') or file_path.startswith('s3://')

    print(f"   🎯 dbt target: {target_name}")
    print(f"   📁 File: {file_path}")

    # Install and load required extensions
    session.execute("INSTALL spatial; LOAD spatial")

    # For R2 access, load httpfs extension (R2 secret configured in profiles.yml)
    if is_prod_target:
        session.execute("INSTALL httpfs; LOAD httpfs")
        print(f"   🌐 Loaded httpfs extension for R2 access")
        print(f"   ✅ Using R2 secret from dbt profiles.yml configuration")

    # Read the GeoParquet file using DuckDB's native support
    # Include both WKT and WKB geometry formats for flexibility
    geometry_parse = "ST_GeomFromWKB(geometry)" if is_prod_target else "geometry"
    query = f"""
        SELECT * EXCLUDE (geometry),
            ST_AsText({geometry_parse}) as geometry_wkt,
            ST_AsWKB({geometry_parse}) as geometry_wkb,
            '{dataset_name}' as dataset_name,
            '{dataset_metadata.get('doi', '')}' as doi,
            '{dataset_metadata.get('repo', '')}' as repo,
            '{dataset_metadata.get('paper_doi', '')}' as paper_doi,
            '{dataset_metadata.get('paper_title', '')}' as paper_title,
            CURRENT_TIMESTAMP as dbt_loaded_at
        FROM read_parquet('{file_path}')
        WHERE ST_IsValid({geometry_parse}) = true  -- Filter out invalid geometries
    """

    # Execute query and return DataFrame for dbt to materialize
    print(f"   🚀 Executing query...:\n\n{query}\n\n")
    result = session.execute(query)
    df = result.df()
    print(f"   ✅ Loaded {len(df):,}:  records")

    return df
