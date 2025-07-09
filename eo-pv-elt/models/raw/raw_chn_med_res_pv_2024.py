import os
import json
import ibis

def model(dbt, session):
    """
    Raw China Medium Resolution PV 2024 dataset.
    Simple Python model that reads GeoParquet and adds basic metadata.
    """
    
    # Dataset identification
    dataset_name = "chn_med_res_pv_2024"

    # Read metadata from doi_manifest.json
    repo_root = os.getenv('REPO_ROOT', '.')
    manifest_path = f"{repo_root}/data_loaders/doi_manifest.json"

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    if dataset_name not in manifest:
        raise ValueError(f"Dataset '{dataset_name}' not found in doi_manifest.json")

    dataset_metadata = manifest[dataset_name]

    # Determine file path based on dbt target
    # Check if we're running against prod target by examining the connection
    try:
        # Connect to see what database we're using
        con = ibis.duckdb.connect()
        current_db = con.raw_sql("SELECT current_database()").fetchone()[0]
        is_prod_target = 'md:' in str(current_db) or 'motherduck' in str(current_db).lower()
        print(f"   🔍 Current database: {current_db}")
        print(f"   🎯 Target detected: {'PROD' if is_prod_target else 'DEV'}")
    except Exception as e:
        # Fallback: check environment variables
        print(f"   ⚠️  Could not detect target from database: {e}")
        motherduck_token = os.getenv('MOTHERDUCK_TOKEN', '')
        is_prod_target = bool(motherduck_token)
        print(f"   🎯 Target fallback: {'PROD' if is_prod_target else 'DEV'} (based on MOTHERDUCK_TOKEN)")

    if is_prod_target:
        # Production: Read from R2 cloud storage
        r2_public_url = os.getenv('R2_PUBLIC_URL')
        if not r2_public_url:
            raise ValueError("R2_PUBLIC_URL environment variable required for prod target")
        file_path = f"{r2_public_url}/geoparquet/raw_{dataset_name}.parquet"
        print(f"📊 Loading dataset: {dataset_name} (PROD - R2 Cloud)")
    else:
        # Development: Read from local files
        gpq_export_path = os.getenv('GPQ_EXPORT_PATH', 'db/geoparquet')
        file_path = f"{gpq_export_path}/raw_{dataset_name}.parquet"
        print(f"📊 Loading dataset: {dataset_name} (DEV - Local)")

    print(f"   📁 File: {file_path}")
    print(f"   📄 DOI: {dataset_metadata.get('doi', 'N/A')}")

    # Connect to DuckDB
    con = ibis.duckdb.connect()

    # Install spatial extension for GeoParquet support
    con.raw_sql("INSTALL spatial")
    con.raw_sql("LOAD spatial")

    # Read the GeoParquet file
    df = con.read_parquet(file_path)

    # Add dataset metadata columns from manifest
    df = df.mutate(
        dataset_name=ibis.literal(dataset_name),
        doi=ibis.literal(dataset_metadata.get('doi', None)),
        repo=ibis.literal(dataset_metadata.get('repo', None)),
        paper_doi=ibis.literal(dataset_metadata.get('paper_doi', None)),
        paper_title=ibis.literal(dataset_metadata.get('paper_title', None)),
        dbt_loaded_at=ibis.now()
    )
    
    print(f"   ✅ Loaded {df.count().execute():,} records")
    print(f"   📋 Columns: {len(df.columns)}")
    
    return df
