#!/usr/bin/env python3
"""
Migrate existing DOI PV data from GeoParquet files to DuckLake.

This script:
1. Sets up DuckLake with SQLite catalog
2. Loads existing GeoParquet files as DOI tables
3. Prepares data for Hamilton consolidation DAG
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add repo root to path
repo_root = Path(os.getenv('REPO_ROOT', '.')).resolve()
sys.path.insert(0, str(repo_root))

def migrate_to_ducklake():
    """Migrate existing DOI PV data to DuckLake."""
    
    try:
        import ibis
        print("✅ Ibis imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import Ibis: {e}")
        return False
    
    # DuckLake configuration
    catalog_path = "ducklake_catalog.sqlite"
    data_path = "ducklake_data"
    geoparquet_path = "geoparquet"
    
    full_catalog_path = repo_root / "db" / catalog_path
    full_data_path = repo_root / "db" / data_path
    full_geoparquet_path = repo_root / "db" / geoparquet_path
    
    # Ensure paths exist
    full_catalog_path.parent.mkdir(parents=True, exist_ok=True)
    full_data_path.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Catalog path: {full_catalog_path}")
    print(f"📂 Data path: {full_data_path}")
    print(f"📦 GeoParquet path: {full_geoparquet_path}")
    
    try:
        # Create DuckDB connection with DuckLake extension
        print("🔧 Creating DuckDB connection with extensions...")
        con = ibis.duckdb.connect(extensions=["ducklake", "spatial"])
        print("✅ DuckDB connection created")
        
        # Install and load spatial extension (required for GeoParquet)
        try:
            print("🔧 Loading spatial extension for GeoParquet support...")
            con.raw_sql("INSTALL spatial")
            con.raw_sql("LOAD spatial")
            print("✅ Spatial extension loaded")
        except Exception as e:
            print(f"⚠️  Spatial extension warning: {e}")
        
        # Install community extensions manually
        try:
            print("🔧 Installing community extensions...")
            con.raw_sql("INSTALL h3 FROM community")
            con.raw_sql("LOAD h3")
            # Note: geography extension temporarily commented out due to availability issues
            # con.raw_sql("INSTALL geography FROM community")
            # con.raw_sql("LOAD geography")
            print("✅ Community extensions loaded")
        except Exception as e:
            print(f"⚠️  Community extension warning: {e}")
        
        # Attach DuckLake using raw SQL (consistent with setup script)
        ducklake_connection_string = f"ducklake:sqlite:{full_catalog_path}"
        print(f"🔗 Attaching DuckLake: {ducklake_connection_string}")
        
        attach_sql = f"""
        ATTACH '{ducklake_connection_string}' AS eo_pv_lakehouse
            (DATA_PATH '{full_data_path}/');
        """
        con.raw_sql(attach_sql)
        con.raw_sql("USE eo_pv_lakehouse")
        print("✅ DuckLake attached successfully")
        
        # Find all GeoParquet files
        geoparquet_files = list(full_geoparquet_path.glob("doi_*.parquet"))
        print(f"📦 Found {len(geoparquet_files)} GeoParquet files")
        
        for parquet_file in geoparquet_files:
            # Extract dataset name from filename
            dataset_name = parquet_file.stem  # Remove .parquet extension
            table_name = dataset_name  # Use full name including doi_ prefix
            
            print(f"🔄 Loading {dataset_name} -> {table_name}")
            
            try:
                # First, check the schema of the GeoParquet file
                schema_sql = f"DESCRIBE SELECT * FROM read_parquet('{parquet_file}') LIMIT 0;"
                schema_result = con.raw_sql(schema_sql).fetchall()
                
                # Check if there are geometry columns
                has_geometry = any('GEOMETRY' in str(row) for row in schema_result)
                
                if has_geometry:
                    print(f"   🗺️  Detected geometry columns, converting to WKT")
                    # Load GeoParquet with geometry conversion to WKT (text)
                    load_sql = f"""
                    CREATE OR REPLACE TABLE {table_name} AS 
                    SELECT 
                        * EXCLUDE (geometry),
                        ST_AsText(geometry) as geometry_wkt
                    FROM read_parquet('{parquet_file}');
                    """
                else:
                    # Load GeoParquet file as-is (no geometry columns)
                    load_sql = f"""
                    CREATE OR REPLACE TABLE {table_name} AS 
                    SELECT * FROM read_parquet('{parquet_file}');
                    """
                
                con.raw_sql(load_sql)
                
                # Get record count
                count_result = con.raw_sql(f"SELECT COUNT(*) as count FROM {table_name}").fetchone()
                count = count_result[0] if count_result else 0
                
                print(f"   ✅ Loaded {count:,} records")
                
            except Exception as e:
                print(f"   ❌ Failed to load {dataset_name}: {e}")
                print(f"      Error details: {str(e)}")
                # Try a fallback approach without geometry
                try:
                    print(f"   🔄 Trying fallback without geometry columns...")
                    fallback_sql = f"""
                    CREATE OR REPLACE TABLE {table_name} AS 
                    SELECT * EXCLUDE (geometry) FROM read_parquet('{parquet_file}');
                    """
                    con.raw_sql(fallback_sql)
                    
                    count_result = con.raw_sql(f"SELECT COUNT(*) as count FROM {table_name}").fetchone()
                    count = count_result[0] if count_result else 0
                    print(f"   ✅ Fallback successful: {count:,} records (without geometry)")
                    
                except Exception as fallback_error:
                    print(f"   ❌ Fallback also failed: {fallback_error}")
        
        # List all tables in DuckLake
        tables = con.list_tables()
        print(f"\n📋 DuckLake tables: {tables}")
        
        # Show table counts
        print("\n📊 Table summary:")
        for table in tables:
            if table.startswith('doi_'):
                try:
                    count_result = con.raw_sql(f"SELECT COUNT(*) as count FROM {table}").fetchone()
                    count = count_result[0] if count_result else 0
                    print(f"   {table}: {count:,} records")
                except Exception as e:
                    print(f"   {table}: Error getting count - {e}")
        
        con.disconnect()
        print("\n✅ Migration to DuckLake completed successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Migration to DuckLake failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🦆 Migrating DOI PV data to DuckLake...")
    success = migrate_to_ducklake()
    
    if success:
        print("\n🎉 Migration completed successfully!")
        print("   Ready to test Hamilton consolidation DAG with DuckLake")
    else:
        print("\n💥 Migration failed!")
        print("   Need to fix migration issues before testing Hamilton DAG")
    
    sys.exit(0 if success else 1)
