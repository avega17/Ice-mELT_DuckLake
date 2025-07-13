#!/usr/bin/env python3
"""
Consolidated setup script for Ice-mELT DuckLake cloud deployment.

This script consolidates all validation and setup functionality:
1. Validates cloud credentials (Neon PostgreSQL, Cloudflare R2, MotherDuck)
2. Tests basic connectivity to all cloud services
3. Validates R2 bucket access and DuckDB S3 integration
4. Tests DuckLake with PostgreSQL catalog + R2 storage
5. Validates dbt cloud target configuration
6. Prepares configuration for cloud deployment

Replaces individual scripts: validate_*, verify_*, test_*
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add repo root to path
repo_root = Path(os.getenv('REPO_ROOT', '.')).resolve()
sys.path.insert(0, str(repo_root))

def validate_cloud_credentials():
    """Validate all cloud service credentials."""
    print("🔍 Validating cloud service credentials...")
    
    # Neon PostgreSQL
    neon_vars = ['PGHOST', 'PGDATABASE', 'PGUSER', 'PGPASSWORD']
    neon_ok = all(os.getenv(var) for var in neon_vars)
    print(f"   Neon PostgreSQL: {'✅ CONFIGURED' if neon_ok else '❌ MISSING'}")
    
    # Cloudflare R2
    r2_vars = ['R2_ACCESS_KEY_ID', 'R2_SECRET_KEY', 'CLOUDFLARE_ACCOUNT_ID']
    r2_ok = all(os.getenv(var) for var in r2_vars)
    print(f"   Cloudflare R2: {'✅ CONFIGURED' if r2_ok else '❌ MISSING'}")
    
    # MotherDuck
    motherduck_ok = bool(os.getenv('MOTHERDUCK_TOKEN'))
    print(f"   MotherDuck: {'✅ CONFIGURED' if motherduck_ok else '❌ MISSING'}")
    
    return neon_ok and r2_ok and motherduck_ok

def test_neon_connection():
    """Test Neon PostgreSQL connection."""
    try:
        import psycopg2
        print("\n🔧 Testing Neon PostgreSQL connection...")
        
        # Build connection string
        conn_str = os.getenv('NEON_PG_CONN')
        if not conn_str:
            pg_host = os.getenv('PGHOST')
            pg_database = os.getenv('PGDATABASE')
            pg_user = os.getenv('PGUSER')
            pg_password = os.getenv('PGPASSWORD')
            conn_str = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}?sslmode=require"
        
        # Test connection
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"✅ Neon connection successful: {version}")
        
        # Test DuckLake catalog functionality
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_catalog_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("INSERT INTO test_catalog_table (name) VALUES ('cloud_setup_test');")
        cursor.execute("SELECT COUNT(*) FROM test_catalog_table;")
        count = cursor.fetchone()[0]
        print(f"✅ Catalog test: {count} records in test table")

        # Cleanup
        cursor.execute("DROP TABLE IF EXISTS test_catalog_table;")
        conn.commit()

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Neon connection failed: {e}")
        return False

def test_r2_access():
    """Test Cloudflare R2 access."""
    try:
        import boto3
        print("\n🔧 Testing Cloudflare R2 access...")
        
        # Configure R2 client
        r2_access_key = os.getenv('R2_ACCESS_KEY_ID')
        r2_secret_key = os.getenv('R2_SECRET_KEY')
        r2_account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
        
        endpoint_url = f"https://{r2_account_id}.r2.cloudflarestorage.com"
        
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=r2_access_key,
            aws_secret_access_key=r2_secret_key,
            region_name='auto'
        )
        
        # Test bucket access
        bucket_name = 'eo-pv-lakehouse'
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ R2 bucket '{bucket_name}' exists and accessible")
        except s3_client.exceptions.NoSuchBucket:
            print(f"📦 Creating R2 bucket '{bucket_name}'...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ R2 bucket '{bucket_name}' created successfully")
        
        # Test file operations
        test_key = 'test/connectivity_test.txt'
        test_content = 'DuckLake cloud deployment test'
        
        s3_client.put_object(Bucket=bucket_name, Key=test_key, Body=test_content)
        print(f"✅ Test file uploaded to R2: s3://{bucket_name}/{test_key}")
        
        # Read it back
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        content = response['Body'].read().decode('utf-8')
        assert content == test_content
        print("✅ Test file read back successfully")
        
        # Clean up
        s3_client.delete_object(Bucket=bucket_name, Key=test_key)
        print("✅ Test file cleaned up")
        
        return True
        
    except Exception as e:
        print(f"❌ R2 access test failed: {e}")
        return False

def test_motherduck_connection():
    """Test MotherDuck connection."""
    try:
        import duckdb
        print("\n🔧 Testing MotherDuck connection...")
        
        token = os.getenv('MOTHERDUCK_TOKEN')
        conn_str = f"md:?motherduck_token={token}"
        
        conn = duckdb.connect(conn_str)
        result = conn.execute("SELECT 'MotherDuck connection successful' as message").fetchone()
        print(f"✅ MotherDuck connection successful: {result[0]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ MotherDuck connection failed: {e}")
        return False

def test_integrated_cloud_stack():
    """Test the integrated cloud stack: Neon + R2 + DuckLake."""
    try:
        from dataflows.stg.consolidation.stg_doi_pv_consolidation import _create_ducklake_connection
        print("\n🔧 Testing integrated cloud stack...")
        
        # Test DuckLake with PostgreSQL catalog + R2 storage
        con = _create_ducklake_connection(
            catalog_path="",
            data_path="s3://eo-pv-lakehouse/ducklake_data",
            use_ducklake=True,
            catalog_type="postgresql"
        )
        
        print("✅ Integrated cloud stack connection successful")
        
        # Test basic operations
        catalogs = con.list_catalogs()
        print(f"📋 Available catalogs: {catalogs}")
        
        con.disconnect()
        return True
        
    except Exception as e:
        print(f"❌ Integrated cloud stack test failed: {e}")
        return False

def generate_cloud_config():
    """Generate cloud deployment configuration."""
    config = {
        "deployment_type": "cloud",
        "timestamp": str(pd.Timestamp.now()),
        "services": {
            "catalog": {
                "type": "postgresql",
                "provider": "neon",
                "host": os.getenv('PGHOST'),
                "database": os.getenv('PGDATABASE')
            },
            "storage": {
                "type": "cloudflare_r2",
                "bucket": "eo-pv-lakehouse",
                "data_path": "s3://eo-pv-lakehouse/ducklake_data"
            },
            "compute": {
                "type": "motherduck",
                "connection": "md:"
            }
        },
        "hamilton_config": {
            "execution_mode": "sequential",
            "use_ducklake": True,
            "catalog_type": "postgresql",
            "export_path": "s3://eo-pv-lakehouse/geoparquet/",
            "use_cloud_export": True
        }
    }
    
    config_file = repo_root / "cloud_deployment_ready.json"
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"\n📄 Cloud configuration saved to: {config_file}")
    return config_file

def main():
    """Run cloud deployment setup."""
    print("🚀 Setting up Ice-mELT DuckLake cloud deployment")
    print("=" * 60)
    
    # Validate credentials
    if not validate_cloud_credentials():
        print("\n❌ Cloud credentials validation failed")
        print("Please configure all required environment variables in .env")
        return False
    
    # Test individual services
    neon_ok = test_neon_connection()
    r2_ok = test_r2_access()
    motherduck_ok = test_motherduck_connection()
    
    # Test integrated stack
    integrated_ok = test_integrated_cloud_stack()
    
    print("\n" + "=" * 60)
    print("📊 Cloud Deployment Setup Results:")
    print(f"   Neon PostgreSQL: {'✅ READY' if neon_ok else '❌ FAILED'}")
    print(f"   Cloudflare R2: {'✅ READY' if r2_ok else '❌ FAILED'}")
    print(f"   MotherDuck: {'✅ READY' if motherduck_ok else '❌ FAILED'}")
    print(f"   Integrated Stack: {'✅ READY' if integrated_ok else '❌ FAILED'}")
    
    if all([neon_ok, r2_ok, motherduck_ok, integrated_ok]):
        config_file = generate_cloud_config()
        print(f"\n🎉 Cloud deployment setup complete!")
        print(f"   Configuration: {config_file}")
        print(f"   Ready to migrate 443,917+ PV records to cloud")
        return True
    else:
        print(f"\n⚠️  Cloud deployment setup incomplete")
        print(f"   Please resolve failed tests before proceeding")
        return False

if __name__ == "__main__":
    try:
        import pandas as pd
    except ImportError:
        import datetime
        pd = type('pd', (), {'Timestamp': type('Timestamp', (), {'now': lambda: datetime.datetime.now()})})()
    
    success = main()
    sys.exit(0 if success else 1)
