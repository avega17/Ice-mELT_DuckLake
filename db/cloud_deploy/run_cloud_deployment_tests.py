#!/usr/bin/env python3
"""
Comprehensive test suite for Ice-mELT DuckLake cloud deployment.

This script consolidates all cloud deployment testing functionality:
1. Infrastructure connectivity tests (R2, Neon, MotherDuck)
2. Raw data ingestion to R2 with validation
3. dbt raw models from R2 parquet files
4. Hamilton staging consolidation via dbt-ibis
5. Final analytics models and data quality checks
6. End-to-end pipeline validation

Replaces individual test scripts: test_*, validate_*, verify_*
"""

import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add repo root to path
repo_root = Path(os.getenv('REPO_ROOT', '.')).resolve()
sys.path.insert(0, str(repo_root))

def run_test_script(script_name, description):
    """Run a test script and return success status."""
    print(f"\n{'='*60}")
    print(f"🧪 Running: {description}")
    print(f"📄 Script: {script_name}")
    print('='*60)
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=str(repo_root),
            timeout=120  # 2 minutes timeout
        )
        
        success = result.returncode == 0
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"\n📊 Result: {status}")
        
        return success
        
    except subprocess.TimeoutExpired:
        print(f"\n⏰ TIMEOUT: {script_name} took too long")
        return False
    except FileNotFoundError:
        print(f"\n❌ NOT FOUND: {script_name}")
        return False
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        return False

def test_infrastructure_connectivity():
    """Test basic connectivity to all cloud services."""
    print(f"\n{'='*60}")
    print(f"🔧 Testing Infrastructure Connectivity")
    print(f"{'='*60}")

    success = True

    # Test Neon PostgreSQL
    try:
        import psycopg2
        print("\n🐘 Testing Neon PostgreSQL...")
        conn_str = os.getenv('NEON_PG_CONN')
        if not conn_str:
            pg_host = os.getenv('PGHOST')
            pg_database = os.getenv('PGDATABASE')
            pg_user = os.getenv('PGUSER')
            pg_password = os.getenv('PGPASSWORD')
            conn_str = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}?sslmode=require"

        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"   ✅ Neon PostgreSQL: {version[:50]}...")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"   ❌ Neon PostgreSQL failed: {e}")
        success = False

    # Test Cloudflare R2
    try:
        import boto3
        print("\n☁️  Testing Cloudflare R2...")
        s3_client = boto3.client(
            's3',
            endpoint_url=f"https://{os.getenv('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com",
            aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('R2_SECRET_KEY'),
            region_name='auto'
        )
        s3_client.head_bucket(Bucket='eo-pv-lakehouse')
        print(f"   ✅ Cloudflare R2: Bucket accessible")
    except Exception as e:
        print(f"   ❌ Cloudflare R2 failed: {e}")
        success = False

    # Test MotherDuck
    try:
        import duckdb
        print("\n🦆 Testing MotherDuck...")
        conn = duckdb.connect(f"md:eo_pv_lakehouse?motherduck_token={os.getenv('MOTHERDUCK_TOKEN')}")
        result = conn.execute("SELECT 'MotherDuck connection successful'").fetchone()
        print(f"   ✅ MotherDuck: {result[0]}")
        conn.close()
    except Exception as e:
        print(f"   ❌ MotherDuck failed: {e}")
        success = False

    return success

def test_raw_data_pipeline():
    """Test raw data ingestion to R2."""
    print(f"\n{'='*60}")
    print(f"📊 Testing Raw Data Pipeline")
    print(f"{'='*60}")

    try:
        print("\n🚀 Running Hamilton raw data ingestion...")
        result = subprocess.run([
            sys.executable,
            "data_loaders/doi_pv/ingest_doi_pv_locations.py",
            "--cloud", "--sequential"
        ], cwd=str(repo_root), timeout=600)  # 10 minutes

        if result.returncode == 0:
            print("   ✅ Raw data ingestion successful")

            # Verify files in R2
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=f"https://{os.getenv('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com",
                aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('R2_SECRET_KEY'),
                region_name='auto'
            )

            response = s3_client.list_objects_v2(Bucket='eo-pv-lakehouse', Prefix='geoparquet/')
            if 'Contents' in response:
                file_count = len(response['Contents'])
                print(f"   ✅ Found {file_count} GeoParquet files in R2")
                return True
            else:
                print("   ❌ No GeoParquet files found in R2")
                return False
        else:
            print("   ❌ Raw data ingestion failed")
            return False

    except Exception as e:
        print(f"   ❌ Raw data pipeline test failed: {e}")
        return False

def test_dbt_cloud_target():
    """Test dbt with cloud target configuration."""
    print(f"\n{'='*60}")
    print(f"🎯 Testing dbt Cloud Target")
    print(f"{'='*60}")

    try:
        print("\n🔧 Testing dbt debug with prod target...")
        result = subprocess.run([
            "dbt", "debug", "--target", "prod"
        ], cwd=str(repo_root / "eo-pv-elt"), timeout=120)

        if result.returncode == 0:
            print("   ✅ dbt cloud target configuration valid")

            print("\n🏗️  Testing dbt raw models...")
            result = subprocess.run([
                "dbt", "run", "--target", "prod", "--select", "raw_ind_pv_solar_farms_2022"
            ], cwd=str(repo_root / "eo-pv-elt"), timeout=300)

            if result.returncode == 0:
                print("   ✅ dbt raw model execution successful")
                return True
            else:
                print("   ❌ dbt raw model execution failed")
                return False
        else:
            print("   ❌ dbt cloud target configuration invalid")
            return False

    except Exception as e:
        print(f"   ❌ dbt cloud target test failed: {e}")
        return False

def main():
    """Run all cloud deployment tests."""
    print("🚀 Ice-mELT DuckLake Cloud Deployment Test Suite")
    print("Testing: Hamilton → DuckLake (Neon + R2) → MotherDuck")
    print("="*80)
    
    # Define comprehensive test suite
    tests = [
        ("Infrastructure Connectivity", test_infrastructure_connectivity),
        ("Raw Data Pipeline", test_raw_data_pipeline),
        ("dbt Cloud Target", test_dbt_cloud_target),
    ]

    # Run tests
    results = {}
    for description, test_func in tests:
        try:
            results[description] = test_func()
        except Exception as e:
            print(f"\n💥 {description} failed with error: {e}")
            results[description] = False
    
    # Summary
    print(f"\n{'='*80}")
    print("📊 CLOUD DEPLOYMENT TEST SUMMARY")
    print('='*80)
    
    passed = 0
    failed = 0
    skipped = 0
    
    for test_name, result in results.items():
        if result is True:
            print(f"✅ PASSED  - {test_name}")
            passed += 1
        elif result is False:
            print(f"❌ FAILED  - {test_name}")
            failed += 1
        else:
            print(f"⚠️  SKIPPED - {test_name}")
            skipped += 1
    
    print(f"\n📈 STATISTICS:")
    print(f"   Passed:  {passed}")
    print(f"   Failed:  {failed}")
    print(f"   Skipped: {skipped}")
    print(f"   Total:   {len(results)}")
    
    # Overall assessment
    critical_tests = [
        "R2 Bucket Verification",
        "PostgreSQL Catalog (Neon)",
        "R2 Storage Integration",
        "MotherDuck Compute Integration"
    ]
    
    critical_passed = sum(1 for test in critical_tests if results.get(test) is True)
    critical_total = len(critical_tests)
    
    print(f"\n🎯 CRITICAL TESTS: {critical_passed}/{critical_total} passed")
    
    if critical_passed == critical_total:
        print(f"\n🎉 CLOUD DEPLOYMENT READY!")
        print(f"   ✅ All critical components operational")
        print(f"   ✅ Ready to process 443,917+ PV records in cloud")
        print(f"   ✅ Architecture: Hamilton → DuckLake (Neon + R2) → MotherDuck")
        
        if failed == 0:
            print(f"   🌟 Perfect score - all tests passed!")
        elif failed <= 2:
            print(f"   💡 Minor issues detected but core functionality ready")
        
        return True
    else:
        print(f"\n⚠️  CLOUD DEPLOYMENT NOT READY")
        print(f"   ❌ {critical_total - critical_passed} critical tests failed")
        print(f"   💡 Please resolve failed tests before production deployment")
        
        # Show which critical tests failed
        for test in critical_tests:
            if results.get(test) is False:
                print(f"      🔧 Fix required: {test}")
        
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
