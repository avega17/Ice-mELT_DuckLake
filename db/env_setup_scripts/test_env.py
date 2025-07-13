#!/usr/bin/env python3
"""Test environment variable setup for dbt models."""

import os

print("🔍 Environment Variable Test")
print("=" * 50)

# Check all relevant environment variables
env_vars = [
    'DBT_TARGET',
    'GEOPARQUET_SOURCE_PATH',
    'GEOPARQUET_SOURCE_PATH_DEV', 
    'GEOPARQUET_SOURCE_PATH_PROD',
    'DUCKLAKE_CONNECTION_STRING',
    'DUCKLAKE_CONNECTION_STRING_DEV',
    'DUCKLAKE_CONNECTION_STRING_PROD'
]

for var in env_vars:
    value = os.getenv(var)
    if value:
        print(f"✅ {var}: {value}")
    else:
        print(f"❌ {var}: NOT SET")

print("\n🔍 File Path Test")
print("=" * 50)

# Test the file path construction
dataset_name = "usa_cali_usgs_pv_2016"
target_name = os.getenv('DBT_TARGET', 'dev')
is_prod_target = target_name == 'prod'

if is_prod_target:
    # Use GEOPARQUET_SOURCE_PATH which should be set to the correct bucket
    geoparquet_source_path = os.getenv('GEOPARQUET_SOURCE_PATH')
    if geoparquet_source_path:
        file_path = f"{geoparquet_source_path}/raw_{dataset_name}.parquet"
        print(f"📊 PROD file path: {file_path}")
    else:
        # Fallback to DUCKLAKE_NAME
        ducklake_name = os.getenv('DUCKLAKE_NAME', 'eo_pv_lakehouse')
        file_path = f"s3://{ducklake_name}/geoparquet/raw_{dataset_name}.parquet"
        print(f"📊 PROD file path (fallback): {file_path}")
else:
    geoparquet_source_path = os.getenv('GEOPARQUET_SOURCE_PATH')
    if geoparquet_source_path:
        file_path = f"{geoparquet_source_path}/raw_{dataset_name}.parquet"
        print(f"📊 DEV file path: {file_path}")
        
        # Check if file exists
        if os.path.exists(file_path):
            print(f"✅ File exists: {file_path}")
        else:
            print(f"❌ File NOT found: {file_path}")
            
            # Check directory
            dir_path = os.path.dirname(file_path)
            if os.path.exists(dir_path):
                print(f"📁 Directory exists: {dir_path}")
                print(f"📋 Directory contents:")
                try:
                    for f in os.listdir(dir_path):
                        if f.endswith('.parquet'):
                            print(f"   - {f}")
                except Exception as e:
                    print(f"   Error listing directory: {e}")
            else:
                print(f"❌ Directory NOT found: {dir_path}")
    else:
        print("❌ GEOPARQUET_SOURCE_PATH not set")
