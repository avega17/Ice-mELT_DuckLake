#!/bin/bash
# Set environment variables for PROD target

echo "🎯 Setting environment for PROD target..."

# Set target for Hamilton DAGs
export DBT_TARGET=prod

# Set active connection strings for dbt (direct values, not variable expansion)
export DUCKLAKE_CONNECTION_STRING="ducklake:postgres:dbname=ducklake_catalog host=ep-broad-rain-a4tdwnxn-pooler.us-east-1.aws.neon.tech user=neondb_owner password=npg_y0ViUzD1Xulc port=5432 sslmode=require (DATA_PATH 's3://eo_pv_lakehouse/ducklake_data')"
export GEOPARQUET_SOURCE_PATH="r2://eo-pv-lakehouse/geoparquet"

# Also set the specific PROD variables for compatibility
export GEOPARQUET_SOURCE_PATH_PROD="r2://eo-pv-lakehouse/geoparquet"
export DUCKLAKE_CONNECTION_STRING_PROD="ducklake:postgres:dbname=ducklake_catalog host=ep-broad-rain-a4tdwnxn-pooler.us-east-1.aws.neon.tech user=neondb_owner password=npg_y0ViUzD1Xulc port=5432 sslmode=require (DATA_PATH 's3://eo_pv_lakehouse/ducklake_data')"

echo "✅ PROD environment configured:"
echo "   DBT_TARGET: $DBT_TARGET"
echo "   DuckLake: PostgreSQL catalog + R2 storage"
echo "   Sources: R2 bucket GeoParquet files"
echo "   GEOPARQUET_SOURCE_PATH: $GEOPARQUET_SOURCE_PATH"
echo ""
echo "Now run: cd eo-pv-elt && dbt-ibis run --target prod"
