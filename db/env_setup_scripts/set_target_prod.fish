#!/usr/bin/env fish
# Set environment variables for PROD target (Fish shell version)

echo "🎯 Setting environment for PROD target (Fish shell)..."

# Set target for Hamilton DAGs
set -gx DBT_TARGET prod

# Set active connection strings for dbt (direct values, not variable expansion)
set -gx DUCKLAKE_CONNECTION_STRING "ducklake:postgres:dbname=ducklake_catalog host=ep-broad-rain-a4tdwnxn-pooler.us-east-1.aws.neon.tech user=neondb_owner password=npg_y0ViUzD1Xulc port=5432 sslmode=require"
set -gx GEOPARQUET_SOURCE_PATH "r2://eo-pv-lakehouse/geoparquet"

# Also set the specific PROD variables for compatibility
set -gx GEOPARQUET_SOURCE_PATH_PROD "r2://eo-pv-lakehouse/geoparquet"
set -gx DUCKLAKE_CONNECTION_STRING_PROD "ducklake:postgres:dbname=ducklake_catalog host=ep-broad-rain-a4tdwnxn-pooler.us-east-1.aws.neon.tech user=neondb_owner password=npg_y0ViUzD1Xulc port=5432 sslmode=require"

echo "✅ PROD environment configured:"
echo "   DBT_TARGET: $DBT_TARGET"
echo "   DuckLake: PostgreSQL catalog + R2 storage"
echo "   Sources: R2 bucket GeoParquet files"
echo "   GEOPARQUET_SOURCE_PATH: $GEOPARQUET_SOURCE_PATH"
echo ""
echo "Now run: cd eo-pv-elt && dbt run --target prod"
