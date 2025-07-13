#!/bin/bash
# Set environment variables for DEV target

echo "🎯 Setting environment for DEV target..."

# Set target for Hamilton DAGs
export DBT_TARGET=dev

# Set active connection strings for dbt (direct values, not variable expansion)
export DUCKLAKE_CONNECTION_STRING="ducklake:sqlite:db/ducklake_catalog.sqlite (DATA_PATH '/Users/alejandovega/ice-mELT_ducklake/db/ducklake_data')"
export GEOPARQUET_SOURCE_PATH="/Users/alejandovega/ice-mELT_ducklake/db/geoparquet"

# Also set the specific DEV variables for compatibility
export GEOPARQUET_SOURCE_PATH_DEV="/Users/alejandovega/ice-mELT_ducklake/db/geoparquet"
export DUCKLAKE_CONNECTION_STRING_DEV="ducklake:sqlite:db/ducklake_catalog.sqlite (DATA_PATH '/Users/alejandovega/ice-mELT_ducklake/db/ducklake_data')"

# Set H3 configuration for consistent spatial indexing
export H3_DEDUP_RES="12"
export OVERLAP_THRESHOLD="0.5"

echo "✅ DEV environment configured:"
echo "   DBT_TARGET: $DBT_TARGET"
echo "   DuckLake: Local SQLite catalog"
echo "   Sources: Local GeoParquet files"
echo "   GEOPARQUET_SOURCE_PATH: $GEOPARQUET_SOURCE_PATH"
echo ""
echo "Now run: cd eo-pv-elt && dbt-ibis run --target dev"
