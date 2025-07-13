#!/usr/bin/env fish
# Set environment variables for DEV target (Fish shell version)

echo "🎯 Setting environment for DEV target (Fish shell)..."

# Set target for Hamilton DAGs
set -gx DBT_TARGET dev

# Set active connection strings for dbt (direct values, not variable expansion)
set -gx DUCKLAKE_CONNECTION_STRING "ducklake:sqlite:/Users/alejandovega/ice-mELT_ducklake/db/ducklake_catalog.sqlite (DATA_PATH '/Users/alejandovega/ice-mELT_ducklake/db/ducklake_data')"
set -gx GEOPARQUET_SOURCE_PATH "/Users/alejandovega/ice-mELT_ducklake/db/geoparquet"

# Also set the specific DEV variables for compatibility
set -gx GEOPARQUET_SOURCE_PATH_DEV "/Users/alejandovega/ice-mELT_ducklake/db/geoparquet"
set -gx DUCKLAKE_CONNECTION_STRING_DEV "ducklake:sqlite:/Users/alejandovega/ice-mELT_ducklake/db/ducklake_catalog.sqlite (DATA_PATH '/Users/alejandovega/ice-mELT_ducklake/db/ducklake_data')"

# Set H3 configuration for consistent spatial indexing
set -gx H3_DEDUP_RES "12"
set -gx OVERLAP_THRESHOLD "0.5"

echo "✅ DEV environment configured:"
echo "   DBT_TARGET: $DBT_TARGET"
echo "   DuckLake: Local SQLite catalog"
echo "   Sources: Local GeoParquet files"
echo "   GEOPARQUET_SOURCE_PATH: $GEOPARQUET_SOURCE_PATH"
echo ""
echo "Now run: cd eo-pv-elt && dbt run --target dev"
