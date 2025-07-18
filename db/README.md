# DuckLake Database Infrastructure

A unified database architecture for Ice-mELT DuckLake supporting seamless development-to-production workflows with ephemeral branching, hybrid compute, and intelligent query routing.

## Architecture Overview

### Unified PostgreSQL Catalog Strategy

**Single Source of Truth**: Both development and production environments use the same PostgreSQL-based DuckLake catalog structure, ensuring environment parity and eliminating configuration drift.

**Development Environment (Neon Local)**:
- **Ephemeral PostgreSQL branches** created from production data
- **Automatic cleanup** when development session ends
- **Safe experimentation** without affecting production catalog
- **Local DuckDB compute** for fast iteration and zero cloud costs

**Production Environment (Neon Cloud)**:
- **Serverless PostgreSQL** with connection pooling and multi-user access
- **MotherDuck hybrid compute** with intelligent local/cloud query routing
- **Cloudflare R2 storage** for zero-egress data access
- **Same dbt models** work identically across environments

### Key Benefits

✅ **Environment Parity**: Identical catalog structure, same models, same data
✅ **Safe Development**: Ephemeral branches prevent production impact
✅ **Cost Optimization**: Free tiers for research, pay-as-you-scale for production
✅ **Hybrid Processing**: Intelligent query routing between local and cloud compute
✅ **Zero Configuration Drift**: Single catalog schema across all environments

## DuckLake Maintenance Utilities

Comprehensive CLI tools for maintaining DuckLake catalogs with snapshot management, file cleanup, and storage optimization across both development and production environments.

## Features

- 📅 **Snapshot Management**: List, expire, and manage DuckLake snapshots
- 🧹 **File Cleanup**: Remove old and unreferenced files
- ⚡ **Storage Optimization**: Merge adjacent files for better performance
- 🔧 **Full Maintenance**: One-command maintenance with all operations
- 🔍 **Dry Run Mode**: Preview changes before applying them
- 📊 **Detailed Reporting**: Verbose output and progress tracking

## Installation

The utility requires Python 3.8+ and the following dependencies:

```bash
pip install click duckdb
```

## Quick Start

### Basic Usage

```bash
# Make the script executable
chmod +x ducklake_maintenance.py

# List all snapshots
python ducklake_maintenance.py snapshots list

# Expire snapshots older than 7 days
python ducklake_maintenance.py snapshots expire --days 7

# Clean up old files
python ducklake_maintenance.py cleanup files

# Optimize storage by merging files
python ducklake_maintenance.py optimize merge

# Full maintenance (all operations)
python ducklake_maintenance.py full-maintenance --days 7
```

### Advanced Usage

```bash
# Use custom catalog and data paths
python ducklake_maintenance.py --catalog /path/to/catalog.sqlite --data-path /path/to/data snapshots list

# Dry run to see what would be changed
python ducklake_maintenance.py snapshots expire --days 30 --dry-run
python ducklake_maintenance.py cleanup files --dry-run
python ducklake_maintenance.py full-maintenance --days 7 --dry-run

# Verbose output
python ducklake_maintenance.py --verbose snapshots list

# Optimize specific table
python ducklake_maintenance.py optimize merge --table my_table
```

## Commands

### Snapshot Management

#### `snapshots list`
Lists all snapshots in the catalog with timestamps and metadata.

```bash
python ducklake_maintenance.py snapshots list
```

#### `snapshots expire`
Expires snapshots older than specified days.

```bash
# Expire snapshots older than 7 days
python ducklake_maintenance.py snapshots expire --days 7

# Dry run to see what would be expired
python ducklake_maintenance.py snapshots expire --days 7 --dry-run
```

### File Cleanup

#### `cleanup files`
Removes old files that are no longer referenced by any snapshot.

```bash
# Clean up old files
python ducklake_maintenance.py cleanup files

# Dry run to see what would be cleaned
python ducklake_maintenance.py cleanup files --dry-run
```

### Storage Optimization

#### `optimize merge`
Merges adjacent files to improve query performance and reduce storage overhead.

```bash
# Optimize all tables
python ducklake_maintenance.py optimize merge

# Optimize specific table
python ducklake_maintenance.py optimize merge --table my_table
```

### Full Maintenance

#### `full-maintenance`
Performs all maintenance operations in sequence: expire snapshots, cleanup files, and optimize storage.

```bash
# Full maintenance with default settings (7 days)
python ducklake_maintenance.py full-maintenance

# Custom expiration period
python ducklake_maintenance.py full-maintenance --days 14

# Dry run to see what would be done
python ducklake_maintenance.py full-maintenance --days 7 --dry-run
```

## Configuration

### Environment Variables

**Unified DuckLake Catalog Configuration**:
- `DUCKLAKE_CONNECTION_STRING`: PostgreSQL connection for DuckLake catalog (dev: Neon Local, prod: Neon Cloud)
- `DUCKLAKE_DATA_PATH`: Data storage path (dev: local files, prod: R2 bucket)

**Neon Local Ephemeral Branches (Development)**:
- `NEON_API_KEY`: Neon API key for creating ephemeral branches
- `NEON_PROJECT_ID`: Neon project ID for branch management
- `NEON_PARENT_BRANCH`: Parent branch ID for ephemeral branches (default: production)

**Cloud Storage and Compute (Production)**:
- `MOTHERDUCK_TOKEN`: MotherDuck token for hybrid query processing
- `R2_ACCESS_KEY_ID`: Cloudflare R2 access key
- `R2_SECRET_ACCESS_KEY`: Cloudflare R2 secret key
- `R2_BUCKET_NAME`: R2 bucket name for data storage

**Environment Targeting**:
- `DBT_TARGET`: Environment target (dev/prod) for dbt model execution

### Configuration File

You can use `ducklake_config.json` to store default settings:

```json
{
  "catalog_path": "ducklake_catalog.sqlite",
  "data_path": "ducklake_data",
  "maintenance": {
    "default_expire_days": 7,
    "auto_cleanup": true,
    "auto_optimize": true
  },
  "cloud": {
    "catalog_connection": "postgresql://user:pass@host/db",
    "data_path": "r2://bucket-name/",
    "enable_cloud_maintenance": true
  }
}
```

## Best Practices

### Regular Maintenance Schedule

1. **Daily**: Expire old snapshots (7-14 days)
2. **Weekly**: Clean up old files
3. **Monthly**: Optimize storage with file merging

### Recommended Workflow

```bash
# 1. Check what would be changed (dry run)
python ducklake_maintenance.py full-maintenance --days 7 --dry-run

# 2. If satisfied, run actual maintenance
python ducklake_maintenance.py full-maintenance --days 7

# 3. Monitor output for any issues
python ducklake_maintenance.py --verbose snapshots list
```

### Safety Tips

- Always use `--dry-run` first to preview changes
- Keep at least 2-3 recent snapshots for rollback capability
- Monitor disk space before and after cleanup operations
- Test maintenance operations on non-production catalogs first

## Automation

### Cron Job Example

Add to your crontab for automated maintenance:

```bash
# Daily maintenance at 2 AM
0 2 * * * cd /path/to/ducklake && python ducklake_maintenance.py full-maintenance --days 7

# Weekly optimization on Sundays at 3 AM
0 3 * * 0 cd /path/to/ducklake && python ducklake_maintenance.py optimize merge
```

### Systemd Timer Example

Create a systemd service and timer for automated maintenance:

```ini
# /etc/systemd/system/ducklake-maintenance.service
[Unit]
Description=DuckLake Maintenance
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/path/to/ducklake
ExecStart=/usr/bin/python3 ducklake_maintenance.py full-maintenance --days 7
User=ducklake
Group=ducklake

# /etc/systemd/system/ducklake-maintenance.timer
[Unit]
Description=Run DuckLake maintenance daily
Requires=ducklake-maintenance.service

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

## Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure DuckDB and ducklake extension are installed
2. **Permission Errors**: Check file permissions on catalog and data directories
3. **Lock Errors**: Ensure no other processes are using the catalog

### Debug Mode

Use verbose mode for detailed output:

```bash
python ducklake_maintenance.py --verbose full-maintenance --days 7
```

## Contributing

Feel free to submit issues and enhancement requests!

## Integration with Ice-mELT Pipeline

This unified database infrastructure supports the complete Ice-mELT DuckLake ELT pipeline:

### Development Workflow
- **Neon Local**: Ephemeral PostgreSQL branches from production data
- **Local DuckDB**: Fast analytical processing with full spatial extensions
- **Docker**: Containerized Neon Local proxy with automatic branch cleanup
- **Environment Scripts**: Simple `source db/env_scripts/dev.fish` activation

### Production Workflow
- **Neon PostgreSQL**: Serverless catalog with connection pooling
- **MotherDuck**: Hybrid query processing with intelligent routing
- **Cloudflare R2**: Zero-egress object storage for data lakehouse
- **Same dbt Models**: Identical transformations across environments

### Pipeline Integration
- **dbt Models**: Unified SQL models work across dev and prod environments
- **Hamilton DAGs**: Standardized data processing with environment-aware configuration
- **Maintenance Tools**: Automated snapshot management and storage optimization
- **Spatial Processing**: H3 indexing and geometry operations with consistent schemas

## License

This utility is part of the Ice-mELT DuckLake project.
