# DuckLake Maintenance Utilities

A comprehensive CLI tool for maintaining DuckLake catalogs with snapshot management, file cleanup, and storage optimization.

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

- `DUCKLAKE_CATALOG_PATH`: Default catalog path
- `DUCKLAKE_DATA_PATH`: Default data directory path

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

## License

This utility is part of the Ice-mELT DuckLake project.
