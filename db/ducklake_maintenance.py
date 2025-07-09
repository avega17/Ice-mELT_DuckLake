#!/usr/bin/env python3
"""
DuckLake Maintenance CLI

A comprehensive command-line utility for maintaining DuckLake catalogs.
Provides tools for snapshot management, file cleanup, and optimization.

Usage:
    python ducklake_maintenance.py --help
    python ducklake_maintenance.py snapshots --help
    python ducklake_maintenance.py cleanup --help
    python ducklake_maintenance.py optimize --help

Examples:
    # List all snapshots
    python ducklake_maintenance.py snapshots list

    # Expire snapshots older than 7 days
    python ducklake_maintenance.py snapshots expire --days 7

    # Clean up old files
    python ducklake_maintenance.py cleanup files

    # Optimize by merging adjacent files
    python ducklake_maintenance.py optimize merge

    # Full maintenance (expire + cleanup + optimize)
    python ducklake_maintenance.py full-maintenance --days 7
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import click
import duckdb


# Configuration constants
DEFAULT_CATALOG_PATH = "ducklake_catalog.sqlite"
DEFAULT_DATA_PATH = "ducklake_data"


class DuckLakeManager:
    """Manages DuckLake catalog operations."""
    
    def __init__(self, catalog_path: str, data_path: str):
        self.catalog_path = catalog_path
        self.data_path = data_path
        self.connection = None
    
    def connect(self):
        """Establish connection to DuckLake catalog."""
        try:
            self.connection = duckdb.connect()
            # Install required extensions
            self.connection.execute("INSTALL ducklake; LOAD ducklake;")
            self.connection.execute("INSTALL spatial; LOAD spatial;")
            
            # Attach DuckLake catalog
            attach_sql = f"ATTACH 'ducklake:sqlite:{self.catalog_path}' AS ducklake_catalog (DATA_PATH '{self.data_path}/')"
            self.connection.execute(attach_sql)
            self.connection.execute("USE ducklake_catalog")
            
            click.echo(f"✅ Connected to DuckLake catalog: {self.catalog_path}")
            return True
        except Exception as e:
            click.echo(f"❌ Failed to connect to DuckLake: {e}", err=True)
            return False
    
    def disconnect(self):
        """Close DuckLake connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def list_snapshots(self):
        """List all snapshots in the catalog."""
        try:
            result = self.connection.execute("SELECT * FROM ducklake_snapshots() ORDER BY snapshot_id DESC").fetchall()
            return result
        except Exception as e:
            click.echo(f"❌ Error listing snapshots: {e}", err=True)
            return []
    
    def expire_snapshots(self, days: int, dry_run: bool = False):
        """Expire snapshots older than specified days."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            cutoff_timestamp = int(cutoff_date.timestamp() * 1000)  # Convert to milliseconds
            
            if dry_run:
                # Show what would be expired
                result = self.connection.execute(
                    "SELECT * FROM ducklake_snapshots() WHERE timestamp_ms < ? ORDER BY snapshot_id",
                    [cutoff_timestamp]
                ).fetchall()
                return result
            else:
                # Actually expire snapshots
                self.connection.execute(f"CALL ducklake_expire_snapshots('{cutoff_timestamp}')")
                click.echo(f"✅ Expired snapshots older than {days} days")
                return True
        except Exception as e:
            click.echo(f"❌ Error expiring snapshots: {e}", err=True)
            return False
    
    def cleanup_old_files(self, dry_run: bool = False):
        """Clean up old files that are no longer referenced."""
        try:
            if dry_run:
                # Show what would be cleaned up
                result = self.connection.execute("SELECT * FROM ducklake_cleanup_old_files(dry_run => true)").fetchall()
                return result
            else:
                # Actually clean up files
                result = self.connection.execute("CALL ducklake_cleanup_old_files()").fetchall()
                click.echo("✅ Cleaned up old files")
                return result
        except Exception as e:
            click.echo(f"❌ Error cleaning up files: {e}", err=True)
            return False
    
    def merge_adjacent_files(self, table_name: Optional[str] = None):
        """Merge adjacent files to optimize storage."""
        try:
            if table_name:
                self.connection.execute(f"CALL ducklake_merge_adjacent_files('{table_name}')")
                click.echo(f"✅ Merged adjacent files for table: {table_name}")
            else:
                # Get all tables and merge each one
                tables = self.connection.execute("SHOW TABLES").fetchall()
                for table in tables:
                    table_name = table[0]
                    self.connection.execute(f"CALL ducklake_merge_adjacent_files('{table_name}')")
                    click.echo(f"✅ Merged adjacent files for table: {table_name}")
            return True
        except Exception as e:
            click.echo(f"❌ Error merging files: {e}", err=True)
            return False


# CLI Context class for sharing state
class CLIContext:
    def __init__(self):
        self.catalog_path = None
        self.data_path = None
        self.manager = None
        self.verbose = False


@click.group()
@click.option('--catalog', '-c', 
              default=DEFAULT_CATALOG_PATH,
              help=f'Path to DuckLake catalog file (default: {DEFAULT_CATALOG_PATH})')
@click.option('--data-path', '-d',
              default=DEFAULT_DATA_PATH, 
              help=f'Path to DuckLake data directory (default: {DEFAULT_DATA_PATH})')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.pass_context
def cli(ctx, catalog, data_path, verbose):
    """
    DuckLake Maintenance CLI
    
    A comprehensive tool for maintaining DuckLake catalogs including
    snapshot management, file cleanup, and storage optimization.
    """
    # Ensure context object exists
    ctx.ensure_object(CLIContext)
    
    # Store configuration in context
    ctx.obj.catalog_path = catalog
    ctx.obj.data_path = data_path
    ctx.obj.verbose = verbose
    
    # Create manager instance
    ctx.obj.manager = DuckLakeManager(catalog, data_path)
    
    if verbose:
        click.echo(f"📁 Catalog: {catalog}")
        click.echo(f"📂 Data path: {data_path}")


@cli.group()
def snapshots():
    """Manage DuckLake snapshots."""
    pass


@snapshots.command('list')
@click.pass_context
def list_snapshots(ctx):
    """List all snapshots in the catalog."""
    manager = ctx.obj.manager
    
    if not manager.connect():
        sys.exit(1)
    
    try:
        snapshots = manager.list_snapshots()
        
        if not snapshots:
            click.echo("📋 No snapshots found")
            return
        
        click.echo("📋 DuckLake Snapshots:")
        click.echo("-" * 80)
        click.echo(f"{'ID':<10} {'Timestamp':<20} {'Parent ID':<12} {'Summary'}")
        click.echo("-" * 80)
        
        for snapshot in snapshots:
            snapshot_id = snapshot[0]
            timestamp_ms = snapshot[1]
            parent_id = snapshot[2] if snapshot[2] else "None"
            summary = snapshot[3] if len(snapshot) > 3 else ""
            
            # Convert timestamp to readable format
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            click.echo(f"{snapshot_id:<10} {timestamp:<20} {parent_id:<12} {summary}")
            
    finally:
        manager.disconnect()


@snapshots.command('expire')
@click.option('--days', '-d', type=int, required=True,
              help='Expire snapshots older than this many days')
@click.option('--dry-run', is_flag=True, 
              help='Show what would be expired without actually doing it')
@click.pass_context
def expire_snapshots(ctx, days, dry_run):
    """Expire snapshots older than specified days."""
    manager = ctx.obj.manager
    
    if not manager.connect():
        sys.exit(1)
    
    try:
        if dry_run:
            click.echo(f"🔍 Dry run: Finding snapshots older than {days} days...")
            snapshots = manager.expire_snapshots(days, dry_run=True)
            
            if not snapshots:
                click.echo("✅ No snapshots would be expired")
                return
            
            click.echo(f"⚠️  Would expire {len(snapshots)} snapshots:")
            for snapshot in snapshots:
                snapshot_id = snapshot[0]
                timestamp_ms = snapshot[1]
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
                click.echo(f"  - Snapshot {snapshot_id} from {timestamp}")
        else:
            click.echo(f"🗑️  Expiring snapshots older than {days} days...")
            if manager.expire_snapshots(days, dry_run=False):
                click.echo("✅ Snapshot expiration completed")
            
    finally:
        manager.disconnect()


@cli.group()
def cleanup():
    """Clean up old and unused files."""
    pass


@cleanup.command('files')
@click.option('--dry-run', is_flag=True,
              help='Show what would be cleaned without actually doing it')
@click.pass_context
def cleanup_files(ctx, dry_run):
    """Clean up old files that are no longer referenced."""
    manager = ctx.obj.manager
    
    if not manager.connect():
        sys.exit(1)
    
    try:
        if dry_run:
            click.echo("🔍 Dry run: Finding files that would be cleaned up...")
            files = manager.cleanup_old_files(dry_run=True)
            
            if not files:
                click.echo("✅ No files would be cleaned up")
                return
            
            click.echo(f"⚠️  Would clean up {len(files)} files:")
            for file_info in files:
                click.echo(f"  - {file_info}")
        else:
            click.echo("🧹 Cleaning up old files...")
            result = manager.cleanup_old_files(dry_run=False)
            if result:
                click.echo("✅ File cleanup completed")
            
    finally:
        manager.disconnect()


@cli.group()
def optimize():
    """Optimize DuckLake storage."""
    pass


@optimize.command('merge')
@click.option('--table', '-t', help='Specific table to optimize (default: all tables)')
@click.pass_context
def merge_files(ctx, table):
    """Merge adjacent files to optimize storage."""
    manager = ctx.obj.manager
    
    if not manager.connect():
        sys.exit(1)
    
    try:
        if table:
            click.echo(f"🔧 Merging adjacent files for table: {table}")
        else:
            click.echo("🔧 Merging adjacent files for all tables...")
        
        if manager.merge_adjacent_files(table):
            click.echo("✅ File merge optimization completed")
            
    finally:
        manager.disconnect()


@cli.command('full-maintenance')
@click.option('--days', '-d', type=int, default=7,
              help='Expire snapshots older than this many days (default: 7)')
@click.option('--dry-run', is_flag=True,
              help='Show what would be done without actually doing it')
@click.pass_context
def full_maintenance(ctx, days, dry_run):
    """
    Perform full maintenance: expire snapshots, cleanup files, and optimize storage.
    """
    manager = ctx.obj.manager
    
    if not manager.connect():
        sys.exit(1)
    
    try:
        click.echo("🔧 Starting full DuckLake maintenance...")
        
        # Step 1: Expire old snapshots
        click.echo(f"\n📅 Step 1: Expiring snapshots older than {days} days")
        if dry_run:
            snapshots = manager.expire_snapshots(days, dry_run=True)
            if snapshots:
                click.echo(f"  Would expire {len(snapshots)} snapshots")
            else:
                click.echo("  No snapshots would be expired")
        else:
            manager.expire_snapshots(days, dry_run=False)
        
        # Step 2: Clean up old files
        click.echo("\n🧹 Step 2: Cleaning up old files")
        if dry_run:
            files = manager.cleanup_old_files(dry_run=True)
            if files:
                click.echo(f"  Would clean up {len(files)} files")
            else:
                click.echo("  No files would be cleaned up")
        else:
            manager.cleanup_old_files(dry_run=False)
        
        # Step 3: Optimize storage
        click.echo("\n⚡ Step 3: Optimizing storage (merging adjacent files)")
        if not dry_run:
            manager.merge_adjacent_files()
        else:
            click.echo("  Would merge adjacent files for all tables")
        
        click.echo("\n✅ Full maintenance completed!")
        
    finally:
        manager.disconnect()


if __name__ == '__main__':
    cli()
