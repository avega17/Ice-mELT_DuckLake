#!/usr/bin/env python3
"""
Hamilton-dbt Bridge for Automatic Source Updates

This module provides utilities to bridge Hamilton pipeline results with dbt source definitions,
enabling automatic source registration and metadata propagation from Hamilton to dbt.

Key Features:
- Automatic dbt sources.yml generation from Hamilton pipeline results
- Metadata propagation including Hamilton tags and lineage information
- Source freshness configuration based on Hamilton execution patterns
- Integration with dbt macros for seamless workflow automation

Usage:
    from hamilton_dbt_bridge import HamiltonDbtBridge
    
    bridge = HamiltonDbtBridge()
    bridge.update_sources_from_hamilton('doi_datasets')
"""

import yaml
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HamiltonDbtBridge:
    """
    Bridge between Hamilton pipelines and dbt source definitions.
    
    This class handles the automatic generation and updating of dbt source
    configurations based on Hamilton pipeline execution results.
    """
    
    def __init__(self, dbt_project_root: str = "."):
        """
        Initialize the Hamilton-dbt bridge.
        
        Args:
            dbt_project_root: Path to the dbt project root directory
        """
        self.dbt_root = Path(dbt_project_root)
        self.sources_dir = self.dbt_root / "sources"
        self.sources_dir.mkdir(exist_ok=True)
        
        # Ensure data_loaders is in Python path
        data_loaders_path = self.dbt_root / "data_loaders"
        if data_loaders_path.exists():
            import sys
            sys.path.insert(0, str(data_loaders_path.parent))
    
    def update_sources_from_hamilton(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Update dbt sources.yml from Hamilton pipeline results.
        
        Args:
            pipeline_name: Name of the Hamilton pipeline to process
            
        Returns:
            Dict containing the generated dbt source configuration
            
        Raises:
            ValueError: If pipeline_name is not recognized
            ImportError: If Hamilton pipeline module cannot be imported
        """
        logger.info(f"Updating dbt sources from Hamilton pipeline: {pipeline_name}")
        
        try:
            if pipeline_name == "doi_datasets":
                stored_tables = self._run_doi_pipeline()
                source_config = self._generate_doi_sources(stored_tables)
            elif pipeline_name == "overture_maps":
                # Future implementation for Overture Maps pipeline
                raise NotImplementedError("Overture Maps pipeline not yet implemented")
            elif pipeline_name == "solar_irradiance":
                # Future implementation for Solar Irradiance pipeline
                raise NotImplementedError("Solar Irradiance pipeline not yet implemented")
            else:
                raise ValueError(f"Unknown pipeline: {pipeline_name}")
            
            # Write updated sources.yml
            sources_file = self.sources_dir / f"{pipeline_name}_sources.yml"
            self._write_sources_file(sources_file, source_config)
            
            logger.info(f"Successfully updated sources file: {sources_file}")
            return source_config
            
        except Exception as e:
            logger.error(f"Failed to update sources from Hamilton pipeline {pipeline_name}: {e}")
            raise
    
    def _run_doi_pipeline(self) -> Dict[str, str]:
        """
        Execute the DOI datasets Hamilton pipeline.
        
        Returns:
            Dict mapping dataset names to table information
        """
        try:
            from data_loaders.raw_pv_doi_ingest import run_doi_pipeline
            logger.info("Executing Hamilton DOI pipeline...")
            
            # Run with conservative settings for source updates
            result = run_doi_pipeline(
                use_parallel=True,
                use_cache=True,
                force_download=False,
                max_mb=250
            )
            
            logger.info(f"Hamilton DOI pipeline completed successfully")
            return result
            
        except ImportError as e:
            logger.error(f"Cannot import Hamilton DOI pipeline: {e}")
            raise ImportError(
                "Hamilton DOI pipeline not found. Ensure raw_pv_doi_ingest.py is in data_loaders/"
            )
    
    def _generate_doi_sources(self, stored_tables: Dict[str, str]) -> Dict[str, Any]:
        """
        Generate dbt sources configuration for DOI datasets.
        
        Args:
            stored_tables: Dict mapping dataset names to table information
            
        Returns:
            Dict containing dbt sources configuration
        """
        current_time = datetime.now().isoformat()
        
        source_config = {
            'version': 2,
            'sources': [{
                'name': 'raw_data',
                'description': 'Raw datasets ingested via Hamilton DOI pipeline',
                'meta': {
                    'hamilton_generated': True,
                    'last_updated': current_time,
                    'pipeline': 'doi_datasets',
                    'hamilton_version': self._get_hamilton_version(),
                    'total_tables': len(stored_tables)
                },
                'tables': []
            }]
        }
        
        # Generate table configurations
        for dataset_name, table_info in stored_tables.items():
            table_config = {
                'name': dataset_name,
                'description': f'Raw {dataset_name} dataset from DOI source',
                'meta': {
                    'table_info': table_info,
                    'hamilton_tags': {
                        'data_source': 'doi',
                        'processing_stage': 'raw',
                        'data_type': 'arrow_table',
                        'hamilton_node_type': 'sink'
                    },
                    'generated_at': current_time
                },
                'freshness': {
                    'warn_after': {'count': 7, 'period': 'day'},
                    'error_after': {'count': 14, 'period': 'day'}
                },
                'loaded_at_field': 'created_at',
                'tests': [
                    {'not_null': {'column_name': 'geometry'}},
                    {'unique': {'column_name': 'id'}} if 'id' in table_info else None
                ]
            }
            
            # Remove None tests
            table_config['tests'] = [test for test in table_config['tests'] if test is not None]
            
            source_config['sources'][0]['tables'].append(table_config)
        
        return source_config
    
    def _write_sources_file(self, sources_file: Path, source_config: Dict[str, Any]) -> None:
        """
        Write dbt sources configuration to file.
        
        Args:
            sources_file: Path to the sources file
            source_config: dbt sources configuration to write
        """
        try:
            with open(sources_file, 'w') as f:
                yaml.dump(
                    source_config, 
                    f, 
                    default_flow_style=False,
                    sort_keys=False,
                    indent=2
                )
            logger.info(f"Sources file written successfully: {sources_file}")
            
        except Exception as e:
            logger.error(f"Failed to write sources file {sources_file}: {e}")
            raise
    
    def _get_hamilton_version(self) -> str:
        """Get Hamilton version for metadata."""
        try:
            import hamilton
            return hamilton.__version__
        except (ImportError, AttributeError):
            return "unknown"
    
    def validate_sources(self, pipeline_name: str) -> bool:
        """
        Validate that generated sources file is valid YAML and contains expected structure.
        
        Args:
            pipeline_name: Name of the pipeline to validate
            
        Returns:
            bool: True if sources file is valid, False otherwise
        """
        sources_file = self.sources_dir / f"{pipeline_name}_sources.yml"
        
        if not sources_file.exists():
            logger.error(f"Sources file does not exist: {sources_file}")
            return False
        
        try:
            with open(sources_file, 'r') as f:
                config = yaml.safe_load(f)
            
            # Basic validation
            if not isinstance(config, dict):
                logger.error("Sources file is not a valid YAML dictionary")
                return False
            
            if 'sources' not in config:
                logger.error("Sources file missing 'sources' key")
                return False
            
            if not isinstance(config['sources'], list):
                logger.error("Sources 'sources' is not a list")
                return False
            
            logger.info(f"Sources file validation passed: {sources_file}")
            return True
            
        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML in sources file {sources_file}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error validating sources file {sources_file}: {e}")
            return False


def main():
    """CLI interface for the Hamilton-dbt bridge."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Hamilton-dbt Bridge CLI")
    parser.add_argument(
        "pipeline", 
        choices=["doi_datasets", "overture_maps", "solar_irradiance"],
        help="Hamilton pipeline to process"
    )
    parser.add_argument(
        "--dbt-root", 
        default=".",
        help="Path to dbt project root (default: current directory)"
    )
    parser.add_argument(
        "--validate-only", 
        action="store_true",
        help="Only validate existing sources file, don't update"
    )
    
    args = parser.parse_args()
    
    bridge = HamiltonDbtBridge(args.dbt_root)
    
    if args.validate_only:
        is_valid = bridge.validate_sources(args.pipeline)
        exit(0 if is_valid else 1)
    else:
        try:
            result = bridge.update_sources_from_hamilton(args.pipeline)
            print(f"Successfully updated sources for {args.pipeline}")
            print(f"Generated {len(result['sources'][0]['tables'])} table definitions")
        except Exception as e:
            print(f"Error: {e}")
            exit(1)


if __name__ == "__main__":
    main()
