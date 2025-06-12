# Staging model for PV features - Python model for geopandas preprocessing
# This model loads the raw PV features from dlt and applies staging transformations

def model(dbt, session):
    """
    Staging model for PV features - focuses on data filtering and standardization.

    This model applies staging-layer processing:
    1. Loads raw PV features from dlt pipeline
    2. Converts WKT geometry back to geopandas geometries
    3. Removes invalid geometries and applies spatial indexing
    4. Calculates centroids and areas for features that need them
    5. Standardizes column names across datasets
    6. Returns clean, filtered geospatial data ready for prepared layer
    """
    import pandas as pd
    import geopandas as gpd
    from shapely import wkt
    import sys
    from pathlib import Path

    # Add utils to path for importing preprocessing functions
    utils_path = Path(dbt.config.project_root) / "utils"
    if str(utils_path) not in sys.path:
        sys.path.append(str(utils_path))

    def standardize_columns(gdf):
        """
        Standardize column names and filter relevant columns for staging.
        Keeps relevant metadata but standardizes naming across datasets.
        """
        # Define column mapping for standardization
        column_mappings = {
            # Common ID fields
            'polygon_id': 'installation_id',
            'sol_id': 'installation_id',
            'fid': 'installation_id',
            'unique_id': 'installation_id',
            'id': 'installation_id',

            # Area fields
            'area_meters': 'area_m2',
            'area_pixels': 'area_pixels',
            'panel.area': 'panel_area_m2',
            'landscape.area': 'landscape_area_m2',
            'Area': 'area_m2',

            # Location fields
            'centroid_latitude': 'centroid_lat',
            'centroid_longitude': 'centroid_lon',
            'Latitude': 'centroid_lat',
            'Longitude': 'centroid_lon',

            # Temporal fields
            'install_date': 'installation_date',
            'Date': 'installation_date',

            # Capacity fields
            'capacity_mw': 'capacity_mw',
            'power': 'capacity_mw',

            # Administrative fields
            'State': 'admin_state',
            'city': 'admin_city',
            'Country': 'admin_country',
            'Province': 'admin_province',

            # Quality/confidence fields
            'confidence': 'confidence_score',
            'jaccard_index': 'quality_score'
        }

        # Apply column renaming
        gdf_renamed = gdf.rename(columns=column_mappings)

        # Define columns to keep (relevant metadata)
        columns_to_keep = [
            'installation_id', 'area_m2', 'centroid_lat', 'centroid_lon',
            'installation_date', 'capacity_mw', 'confidence_score', 'quality_score',
            'admin_state', 'admin_city', 'admin_country', 'admin_province',
            'panel_area_m2', 'landscape_area_m2', 'geometry'
        ]

        # Keep only existing columns from our list
        available_columns = [col for col in columns_to_keep if col in gdf_renamed.columns]

        # Always keep geometry
        if 'geometry' not in available_columns:
            available_columns.append('geometry')

        return gdf_renamed[available_columns].copy()

    # Get metadata from the source table
    metadata_df = dbt.ref("doi_datasets").to_pandas()

    # Initialize list to collect all processed datasets
    all_datasets = []
    
    for _, row in metadata_df.iterrows():
        dataset_name = row['dataset_name']
        output_folder = row.get('output_folder', '')
        
        # Skip if no output folder (dataset not downloaded)
        if not output_folder or not os.path.exists(output_folder):
            dbt.logger.info(f"Skipping {dataset_name} - no data folder found")
            continue
            
        try:
            # Find vector files in the dataset folder
            vector_extensions = ['.geojson', '.json', '.shp', '.gpkg']
            geom_files = []
            
            for root, _, files in os.walk(output_folder):
                for file in files:
                    if any(file.lower().endswith(ext) for ext in vector_extensions):
                        geom_files.append(os.path.join(root, file))
            
            if not geom_files:
                dbt.logger.warning(f"No vector files found for {dataset_name}")
                continue
                
            dbt.logger.info(f"Processing {dataset_name} with {len(geom_files)} files")
            
            # Process the vector geometries with staging-specific settings
            gdf = process_vector_geoms(
                geom_files=geom_files,
                dataset_name=dataset_name,
                output_dir=None,  # Don't write files, just return GDF
                subset_bbox=None,
                geom_type=row.get('geometry_type', 'Polygon'),
                rm_invalid=True,  # Remove invalid geometries
                dedup_geoms=True,  # Remove overlapping duplicates
                overlap_thresh=0.75,  # 75% overlap threshold for duplicates
                out_fmt='geoparquet'
            )

            if gdf is None or len(gdf) == 0:
                dbt.logger.warning(f"No valid geometries found for {dataset_name}")
                continue

            # Apply additional staging-specific filtering
            initial_count = len(gdf)

            # Ensure spatial index is created for efficient operations
            if not hasattr(gdf, 'sindex') or gdf.sindex is None:
                gdf = gdf.reset_index(drop=True)  # Ensure clean index for spatial operations

            # Additional duplicate filtering if not already applied
            if len(gdf) > 1 and 'geometry' in gdf.columns:
                gdf = filter_gdf_duplicates(gdf, geom_type='Polygon', overlap_thresh=0.75)

            dbt.logger.info(f"Filtered {initial_count - len(gdf)} duplicate/invalid geometries from {dataset_name}")
            
            # Standardize column names and filter relevant columns
            standardized_gdf = standardize_columns(gdf)

            # Add dataset metadata to each row
            standardized_gdf['dataset_name'] = dataset_name
            standardized_gdf['doi'] = row['doi']
            standardized_gdf['repository_type'] = row['repo']
            standardized_gdf['label_format'] = row['label_format']
            standardized_gdf['source_geometry_type'] = row.get('geometry_type', 'Unknown')
            standardized_gdf['source_crs'] = row.get('crs', 'Unknown')

            # Ensure we have required spatial columns
            required_columns = {
                'area_m2': 0.0,
                'centroid_lon': 0.0,
                'centroid_lat': 0.0
            }

            for col, default_val in required_columns.items():
                if col not in standardized_gdf.columns:
                    standardized_gdf[col] = default_val
            
            # Convert to standard DataFrame for DuckDB (geometry as WKT)
            df = pd.DataFrame(standardized_gdf)
            df['geometry_wkt'] = standardized_gdf.geometry.to_wkt()
            df = df.drop(columns=['geometry'])  # Remove geopandas geometry column

            all_datasets.append(df)
            dbt.logger.info(f"Successfully processed {len(df)} features from {dataset_name}")
            dbt.logger.info(f"Columns retained: {list(df.columns)}")
            
        except Exception as e:
            dbt.logger.error(f"Error processing {dataset_name}: {str(e)}")
            continue
    
    if not all_datasets:
        # Return empty DataFrame with expected schema if no data processed
        return pd.DataFrame(columns=[
            'dataset_name', 'doi', 'repository_type', 'label_format',
            'source_geometry_type', 'source_crs', 'source_label_count',
            'area_m2', 'centroid_lon', 'centroid_lat', 'geometry_wkt'
        ])
    
    # Combine all datasets
    combined_df = pd.concat(all_datasets, ignore_index=True)
    
    # Add processing metadata
    combined_df['processed_at'] = pd.Timestamp.now()
    combined_df['source_system'] = 'doi_dataset_pipeline'
    
    dbt.logger.info(f"Combined {len(combined_df)} total PV installations from {len(all_datasets)} datasets")
    
    return combined_df
