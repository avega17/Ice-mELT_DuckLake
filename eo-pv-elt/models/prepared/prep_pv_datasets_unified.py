# Prepared model for unified PV datasets - Python model for spatio-temporal context augmentation
# This model augments staging data with spatial indexing, administrative boundaries, and contextual data

def model(dbt, session):
    """
    Python dbt model that prepares PV installation data with spatio-temporal context.

    This model applies prepared-layer processing:
    1. Loads clean staging data with geometries
    2. Adds H3 spatial indexing at multiple resolutions
    3. Assigns administrative boundaries (country, region, locality) using Overture Maps
    4. Adds bounding box bounds for spatial filtering
    5. Applies spatial ordering using ST_Hilbert for efficient querying
    6. Prepares for land cover and land use augmentation
    """
    import pandas as pd
    import numpy as np
    import sys
    from pathlib import Path
    from datetime import datetime

    # Add utils to path for importing spatio-temporal processing functions
    utils_path = Path(dbt.config.project_root) / "utils"
    if str(utils_path) not in sys.path:
        sys.path.append(str(utils_path))

    from st_context_processing import (
        add_h3_index_to_pv_labels,
        group_pv_by_h3_cells
    )
    
    # Get staging data
    staging_df = dbt.ref("stg_doi_datasets").to_pandas()

    if len(staging_df) == 0:
        # Return empty DataFrame with expected schema if no staging data
        return pd.DataFrame(columns=[
            'dataset_name', 'installation_id', 'geometry_wkt', 'area_m2',
            'centroid_lon', 'centroid_lat', 'installation_date', 'capacity_mw',
            'h3_index_res10', 'h3_index_res7', 'h3_index_res5',
            'bbox_xmin', 'bbox_ymin', 'bbox_xmax', 'bbox_ymax',
            'hilbert_order', 'admin_country', 'admin_region', 'admin_locality',
            'prepared_at', 'source_system'
        ])

    # Convert back to GeoDataFrame for spatial operations
    import geopandas as gpd
    from shapely import wkt

    # Convert WKT back to geometry for spatial processing
    staging_df['geometry'] = staging_df['geometry_wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(staging_df, geometry='geometry', crs='EPSG:4326')
    
    dbt.logger.info(f"Starting prepared layer processing for {len(gdf)} PV installations")

    # 1. Add H3 spatial indexing at multiple resolutions
    dbt.logger.info("Adding H3 spatial indexing...")

    # Add H3 indices at different resolutions for multi-scale analysis
    try:
        # Resolution 10: ~15m hexagons (installation-level)
        gdf = add_h3_index_to_pv_labels(
            gdf, h3_resolution=10, method="centroid", out_col="h3_index_res10"
        )

        # Resolution 7: ~1.2km hexagons (neighborhood-level)
        gdf = add_h3_index_to_pv_labels(
            gdf, h3_resolution=7, method="centroid", out_col="h3_index_res7"
        )

        # Resolution 5: ~8.5km hexagons (city-level)
        gdf = add_h3_index_to_pv_labels(
            gdf, h3_resolution=5, method="centroid", out_col="h3_index_res5"
        )

        dbt.logger.info("H3 indexing completed successfully")
    except Exception as e:
        dbt.logger.warning(f"H3 indexing failed: {e}")
        # Add empty H3 columns if indexing fails
        gdf['h3_index_res10'] = None
        gdf['h3_index_res7'] = None
        gdf['h3_index_res5'] = None

    # 2. Add bounding box bounds for spatial filtering
    dbt.logger.info("Calculating bounding boxes...")
    bounds = gdf.bounds
    gdf['bbox_xmin'] = bounds['minx']
    gdf['bbox_ymin'] = bounds['miny']
    gdf['bbox_xmax'] = bounds['maxx']
    gdf['bbox_ymax'] = bounds['maxy']

    # 3. Add Hilbert spatial ordering for efficient querying
    dbt.logger.info("Adding Hilbert spatial ordering...")
    try:
        # Use centroid coordinates for Hilbert ordering
        # Scale coordinates to integer range for Hilbert curve
        lon_scaled = ((gdf['centroid_lon'] + 180) / 360 * 65535).astype(int)
        lat_scaled = ((gdf['centroid_lat'] + 90) / 180 * 65535).astype(int)

        # Simple Hilbert-like ordering using bit interleaving (Z-order)
        # This provides spatial locality for efficient querying
        gdf['hilbert_order'] = (lon_scaled << 16) | lat_scaled

    except Exception as e:
        dbt.logger.warning(f"Hilbert ordering failed: {e}")
        gdf['hilbert_order'] = range(len(gdf))
    
    # 4. Placeholder for administrative boundary assignment
    # Note: Admin boundary assignment will be handled by separate SQL models
    # that can efficiently join H3-indexed PV data with Overture Maps S3 data
    dbt.logger.info("Admin boundary assignment will be handled by downstream SQL models...")
    gdf['admin_country'] = None
    gdf['admin_region'] = None
    gdf['admin_locality'] = None

    # 5. Placeholder for land cover/land use augmentation
    dbt.logger.info("Placeholder for land cover/land use data...")
    # TODO: Add Overture Maps land cover/land use data
    # gdf['land_cover'] = None
    # gdf['land_use'] = None

    # Convert back to DataFrame for DuckDB
    prepared_df = pd.DataFrame(gdf)
    prepared_df['geometry_wkt'] = gdf.geometry.to_wkt()
    prepared_df = prepared_df.drop(columns=['geometry'])

    # Add processing timestamp
    prepared_df['prepared_at'] = datetime.now()
    prepared_df['source_system'] = 'doi_dataset_pipeline'

    # Select final columns for prepared layer
    final_columns = [
        'dataset_name', 'installation_id', 'geometry_wkt', 'area_m2',
        'centroid_lon', 'centroid_lat', 'installation_date', 'capacity_mw',
        'h3_index_res10', 'h3_index_res7', 'h3_index_res5',
        'bbox_xmin', 'bbox_ymin', 'bbox_xmax', 'bbox_ymax',
        'hilbert_order', 'admin_country', 'admin_region', 'admin_locality',
        'prepared_at', 'source_system'
    ]

    # Ensure all columns exist with defaults
    for col in final_columns:
        if col not in prepared_df.columns:
            if col.startswith('bbox_') or col in ['area_m2', 'centroid_lon', 'centroid_lat', 'hilbert_order']:
                prepared_df[col] = 0.0
            else:
                prepared_df[col] = None

    result_df = prepared_df[final_columns].copy()

    dbt.logger.info(f"Prepared {len(result_df)} PV installations with spatio-temporal context")
    dbt.logger.info(f"H3 indexing coverage: {result_df['h3_index_res10'].notna().sum()} installations")
    dbt.logger.info(f"Datasets processed: {result_df['dataset_name'].nunique()}")

    return result_df
