{{
    config(
        materialized='view',
        description='Consolidated staging table with unified schema and H3 spatial indexes of processed PV installation geometries from all DOI datasets'
    )
}}



SELECT * FROM {{ ref('stg_global_pv_inventory_sent2_spot_2021') }}
UNION ALL BY NAME
SELECT * FROM {{ ref('stg_usa_cali_usgs_pv_2016') }}
UNION ALL BY NAME
SELECT * FROM {{ ref('stg_uk_crowdsourced_pv_2020') }}
UNION ALL BY NAME
SELECT * FROM {{ ref('stg_chn_med_res_pv_2024') }}
UNION ALL BY NAME
SELECT * FROM {{ ref('stg_ind_pv_solar_farms_2022') }}
UNION ALL BY NAME
SELECT * FROM {{ ref('stg_global_harmonized_large_solar_farms_2020') }} 