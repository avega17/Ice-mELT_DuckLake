SELECT
  *
FROM (
  SELECT
    *
  FROM (
    SELECT
      *
    FROM (
      SELECT
        *
      FROM (
        SELECT
          *
        FROM "eo_pv_lakehouse"."main"."stg_chn_med_res_pv_2024" AS "t4"
        UNION ALL
        SELECT
          *
        FROM "eo_pv_lakehouse"."main"."stg_global_harmonized_large_solar_farms_2020" AS "t5"
      ) AS "t6"
      UNION ALL
      SELECT
        *
      FROM "eo_pv_lakehouse"."main"."stg_global_pv_inventory_sent2_spot_2021" AS "t3"
    ) AS "t7"
    UNION ALL
    SELECT
      *
    FROM "eo_pv_lakehouse"."main"."stg_ind_pv_solar_farms_2022" AS "t2"
  ) AS "t8"
  UNION ALL
  SELECT
    *
  FROM "eo_pv_lakehouse"."main"."stg_uk_crowdsourced_pv_2020" AS "t1"
) AS "t9"
UNION ALL
SELECT
  *
FROM "eo_pv_lakehouse"."main"."stg_usa_cali_usgs_pv_2016" AS "t0"