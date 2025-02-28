   -- models/marts/enriched_zoning_with_zip.sql

   {{ config(
       materialized='table'
   ) }}

   WITH zoning AS (
       SELECT
           zoning_id,
           zoning_code,
           zoning_long_code,
           zoning_group,
           polygon_type,
           polygon_coordinates,
           load_date
       FROM {{ ref('stg_zoning_polygon') }}  -- Reference the zoning staging model
   ),

   zip_codes AS (
       SELECT
           zip_code,
           polygon_coordinates  -- Include geometry for the join
       FROM {{ ref('stg_zipcode_polygon') }}  -- Reference the zip code staging model
   )
   , combined_data AS (
   SELECT
       z.zoning_id,
       z.zoning_code,
       z.zoning_long_code,
       z.zoning_group,
       z.polygon_type,
       z.polygon_coordinates,
       z.load_date,
       zc.zip_code,
       row_number() over (partition by z.zoning_id order by zc.zip_code desc) as rn
   FROM zoning z
   LEFT JOIN zip_codes zc ON ST_Intersects(z.polygon_coordinates, zc.polygon_coordinates)  -- Assuming you have geometry to join on
   )

SELECT
    zoning_id,
    zoning_code,
    zoning_long_code,
    zoning_group,
    polygon_type,
    polygon_coordinates,
    load_date,
    zip_code
FROM combined_data
WHERE rn = 1