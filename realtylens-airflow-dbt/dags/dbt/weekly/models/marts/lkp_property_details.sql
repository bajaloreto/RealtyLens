-- models/marts/enriched_property_zoning.sql

{{ config(
    materialized='table'
) }}

WITH property AS (
    SELECT *
    FROM {{ ref('stg_property_details') }}  -- Reference the property staging model
),

zoning AS (
    SELECT
        zoning_id,
        zoning_code,
        zoning_long_code,
        zoning_group,
        polygon_coordinates  -- Assuming you have polygon_coordinates in zoning details
    FROM {{ ref('stg_zoning_polygon') }}  -- Reference the zoning staging model
)
, combined_data as (
SELECT
    p.*,  -- Select all columns from the property details
    z.zoning_code,
    z.zoning_long_code,
    z.zoning_group,
    z.zoning_id,
    z.polygon_coordinates,
    row_number() over (partition by p.property_id order by z.zoning_id desc) as rn
FROM property p
LEFT JOIN zoning z ON ST_Contains(z.polygon_coordinates, ST_MakePoint(p.longitude, p.latitude))  -- Spatial join using latitude and longitude
)

SELECT * FROM combined_data WHERE rn = 1