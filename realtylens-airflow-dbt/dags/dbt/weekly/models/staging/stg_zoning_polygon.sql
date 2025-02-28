-- models/stg_zoning_polygon.sql

{{ config(
    materialized='view'  
) }}

WITH raw_data AS (
    SELECT
        OBJECTID AS zoning_id,
        CODE AS zoning_code,
        LONG_CODE AS zoning_long_code,
        ZONINGGROUP AS zoning_group,
        geometry_type AS polygon_type,
        TO_GEOGRAPHY(geometry_json) AS polygon_coordinates,
        load_date
    FROM {{ source('jmusni07', 'raw_zoning_polygon') }} 
)

SELECT
    zoning_id,
    zoning_code,
    zoning_long_code,
    zoning_group,
    polygon_type,
    polygon_coordinates,
    load_date
FROM raw_data
WHERE zoning_id IS NOT NULL