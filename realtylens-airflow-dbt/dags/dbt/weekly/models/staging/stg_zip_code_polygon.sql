-- models/stg_zipcode_polygon.sql

{{ config(
    materialized='view'  
) }}

WITH raw_data AS (
    SELECT
        OBJECTID AS zip_code_id,
        CODE AS zip_code,
        COD AS cod,
        Shape__Area AS shape_area,
        Shape__Length AS shape_length,
        geometry_type AS polygon_type,
        TO_GEOGRAPHY(geometry_json) AS polygon_coordinates,
        load_date
    FROM {{ source('realtylens', 'raw_zip_code_polygon') }} 
)

SELECT
    zip_code_id,
    zip_code,
    cod,
    shape_area,
    shape_length,
    polygon_type,
    polygon_coordinates,
    load_date
FROM raw_data
WHERE zip_code_id IS NOT NULL