-- models/stg_landmark_polygon.sql

{{ config(
    materialized='view'
) }}

WITH raw_data AS (
    SELECT
        OBJECTID AS landmark_id,
        NAME AS landmark_name,
        ADDRESS AS address,
        FEAT_TYPE AS feat_type,
        SUB_TYPE AS sub_type,
        VANITY_NAME AS vanity_name,
        SECONDARY_NAME AS secondary_name,
        BLDG AS bldg,
        PARENT_NAME AS parent_name,
        PARENT_TYPE AS parent_type,
        ACREAGE AS acreage,
        PARENT_ACREAGE AS parent_acreage,
        Shape__Area AS shape_area,
        Shape__Length AS shape_length,
        geometry_type AS polygon_type,
        TO_GEOGRAPHY(geometry_json) AS polygon_coordinates,
        load_date
    FROM {{ source('realtylens', 'raw_landmark_polygon') }}
)

SELECT
    landmark_id,
    landmark_name,
    address,
    feat_type,
    sub_type,
    vanity_name,
    secondary_name,
    bldg,
    parent_name,
    parent_type,
    acreage,
    parent_acreage,
    shape_area,
    shape_length,
    polygon_type,
    polygon_coordinates,
    load_date
FROM raw_data
WHERE landmark_id IS NOT NULL