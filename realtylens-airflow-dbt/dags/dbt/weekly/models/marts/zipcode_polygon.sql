-- models/stg_zipcode_polygon.sql

{{ config(
    materialized='table'  
) }}

SELECT
    *
FROM {{ ref('stg_zipcode_polygon') }} 
