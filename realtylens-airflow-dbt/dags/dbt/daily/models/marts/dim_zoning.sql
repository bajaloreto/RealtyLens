{{
  config(
    materialized = 'table',
    unique_key = 'zoning_sk'
  )
}}

SELECT
  {{ dbt_utils.generate_surrogate_key(['ZONING_ID']) }} as zoning_sk,
  ZONING_ID,
  ZONING_CODE,
  ZONING_LONG_CODE,
  ZONING_GROUP,
  POLYGON_TYPE,
  POLYGON_COORDINATES,
  ZIP_CODE,
  LOAD_DATE
FROM {{ source('realtylens', 'lkp_zoning_polygon') }}