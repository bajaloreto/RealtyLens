{{
  config(
    materialized = 'table',
    unique_key = 'property_sk'
  )
}}

WITH listings AS (
  -- All properties from rent listings
  SELECT DISTINCT
    PROPERTY_ID,
    LOAD_DATE
  FROM {{ ref('stg_daily_rent_listing') }}
  
  UNION
  
  -- All properties from sale listings
  SELECT DISTINCT
    PROPERTY_ID,
    LOAD_DATE
  FROM {{ ref('stg_daily_sale_listing') }}
),

stg_property AS (
  SELECT
    p.PROPERTY_ID,
    p.FORMATTED_ADDRESS,
    p.ADDRESS_LINE1 as ADDRESS_LINE_1,
    p.ADDRESS_LINE2 as ADDRESS_LINE_2,
    p.CITY,
    p.STATE,
    p.ZIP_CODE,
    p.COUNTY,
    p.LATITUDE,
    p.LONGITUDE,
    p.PROPERTY_TYPE,
    p.BEDROOMS,
    p.BATHROOMS,
    p.SQUARE_FOOTAGE,
    p.LOT_SIZE,
    p.YEAR_BUILT,
    p.LAST_SALE_DATE,
    p.LAST_SALE_PRICE,
    p.LOAD_DATE,
    p.ZONING_CODE,
    p.ZONING_LONG_CODE,
    p.ZONING_GROUP,
    p.ZONING_ID
  FROM {{ source('realtylens', 'lkp_property_details') }} p
  INNER JOIN listings l
    ON p.PROPERTY_ID = l.PROPERTY_ID
),

-- Perform SCD Type 2 tracking
property_changes AS (
  SELECT 
    *,
    LAG(FORMATTED_ADDRESS) OVER (PARTITION BY PROPERTY_ID ORDER BY LOAD_DATE) as prev_address,
    LAG(BEDROOMS) OVER (PARTITION BY PROPERTY_ID ORDER BY LOAD_DATE) as prev_bedrooms,
    LAG(BATHROOMS) OVER (PARTITION BY PROPERTY_ID ORDER BY LOAD_DATE) as prev_bathrooms,
    LAG(SQUARE_FOOTAGE) OVER (PARTITION BY PROPERTY_ID ORDER BY LOAD_DATE) as prev_sqft,
    LAG(ZONING_CODE) OVER (PARTITION BY PROPERTY_ID ORDER BY LOAD_DATE) as prev_zoning_code,
    LAG(LOAD_DATE) OVER (PARTITION BY PROPERTY_ID ORDER BY LOAD_DATE) as prev_load_date
  FROM stg_property
),

property_with_changes AS (
  SELECT
    *,
    CASE
      WHEN prev_address IS NULL 
        OR prev_bedrooms != BEDROOMS 
        OR prev_bathrooms != BATHROOMS 
        OR prev_sqft != SQUARE_FOOTAGE 
        OR prev_zoning_code != ZONING_CODE
      THEN 1
      ELSE 0
    END as is_changed
  FROM property_changes
),

property_with_dates AS (
  SELECT
    *,
    LOAD_DATE as valid_from,
    CASE
      WHEN is_changed = 1 AND LEAD(LOAD_DATE) OVER (PARTITION BY PROPERTY_ID ORDER BY LOAD_DATE) IS NOT NULL
      THEN LEAD(LOAD_DATE) OVER (PARTITION BY PROPERTY_ID ORDER BY LOAD_DATE)
      ELSE NULL
    END as valid_to,
    CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END as is_current
  FROM property_with_changes
  WHERE is_changed = 1
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['PROPERTY_ID', 'valid_from']) }} as property_sk,
  PROPERTY_ID,
  FORMATTED_ADDRESS,
  ADDRESS_LINE_1,
  ADDRESS_LINE_2,
  CITY,
  STATE,
  ZIP_CODE,
  COUNTY,
  LATITUDE,
  LONGITUDE,
  PROPERTY_TYPE,
  BEDROOMS,
  BATHROOMS,
  SQUARE_FOOTAGE,
  LOT_SIZE,
  YEAR_BUILT,
  LAST_SALE_DATE,
  LAST_SALE_PRICE,
  valid_from,
  valid_to,
  is_current,
  ZONING_ID,
  ZONING_CODE,
  ZONING_LONG_CODE,
  ZONING_GROUP
FROM property_with_dates