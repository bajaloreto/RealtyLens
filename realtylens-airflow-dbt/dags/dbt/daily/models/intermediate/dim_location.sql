{{
  config(
    materialized = 'table',
    unique_key = 'location_sk'
  )
}}

WITH locations AS (
  -- Rent listings
  SELECT DISTINCT
    CITY,
    STATE,
    ZIP_CODE,
    COUNTY,
    LOAD_DATE
  FROM {{ ref('stg_daily_rent_listing') }}
  
  UNION
  
  -- Sale listings
  SELECT DISTINCT
    CITY,
    STATE,
    ZIP_CODE,
    COUNTY,
    LOAD_DATE
  FROM {{ ref('stg_daily_sale_listing') }}
  
  UNION
  
  -- Property details
  SELECT DISTINCT
    CITY,
    STATE,
    ZIP_CODE,
    COUNTY,
    LOAD_DATE
  FROM {{ source('realtylens', 'lkp_property_details') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['CITY', 'STATE', 'ZIP_CODE', 'COUNTY']) }} as location_sk,
  CITY,
  STATE,
  ZIP_CODE,
  COUNTY,
  MIN(LOAD_DATE) as first_seen_date,
  MAX(LOAD_DATE) as last_seen_date
FROM locations
GROUP BY CITY, STATE, ZIP_CODE, COUNTY