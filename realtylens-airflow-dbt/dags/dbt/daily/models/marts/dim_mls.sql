{{
  config(
    materialized = 'table',
    unique_key = 'mls_sk'
  )
}}

WITH mls AS (
  -- Rent listings
  SELECT DISTINCT
    MLS_NAME,
    MLS_NUMBER,
    LOAD_DATE
  FROM {{ ref('stg_daily_rent_listing') }}
  
  UNION
  
  -- Sale listings
  SELECT DISTINCT
    MLS_NAME,
    MLS_NUMBER,
    LOAD_DATE
  FROM {{ ref('stg_daily_sale_listing') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['MLS_NAME', 'MLS_NUMBER']) }} as mls_sk,
  MLS_NAME,
  MLS_NUMBER,
  MIN(LOAD_DATE) as first_seen_date,
  MAX(LOAD_DATE) as last_seen_date
FROM mls
GROUP BY MLS_NAME, MLS_NUMBER