{{
  config(
    materialized = 'incremental',
    unique_key = 'listing_sk',
    incremental_strategy = 'merge'
  )
}}

WITH sale_listings AS (
  SELECT
    PROPERTY_ID,
    CITY,
    STATE,
    ZIP_CODE,
    COUNTY,
    STATUS,
    SALE_PRICE,
    LISTING_TYPE,
    LISTED_DATE,
    REMOVED_DATE,
    CREATED_DATE,
    LAST_SEEN_DATE,
    DAYS_ON_MARKET,
    MLS_NAME,
    MLS_NUMBER,
    LOAD_DATE,
    PROPERTY_STATUS,
    LISTING_ID
  FROM {{ ref('stg_daily_sale_listing') }}
  
  {% if is_incremental() and not flags.FULL_REFRESH %}
    WHERE LOAD_DATE::DATE > (SELECT COALESCE(MAX(load_date_sk), '1900-01-01'::DATE) FROM {{ this }})
  {% endif %}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['LISTING_ID', 'LOAD_DATE']) }} as listing_sk,
  l.LISTING_ID,
  p.property_sk,
  s.status_sk,
  loc.location_sk,
  m.mls_sk,
  
  -- Date dimensions with direct casting
  l.LOAD_DATE::DATE as load_date_sk,
  l.LISTED_DATE::DATE as listed_date_sk,
  l.REMOVED_DATE::DATE as removed_date_sk,
  l.CREATED_DATE::DATE as created_date_sk,
  l.LAST_SEEN_DATE::DATE as last_seen_date_sk,
  
  -- Facts
  l.SALE_PRICE,
  l.DAYS_ON_MARKET,
  l.PROPERTY_STATUS,
  l.STATUS,
  l.LISTING_TYPE,
  l.LOAD_DATE,
  
  -- Metadata
  CURRENT_TIMESTAMP() as etl_timestamp
FROM sale_listings l
LEFT JOIN {{ ref('dim_property') }} p 
  ON l.PROPERTY_ID = p.PROPERTY_ID 
  AND l.LOAD_DATE::DATE >= p.valid_from 
  AND (l.LOAD_DATE::DATE < p.valid_to OR p.valid_to IS NULL)
LEFT JOIN {{ ref('dim_listing_status') }} s 
  ON (
    CASE
      WHEN l.STATUS = 'active' THEN 'A'
      WHEN l.STATUS = 'inactive' THEN 'I'
      WHEN l.LISTING_TYPE = 'For Sale' THEN 'FS'
      ELSE 'UNKNOWN'
    END
  ) = s.status_code
LEFT JOIN {{ ref('dim_location') }} loc 
  ON l.CITY = loc.CITY
  AND l.STATE = loc.STATE
  AND l.ZIP_CODE = loc.ZIP_CODE
  AND l.COUNTY = loc.COUNTY
LEFT JOIN {{ ref('dim_mls') }} m 
  ON l.MLS_NAME = m.MLS_NAME
  AND l.MLS_NUMBER = m.MLS_NUMBER