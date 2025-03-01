{{
  config(
    materialized = 'incremental',
    unique_key = 'listing_sk',
    incremental_strategy = 'merge'
  )
}}

WITH rent_listings AS (
  SELECT
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
    LOT_SIZE,
    STATUS,
    RENT_PRICE,
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
  FROM {{ ref('stg_daily_rent_listing') }}
  
  {% if is_incremental() %}
    WHERE LOAD_DATE > (SELECT MAX(LOAD_DATE) FROM {{ this }})
  {% endif %}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['LISTING_ID', 'COALESCE(l.LOAD_DATE, CURRENT_DATE())']) }} as listing_sk,
  l.LISTING_ID,
  COALESCE(p.property_sk, {{ dbt_utils.generate_surrogate_key(['l.PROPERTY_ID', 'CURRENT_DATE()']) }}) as property_sk,
  COALESCE(s.status_sk, {{ dbt_utils.generate_surrogate_key(['CASE WHEN l.STATUS = \'active\' THEN \'A\' WHEN l.STATUS = \'inactive\' THEN \'I\' WHEN l.LISTING_TYPE = \'For Rent\' THEN \'FR\' ELSE \'UNKNOWN\' END']) }}) as status_sk,
  COALESCE(loc.location_sk, {{ dbt_utils.generate_surrogate_key(['COALESCE(l.CITY, \'UNKNOWN\')', 'COALESCE(l.STATE, \'UNKNOWN\')', 'COALESCE(l.ZIP_CODE, \'UNKNOWN\')', 'COALESCE(l.COUNTY, \'UNKNOWN\')']) }}) as location_sk,
  COALESCE(m.mls_sk, {{ dbt_utils.generate_surrogate_key(['COALESCE(l.MLS_NAME, \'UNKNOWN\')', 'COALESCE(l.MLS_NUMBER, \'UNKNOWN\')']) }}) as mls_sk,
  
  -- Date dimensions - using actual date values instead of conversion
  -- This ensures proper join to date dimension table
  COALESCE(TRY_TO_DATE(l.LOAD_DATE), CURRENT_DATE()) as load_date_sk,
  TRY_TO_DATE(l.LISTED_DATE) as listed_date_sk,
  TRY_TO_DATE(l.REMOVED_DATE) as removed_date_sk,
  TRY_TO_DATE(l.CREATED_DATE) as created_date_sk,
  TRY_TO_DATE(l.LAST_SEEN_DATE) as last_seen_date_sk,
  
  -- Facts
  l.RENT_PRICE,
  l.DAYS_ON_MARKET,
  l.PROPERTY_STATUS,
  l.STATUS,
  l.LISTING_TYPE,
  l.LOAD_DATE,
  
  -- Zoning dimension (based on property)
  COALESCE(z.zoning_sk, {{ dbt_utils.generate_surrogate_key(['\'UNKNOWN\'']) }}) as zoning_sk,
  
  -- Additional metadata to help with troubleshooting
  CURRENT_TIMESTAMP() as etl_timestamp,
  CASE 
    WHEN p.property_sk IS NULL OR loc.location_sk IS NULL OR s.status_sk IS NULL 
    THEN 'PARTIAL_JOIN' 
    ELSE 'FULL_JOIN' 
  END as join_quality,
  {% if is_incremental() %}
    'INCREMENTAL_LOAD'
  {% else %}
    'INITIAL_LOAD'
  {% endif %} as record_source
FROM rent_listings l
LEFT JOIN {{ ref('dim_property') }} p 
  ON l.PROPERTY_ID = p.PROPERTY_ID 
  AND COALESCE(TRY_TO_DATE(l.LOAD_DATE), CURRENT_DATE()) >= p.valid_from 
  AND (COALESCE(TRY_TO_DATE(l.LOAD_DATE), CURRENT_DATE()) < p.valid_to OR p.valid_to IS NULL)
  AND p.is_current = 1
LEFT JOIN {{ ref('dim_listing_status') }} s 
  ON (
    CASE
      WHEN l.STATUS = 'active' THEN 'A'
      WHEN l.STATUS = 'inactive' THEN 'I'
      WHEN l.LISTING_TYPE = 'For Rent' THEN 'FR'
      ELSE 'UNKNOWN' -- More explicit unknown status
    END
  ) = s.status_code
LEFT JOIN {{ ref('dim_location') }} loc 
  ON COALESCE(l.CITY, 'UNKNOWN') = loc.CITY
  AND COALESCE(l.STATE, 'UNKNOWN') = loc.STATE
  AND COALESCE(l.ZIP_CODE, 'UNKNOWN') = loc.ZIP_CODE
  AND COALESCE(l.COUNTY, 'UNKNOWN') = loc.COUNTY
LEFT JOIN {{ ref('dim_mls') }} m 
  ON COALESCE(l.MLS_NAME, 'UNKNOWN') = m.MLS_NAME
  AND COALESCE(l.MLS_NUMBER, 'UNKNOWN') = m.MLS_NUMBER
LEFT JOIN {{ ref('dim_zoning') }} z
  ON p.ZONING_ID = z.ZONING_ID