create or replace transient table DATAEXPERT_STUDENT.jmusni07.fct_rent_listing
as
(
WITH rent_listings AS (
  SELECT *
  FROM {{ ref('stg_daily_rent_listing') }}
)
SELECT
  md5(cast(coalesce(cast(l.LISTING_ID as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(l.LOAD_DATE as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as listing_sk,
  l.LISTING_ID,
  COALESCE(p.property_sk, md5(cast(l.PROPERTY_ID as TEXT))) as property_sk,
  COALESCE(s.status_sk, md5(cast(l.STATUS as TEXT))) as status_sk,
  COALESCE(loc.location_sk, md5(cast(coalesce(l.CITY, '') || '-' || coalesce(l.STATE, '') || '-' || coalesce(l.ZIP_CODE, '') as TEXT))) as location_sk,
  COALESCE(m.mls_sk, md5(cast(coalesce(l.MLS_NAME, '') || '-' || coalesce(l.MLS_NUMBER, '') as TEXT))) as mls_sk,
  -- Date dimensions
  TO_DATE(l.LOAD_DATE) as load_date_sk,
  TO_DATE(l.LISTED_DATE) as listed_date_sk,
  TO_DATE(l.REMOVED_DATE) as removed_date_sk,
  TO_DATE(l.CREATED_DATE) as created_date_sk,
  TO_DATE(l.LAST_SEEN_DATE) as last_seen_date_sk,
  -- Facts
  l.RENT_PRICE,
  l.DAYS_ON_MARKET,
  l.PROPERTY_STATUS,
  l.STATUS,
  l.LISTING_TYPE,
  l.LOAD_DATE
FROM rent_listings l
LEFT JOIN DATAEXPERT_STUDENT.jmusni07.dim_property p 
  ON l.PROPERTY_ID = p.PROPERTY_ID 
  -- Remove date filtering entirely for first load
LEFT JOIN DATAEXPERT_STUDENT.jmusni07.dim_listing_status s 
  ON (
    CASE
      WHEN l.STATUS = 'active' THEN 'A'
      WHEN l.STATUS = 'inactive' THEN 'I'
      WHEN l.LISTING_TYPE = 'For Rent' THEN 'FR'
      ELSE 'FR' -- Default for rent listings
    END
  ) = s.status_code
LEFT JOIN DATAEXPERT_STUDENT.jmusni07.dim_location loc 
  ON l.CITY = loc.CITY 
  AND l.STATE = loc.STATE 
  AND l.ZIP_CODE = loc.ZIP_CODE 
  AND l.COUNTY = loc.COUNTY
LEFT JOIN DATAEXPERT_STUDENT.jmusni07.dim_mls m 
  ON l.MLS_NAME = m.MLS_NAME 
  AND l.MLS_NUMBER = m.MLS_NUMBER
);