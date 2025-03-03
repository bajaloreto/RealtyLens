{{
  config(
    materialized = 'table',
    alias = 'rental_pricing_geo_analysis'
  )
}}

WITH location_listings AS (
  SELECT
    r.listing_sk,
    r.rent_price,
    r.status,
    r.listing_type,
    r.load_date_sk,
    p.property_type,
    p.bedrooms,
    p.zoning_group,
    loc.city,
    loc.state,
    loc.zip_code
  FROM {{ ref('fct_rent_listing') }} r
  LEFT JOIN {{ ref('dim_property') }} p ON r.property_sk = p.property_sk
  JOIN {{ ref('dim_location') }} loc ON r.location_sk = loc.location_sk
  WHERE 
    r.rent_price > 0
    AND loc.zip_code IS NOT NULL
)

-- Instead of trying to aggregate GEOGRAPHY in the mart,
-- we'll just keep the price metrics and zip codes
-- and join with the polygon data in the dashboard
SELECT
  CASE
    WHEN GROUPING(city) = 0 AND GROUPING(state) = 0 AND GROUPING(zip_code) = 0 AND GROUPING(property_type) = 0 AND GROUPING(bedrooms) = 0
      THEN 'city_state_zip__property_type__bedrooms'
    WHEN GROUPING(city) = 0 AND GROUPING(state) = 0 AND GROUPING(zip_code) = 0 AND GROUPING(property_type) = 0
      THEN 'city_state_zip__property_type'
    WHEN GROUPING(city) = 0 AND GROUPING(state) = 0 AND GROUPING(zip_code) = 0 AND GROUPING(bedrooms) = 0
      THEN 'city_state_zip__bedrooms'
    WHEN GROUPING(city) = 0 AND GROUPING(state) = 0 AND GROUPING(zip_code) = 0
      THEN 'city_state_zip'
    WHEN GROUPING(city) = 0 AND GROUPING(state) = 0
      THEN 'city_state'
    ELSE 'overall'
  END as aggregation_level,
  
  COALESCE(city, '(overall)') as city,
  COALESCE(state, '(overall)') as state,
  COALESCE(zip_code, '(overall)') as zip_code,
  COALESCE(property_type, '(overall)') as property_type,
  COALESCE(CAST(bedrooms AS VARCHAR), '(overall)') as bedrooms,
  load_date_sk as day,
  
  -- Key metrics for visualization
  COUNT(1) as listing_count,
  AVG(rent_price) as avg_rent_price,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rent_price) as median_rent_price,
  MIN(rent_price) as min_rent_price,
  MAX(rent_price) as max_rent_price,
  STDDEV(rent_price) as stddev_rent_price
  
FROM location_listings
GROUP BY 
  load_date_sk,
  GROUPING SETS (
    (city, state, zip_code, property_type, bedrooms),
    (city, state, zip_code, property_type),
    (city, state, zip_code, bedrooms),
    (city, state, zip_code),
    (city, state),
    ()
  )