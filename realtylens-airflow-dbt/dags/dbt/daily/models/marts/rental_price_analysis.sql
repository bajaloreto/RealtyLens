{{
  config(
    materialized = 'table'  )
}}


WITH philly_listings AS (
  SELECT
    r.listing_sk,
    r.rent_price,
    r.status,
    r.listing_type,
    r.load_date_sk,
    p.property_type,
    p.bedrooms,
    p.zoning_group
  FROM {{ ref('fct_rent_listing') }} r
  JOIN {{ ref('dim_property') }} p ON r.property_sk = p.property_sk
  JOIN {{ ref('dim_location') }} loc ON r.location_sk = loc.location_sk
  WHERE 
    loc.city = 'Philadelphia' 
    AND loc.state = 'PA'
    AND r.rent_price > 0
)

SELECT
  CASE
    WHEN GROUPING(property_type) = 0 AND GROUPING(bedrooms) = 0 AND GROUPING(zoning_group) = 0
      THEN 'property_type__bedrooms__zoning'
    WHEN GROUPING(property_type) = 0 AND GROUPING(bedrooms) = 0
      THEN 'property_type__bedrooms'
    WHEN GROUPING(property_type) = 0 AND GROUPING(zoning_group) = 0
      THEN 'property_type__zoning'
    WHEN GROUPING(bedrooms) = 0 AND GROUPING(zoning_group) = 0
      THEN 'bedrooms__zoning'
    WHEN GROUPING(property_type) = 0
      THEN 'property_type'
    WHEN GROUPING(bedrooms) = 0
      THEN 'bedrooms'
    WHEN GROUPING(zoning_group) = 0
      THEN 'zoning_group'
    ELSE 'overall'
  END as aggregation_level,
  
  COALESCE(property_type, '(overall)') as property_type,
  COALESCE(CAST(bedrooms AS VARCHAR), '(overall)') as bedrooms,
  COALESCE(zoning_group, '(overall)') as zoning_group,
  load_date_sk as day,
  
  COUNT(1) as listing_count,
  AVG(rent_price) as avg_rent_price,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rent_price) as median_rent_price,
  MIN(rent_price) as min_rent_price,
  MAX(rent_price) as max_rent_price,
  STDDEV(rent_price) as stddev_rent_price
  
FROM philly_listings
GROUP BY 
  load_date_sk,
  GROUPING SETS (
    (property_type, bedrooms, zoning_group),
    (property_type, bedrooms),
    (property_type, zoning_group),
    (bedrooms, zoning_group),
    (property_type),
    (bedrooms),
    (zoning_group),
    ()
  )
