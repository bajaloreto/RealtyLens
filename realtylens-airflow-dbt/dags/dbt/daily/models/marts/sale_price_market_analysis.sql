{{
  config(
    materialized = 'table'
  )
}}

WITH listing_data AS (
  SELECT
    s.LOAD_DATE,
    p.PROPERTY_TYPE,
    s.SALE_PRICE,
    s.STATUS,
    p.BEDROOMS
  FROM {{ ref('fct_sale_listing') }} s
  JOIN {{ ref('dim_property') }} p ON s.property_sk = p.property_sk
  WHERE s.LOAD_DATE = (SELECT MAX(LOAD_DATE) FROM {{ ref('fct_sale_listing') }})
)

SELECT
  CASE
    WHEN GROUPING(PROPERTY_TYPE) = 0 AND GROUPING(STATUS) = 0 AND GROUPING(BEDROOMS) = 0
      THEN 'property_type__status__bedrooms'
    WHEN GROUPING(PROPERTY_TYPE) = 0 AND GROUPING(STATUS) = 0
      THEN 'property_type__status'
    WHEN GROUPING(PROPERTY_TYPE) = 0 AND GROUPING(BEDROOMS) = 0
      THEN 'property_type__bedrooms'
    WHEN GROUPING(STATUS) = 0 AND GROUPING(BEDROOMS) = 0
      THEN 'status__bedrooms'
    WHEN GROUPING(PROPERTY_TYPE) = 0
      THEN 'property_type'
    WHEN GROUPING(STATUS) = 0
      THEN 'status'
    WHEN GROUPING(BEDROOMS) = 0
      THEN 'bedrooms'
    ELSE 'overall'
  END as aggregation_level,
  COALESCE(PROPERTY_TYPE, '(overall)') as PROPERTY_TYPE,
  COALESCE(STATUS, '(overall)') as STATUS,
  COALESCE(CAST(BEDROOMS AS VARCHAR), '(overall)') as BEDROOMS,
  COUNT(1) as listing_count,
  AVG(SALE_PRICE) as avg_sale_price,
  MIN(SALE_PRICE) as min_sale_price,
  MAX(SALE_PRICE) as max_sale_price
FROM listing_data
GROUP BY GROUPING SETS (
  (PROPERTY_TYPE, STATUS, BEDROOMS),
  (PROPERTY_TYPE, STATUS),
  (PROPERTY_TYPE, BEDROOMS),
  (STATUS, BEDROOMS),
  (PROPERTY_TYPE),
  (STATUS),
  (BEDROOMS),
  ()
)
ORDER BY listing_count DESC