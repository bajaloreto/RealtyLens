-- models/marts/philly_sale_lifecycle_mart.sql
{{
  config(
    materialized = 'incremental'
      )
}}

WITH sale_listings AS (
  SELECT
    s.PROPERTY_ID,
    s.LISTING_ID,
    s.SALE_PRICE,
    s.DAYS_ON_MARKET,
    s.PROPERTY_STATUS,
    s.STATUS,
    s.LISTING_TYPE,
    s.load_date_sk,
    s.listed_date_sk,
    s.removed_date_sk,
    loc.ZIP_CODE,
    p.PROPERTY_TYPE,
    p.BEDROOMS
  FROM {{ ref('fct_sale_listing') }} s
  LEFT JOIN {{ ref('dim_location') }} loc ON s.location_sk = loc.location_sk
  LEFT JOIN {{ ref('dim_property') }} p ON s.property_sk = p.property_sk
  WHERE loc.CITY = 'Philadelphia'
),

-- Get previous day's data for each property to track changes
previous_day_data AS (
  SELECT
    PROPERTY_ID,
    load_date_sk,
    SALE_PRICE,
    PROPERTY_STATUS,
    STATUS,
    LAG(SALE_PRICE) OVER (PARTITION BY PROPERTY_ID ORDER BY load_date_sk) AS prev_price,
    LAG(PROPERTY_STATUS) OVER (PARTITION BY PROPERTY_ID ORDER BY load_date_sk) AS prev_property_status,
    LAG(STATUS) OVER (PARTITION BY PROPERTY_ID ORDER BY load_date_sk) AS prev_status,
    LAG(load_date_sk) OVER (PARTITION BY PROPERTY_ID ORDER BY load_date_sk) AS prev_load_date,
    MIN(load_date_sk) OVER (PARTITION BY PROPERTY_ID) AS first_appearance_date
  FROM sale_listings
),

-- Calculate daily status for each property
daily_property_status AS (
  SELECT
    s.PROPERTY_ID,
    s.LISTING_ID,
    s.load_date_sk,
    s.ZIP_CODE,
    s.PROPERTY_TYPE,
    s.BEDROOMS,
    s.SALE_PRICE,
    s.DAYS_ON_MARKET,
    p.prev_price,
    p.first_appearance_date,
    s.STATUS,
    p.prev_status,
    -- Define property lifecycle states
    CASE
      WHEN p.prev_status IS NULL THEN 'New'
      WHEN s.STATUS = 'Active' AND p.prev_status = 'Active' THEN 'Retained'
      WHEN s.STATUS = 'Active' AND p.prev_status != 'Active' THEN 'Resurrected'
      WHEN s.STATUS != 'Active' AND p.prev_status = 'Active' THEN 'Deactivated'
      ELSE 'Inactive'
    END AS property_state,
    -- Define price change states
    CASE
      WHEN p.prev_price IS NULL THEN 'New'
      WHEN s.SALE_PRICE > p.prev_price THEN 'Price Increased'
      WHEN s.SALE_PRICE < p.prev_price THEN 'Price Decreased'
      ELSE 'Price Unchanged'
    END AS price_state,
    -- Calculate price change percentage
    CASE
      WHEN p.prev_price IS NULL OR p.prev_price = 0 THEN NULL
      ELSE ((s.SALE_PRICE - p.prev_price) / p.prev_price) * 100
    END AS price_change_percent
  FROM sale_listings s
  LEFT JOIN previous_day_data p ON s.PROPERTY_ID = p.PROPERTY_ID AND s.load_date_sk = p.load_date_sk
)

-- Final output with GROUPING SETS
SELECT
  load_date_sk AS date,
  DATE_TRUNC('week', load_date_sk) AS week,
  ZIP_CODE,
  PROPERTY_TYPE,
  BEDROOMS,
  GROUPING(ZIP_CODE) as is_zip_code_grouping,
  GROUPING(PROPERTY_TYPE) as is_property_type_grouping,
  GROUPING(BEDROOMS) as is_bedrooms_grouping,
  
  -- Listing counts
  COUNT(DISTINCT PROPERTY_ID) AS total_properties,
  COUNT(DISTINCT CASE WHEN property_state = 'New' THEN PROPERTY_ID END) AS new_properties,
  COUNT(DISTINCT CASE WHEN property_state = 'Retained' THEN PROPERTY_ID END) AS retained_properties,
  COUNT(DISTINCT CASE WHEN property_state = 'Deactivated' THEN PROPERTY_ID END) AS deactivated_properties,
  COUNT(DISTINCT CASE WHEN property_state = 'Resurrected' THEN PROPERTY_ID END) AS resurrected_properties,
  COUNT(DISTINCT CASE WHEN property_state = 'Inactive' THEN PROPERTY_ID END) AS inactive_properties,
  
  -- Conversion metrics
  COUNT(DISTINCT CASE WHEN property_state = 'New' THEN PROPERTY_ID END) / 
    NULLIF(COUNT(DISTINCT PROPERTY_ID), 0) AS new_property_rate,
  COUNT(DISTINCT CASE WHEN property_state = 'Retained' THEN PROPERTY_ID END) /
    NULLIF(COUNT(DISTINCT PROPERTY_ID), 0) AS retention_rate,
  COUNT(DISTINCT CASE WHEN property_state = 'Deactivated' THEN PROPERTY_ID END) /
    NULLIF(COUNT(DISTINCT PROPERTY_ID), 0) AS deactivation_rate,
    
  -- Price change metrics
  COUNT(DISTINCT CASE WHEN price_state = 'Price Increased' THEN PROPERTY_ID END) AS price_increased,
  COUNT(DISTINCT CASE WHEN price_state = 'Price Decreased' THEN PROPERTY_ID END) AS price_decreased,
  COUNT(DISTINCT CASE WHEN price_state = 'Price Unchanged' THEN PROPERTY_ID END) AS price_unchanged,
  COUNT(DISTINCT CASE WHEN price_state = 'Price Increased' THEN PROPERTY_ID END) /
    NULLIF(COUNT(DISTINCT CASE WHEN price_state != 'New' THEN PROPERTY_ID END), 0) AS price_increase_rate,
  COUNT(DISTINCT CASE WHEN price_state = 'Price Decreased' THEN PROPERTY_ID END) /
    NULLIF(COUNT(DISTINCT CASE WHEN price_state != 'New' THEN PROPERTY_ID END), 0) AS price_decrease_rate,
    
  -- Price metrics
  AVG(SALE_PRICE) AS avg_sale_price,
  MEDIAN(SALE_PRICE) AS median_sale_price,
  AVG(price_change_percent) AS avg_price_change_pct,
  
  -- Time metrics
  AVG(DAYS_ON_MARKET) AS avg_days_on_market,
  MEDIAN(DAYS_ON_MARKET) AS median_days_on_market,
  AVG(DATEDIFF('day', first_appearance_date, load_date_sk)) AS avg_listing_age
  
FROM daily_property_status
GROUP BY GROUPING SETS (
  -- All dimensions
  (load_date_sk, ZIP_CODE, PROPERTY_TYPE, BEDROOMS),
  
  -- Time-based analysis
  (load_date_sk, ZIP_CODE, PROPERTY_TYPE),
  (load_date_sk, ZIP_CODE),
  (load_date_sk, PROPERTY_TYPE),
  (load_date_sk),
  
  -- Weekly aggregations
  (DATE_TRUNC('week', load_date_sk), ZIP_CODE, PROPERTY_TYPE, BEDROOMS),
  (DATE_TRUNC('week', load_date_sk), ZIP_CODE, PROPERTY_TYPE),
  (DATE_TRUNC('week', load_date_sk), ZIP_CODE),
  (DATE_TRUNC('week', load_date_sk), PROPERTY_TYPE),
  (DATE_TRUNC('week', load_date_sk))
)