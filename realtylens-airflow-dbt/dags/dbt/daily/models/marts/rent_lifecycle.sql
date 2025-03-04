{{
  config(
    materialized = 'table'
  )
}}

WITH lifecycle_stages AS (
  SELECT
    PROPERTY_ID,
    date,
    days_on_market,
    RENT_PRICE,
    property_state,
    price_state,
    -- Create lifecycle stage indicators
    CASE 
      WHEN days_on_market BETWEEN 0 AND 7 THEN 'Week1'
      WHEN days_on_market BETWEEN 8 AND 14 THEN 'Week2'
      WHEN days_on_market BETWEEN 15 AND 30 THEN 'Month1'
      WHEN days_on_market BETWEEN 31 AND 60 THEN 'Month2'
      ELSE 'Extended'
    END as lifecycle_stage,
    -- Identify key events
    CASE 
      WHEN property_state = 'Churned' AND LAG(property_state) 
        OVER (PARTITION BY PROPERTY_ID ORDER BY date) IN ('Retained', 'New') 
      THEN 1 ELSE 0 
    END as likely_rented,
    CASE 
      WHEN price_state = 'Price Decreased' 
      THEN 1 ELSE 0 
    END as had_price_drop
  FROM {{ ref('cumulative_rent_listing') }}
)

SELECT
  lifecycle_stage,
  COUNT(DISTINCT PROPERTY_ID) as property_count,
  SUM(likely_rented) as conversions,
  SUM(likely_rented) * 100.0 / NULLIF(COUNT(DISTINCT PROPERTY_ID), 0) as conversion_rate,
  SUM(had_price_drop) as price_drops,
  SUM(had_price_drop) * 100.0 / NULLIF(COUNT(DISTINCT PROPERTY_ID), 0) as price_drop_rate,
  AVG(CASE WHEN likely_rented = 1 THEN days_on_market END) as avg_days_to_conversion,
  CURRENT_DATE as snapshot_date  -- Add snapshot date for incremental loading
FROM lifecycle_stages
GROUP BY 1
ORDER BY 
  CASE 
    WHEN lifecycle_stage = 'Week1' THEN 1
    WHEN lifecycle_stage = 'Week2' THEN 2
    WHEN lifecycle_stage = 'Month1' THEN 3
    WHEN lifecycle_stage = 'Month2' THEN 4
    WHEN lifecycle_stage = 'Extended' THEN 5
  END