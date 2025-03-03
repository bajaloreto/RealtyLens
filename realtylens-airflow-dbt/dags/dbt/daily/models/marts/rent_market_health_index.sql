{{
  config(
    materialized = 'table'
  )
}}

WITH daily_metrics AS (
  SELECT
    DATE_TRUNC('day', date) AS day,
    COUNT(DISTINCT PROPERTY_ID) AS daily_listings,
    COUNT(DISTINCT CASE WHEN property_state = 'New' THEN PROPERTY_ID END) AS new_listings,
    COUNT(DISTINCT CASE WHEN property_state = 'Churned' THEN PROPERTY_ID END) AS churned_listings,
    COUNT(DISTINCT CASE WHEN property_state = 'Retained' THEN PROPERTY_ID END) AS retained_listings,
    AVG(days_on_market) AS avg_days_on_market,
    COUNT(DISTINCT CASE WHEN price_state = 'Price Decreased' THEN PROPERTY_ID END) AS price_decreases,
    AVG(RENT_PRICE) AS avg_price
  FROM {{ ref('cumulative_rent_listing') }}
  GROUP BY day
)

SELECT
  day,
  daily_listings,
  new_listings,
  churned_listings,
  retained_listings,
  (new_listings * 1.0 - churned_listings) / NULLIF(LAG(retained_listings) OVER (ORDER BY day), 0) AS market_growth_rate,
  avg_days_on_market,
  price_decreases,
  avg_price,
  (avg_price - LAG(avg_price) OVER (ORDER BY day)) / NULLIF(LAG(avg_price) OVER (ORDER BY day), 0) AS price_change_pct,
  -- Market health composite score (customize weights based on your business needs)
  (new_listings * 0.3) - (churned_listings * 0.2) - (price_decreases * 0.1) - (avg_days_on_market * 0.01) AS market_health_score
FROM daily_metrics
ORDER BY day DESC