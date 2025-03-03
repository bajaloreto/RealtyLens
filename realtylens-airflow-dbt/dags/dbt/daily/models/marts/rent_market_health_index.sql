{{
  config(
    materialized = 'table'
  )
}}

WITH market_metrics AS (
  SELECT
    DATE_TRUNC('week', date) as week,
    COUNT(CASE WHEN property_state = 'New' THEN 1 END) as new_listings,
    COUNT(CASE WHEN property_state = 'Churned' THEN 1 END) as churned_listings,
    COUNT(CASE WHEN property_state = 'Retained' THEN 1 END) as retained_listings,
    AVG(days_on_market) as avg_days_on_market,
    COUNT(CASE WHEN price_state = 'Price Decreased' THEN 1 END) as price_decreases,
    AVG(RENT_PRICE) as avg_price
  FROM {{ ref('cumulative_rent_listing') }}
  GROUP BY 1
)

SELECT
  week,
  new_listings,
  churned_listings,
  retained_listings,
  (new_listings * 1.0 - churned_listings) / NULLIF(LAG(retained_listings) OVER (ORDER BY week), 0) as market_growth_rate,
  avg_days_on_market,
  price_decreases,
  avg_price,
  (avg_price - LAG(avg_price) OVER (ORDER BY week)) / NULLIF(LAG(avg_price) OVER (ORDER BY week), 0) as price_change_pct,
  -- Market health composite score (customize weights based on your business needs)
  (new_listings * 0.3) - (churned_listings * 0.2) - (price_decreases * 0.1) - (avg_days_on_market * 0.01) as market_health_score
FROM market_metrics
ORDER BY week DESC