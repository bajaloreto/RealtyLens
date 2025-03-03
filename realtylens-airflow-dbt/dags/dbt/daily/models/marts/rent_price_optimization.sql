{{
  config(
    materialized = 'table'
  )
}}

WITH property_dates AS (
  -- First, get the first and last dates for each property
  SELECT
    PROPERTY_ID,
    MIN(date) as first_date,
    MAX(date) as last_date
  FROM {{ ref('cumulative_rent_listing') }}
  GROUP BY 1
),

property_prices AS (
  -- Get initial and final prices by joining with the original data
  SELECT
    p.PROPERTY_ID,
    p.first_date,
    p.last_date,
    i.RENT_PRICE as initial_price,
    f.RENT_PRICE as final_price,
    f.days_on_market,
    CASE 
      WHEN f.property_state = 'Churned' AND i.property_state IN ('New', 'Retained') THEN 1 
      ELSE 0 
    END as likely_rented
  FROM property_dates p
  JOIN cumulative_rent_listing i ON p.PROPERTY_ID = i.PROPERTY_ID AND p.first_date = i.date
  JOIN cumulative_rent_listing f ON p.PROPERTY_ID = f.PROPERTY_ID AND p.last_date = f.date
),

price_changes AS (
  -- Count distinct prices for each property
  SELECT
    PROPERTY_ID,
    COUNT(DISTINCT RENT_PRICE) > 1 as had_price_change
  FROM cumulative_rent_listing
  GROUP BY 1
),

pricing_effectiveness AS (
  -- Now calculate the price strategy and other metrics
  SELECT
    p.PROPERTY_ID,
    p.initial_price,
    p.final_price,
    p.days_on_market,
    c.had_price_change,
    p.likely_rented,
    CASE 
      WHEN p.initial_price > p.final_price THEN 'Discount'
      WHEN p.initial_price < p.final_price THEN 'Premium'
      ELSE 'Unchanged'
    END as price_strategy
  FROM property_prices p
  JOIN price_changes c ON p.PROPERTY_ID = c.PROPERTY_ID
),

price_quintiles AS (
  -- Calculate price quintiles in a separate step
  SELECT
    *,
    NTILE(5) OVER (ORDER BY initial_price) as price_quintile
  FROM pricing_effectiveness
),

final_metrics AS (
  -- Final aggregations
  SELECT
    price_quintile,
    price_strategy,
    COUNT(*) as property_count,
    AVG(days_on_market) as avg_days_on_market,
    SUM(likely_rented) as likely_rented_count,
    SUM(likely_rented) * 1.0 / COUNT(*) as conversion_rate,
    AVG((final_price - initial_price) / NULLIF(initial_price, 0) * 100) as avg_price_adjustment_pct,
    CURRENT_DATE as snapshot_date  -- Add snapshot date for incremental loading
  FROM price_quintiles
  GROUP BY 1, 2
)

SELECT * FROM final_metrics
ORDER BY price_quintile, price_strategy;