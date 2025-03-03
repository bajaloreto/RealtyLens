{{
  config(
    materialized = 'table'
  )
}}

WITH property_state_changes AS (
  SELECT
    PROPERTY_ID,
    date,
    SALE_PRICE,
    days_on_market,
    property_state,
    price_state,
    LAG(property_state) OVER (PARTITION BY PROPERTY_ID ORDER BY date) as previous_state,
    CASE 
      WHEN property_state = 'Churned' AND 
           LAG(property_state) OVER (PARTITION BY PROPERTY_ID ORDER BY date) IN ('Retained', 'New') 
      THEN 1 
      ELSE 0 
    END as is_likely_sold
  FROM cumulative_sale_listing
),

price_segments AS (
  SELECT
    PROPERTY_ID,
    date,
    days_on_market,
    SALE_PRICE,
    property_state,
    price_state,
    is_likely_sold,
    -- Create price segments
    CASE 
      WHEN SALE_PRICE < 250000 THEN 'Entry-Level'
      WHEN SALE_PRICE BETWEEN 250000 AND 500000 THEN 'Mid-Market'
      WHEN SALE_PRICE BETWEEN 500001 AND 1000000 THEN 'Premium'
      ELSE 'Luxury'
    END as price_segment,
    -- Days on market segments
    CASE 
      WHEN days_on_market BETWEEN 0 AND 30 THEN 'Fast (0-30 days)'
      WHEN days_on_market BETWEEN 31 AND 90 THEN 'Normal (31-90 days)'
      ELSE 'Slow (90+ days)'
    END as days_segment
  FROM property_state_changes
),

-- Get first and last prices
first_last_dates AS (
  SELECT
    PROPERTY_ID,
    MIN(date) as first_date,
    MAX(date) as last_date
  FROM cumulative_sale_listing
  GROUP BY PROPERTY_ID
),

property_prices AS (
  SELECT
    f.PROPERTY_ID,
    first_price.SALE_PRICE as initial_price,
    last_price.SALE_PRICE as final_price
  FROM first_last_dates f
  JOIN cumulative_sale_listing first_price 
    ON f.PROPERTY_ID = first_price.PROPERTY_ID AND f.first_date = first_price.date
  JOIN cumulative_sale_listing last_price 
    ON f.PROPERTY_ID = last_price.PROPERTY_ID AND f.last_date = last_price.date
),

market_efficiency AS (
  SELECT
    ps.price_segment,
    ps.days_segment,
    COUNT(DISTINCT ps.PROPERTY_ID) as listing_count,
    SUM(ps.is_likely_sold) as likely_sold_count,
    SUM(ps.is_likely_sold) * 100.0 / NULLIF(COUNT(DISTINCT ps.PROPERTY_ID), 0) as conversion_rate,
    AVG(CASE WHEN ps.is_likely_sold = 1 THEN 
      (pp.final_price - pp.initial_price) / NULLIF(pp.initial_price, 0) * 100 
      ELSE NULL END) as avg_price_adjustment_pct,
    AVG(CASE WHEN ps.is_likely_sold = 1 THEN ps.days_on_market ELSE NULL END) as avg_days_to_sell,
    -- Market efficiency score (higher is better)
    (SUM(ps.is_likely_sold) * 100.0 / NULLIF(COUNT(DISTINCT ps.PROPERTY_ID), 0)) - 
    (AVG(CASE WHEN ps.is_likely_sold = 1 THEN 
      ABS((pp.final_price - pp.initial_price) / NULLIF(pp.initial_price, 0) * 100) 
      ELSE 5 END)) as market_efficiency_score,
    CURRENT_DATE as snapshot_date
  FROM price_segments ps
  LEFT JOIN property_prices pp ON ps.PROPERTY_ID = pp.PROPERTY_ID
  GROUP BY 1, 2
)

SELECT * FROM market_efficiency
ORDER BY price_segment, days_segment;