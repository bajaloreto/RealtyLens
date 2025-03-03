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
    COUNT(DISTINCT CASE WHEN price_state = 'Price Decreased' THEN PROPERTY_ID END) AS price_decreases
  FROM {{ ref('cumulative_rent_listing') }}
  GROUP BY day
),

weekly_metrics AS (
  SELECT
    DATE_TRUNC('week', day) AS week,
    AVG(daily_listings) AS avg_weekly_listings,
    SUM(new_listings) AS total_new_listings,
    SUM(churned_listings) AS total_churned_listings,
    SUM(retained_listings) AS total_retained_listings,
    AVG(avg_days_on_market) AS avg_days_on_market,
    SUM(price_decreases) AS total_price_decreases
  FROM daily_metrics
  GROUP BY week
)

SELECT
  week,
  avg_weekly_listings,
  total_new_listings,
  total_churned_listings,
  total_retained_listings,
  avg_days_on_market,
  total_price_decreases
FROM weekly_metrics
ORDER BY week DESC;