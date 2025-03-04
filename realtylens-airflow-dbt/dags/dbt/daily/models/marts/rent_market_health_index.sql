{{
  config(
    materialized = 'table'
  )
}}

WITH daily_metrics AS (
  SELECT
    DATE_TRUNC('day', date) AS day,
    -- Property state counts
    COUNT(DISTINCT PROPERTY_ID) AS total_listings,
    COUNT(DISTINCT CASE WHEN property_state = 'New' THEN PROPERTY_ID END) AS new_listings,
    COUNT(DISTINCT CASE WHEN property_state = 'Retained' THEN PROPERTY_ID END) AS retained_listings,
    COUNT(DISTINCT CASE WHEN property_state = 'Churned' THEN PROPERTY_ID END) AS churned_listings,
    COUNT(DISTINCT CASE WHEN property_state = 'Resurrected' THEN PROPERTY_ID END) AS resurrected_listings,
    COUNT(DISTINCT CASE WHEN property_state = 'Inactive' THEN PROPERTY_ID END) AS inactive_listings,
    
    -- Price state counts
    COUNT(DISTINCT CASE WHEN price_state = 'Price Increased' THEN PROPERTY_ID END) AS price_increased,
    COUNT(DISTINCT CASE WHEN price_state = 'Price Decreased' THEN PROPERTY_ID END) AS price_decreased,
    COUNT(DISTINCT CASE WHEN price_state = 'Price Unchanged' THEN PROPERTY_ID END) AS price_unchanged,
    
    -- Other metrics
    AVG(days_on_market) AS avg_days_on_market,
    AVG(RENT_PRICE) AS avg_price
  FROM {{ ref('cumulative_rent_listing') }}
  GROUP BY day
)

SELECT
  day,
  total_listings,
  new_listings,
  retained_listings,
  churned_listings,
  resurrected_listings,
  inactive_listings,
  
  -- Market dynamics metrics with corrected denominators
  -- New listing rate: new listings as percentage of all active listings
  new_listings * 100.0 / NULLIF(new_listings + resurrected_listings + retained_listings, 0) AS new_listing_rate,
  
  -- Churn rate: churned listings as percentage of listings that could churn
  churned_listings * 100.0 / NULLIF(churned_listings + retained_listings, 0) AS churn_rate,
  
  -- Resurrection rate: resurrected listings as percentage of all active listings
  resurrected_listings * 100.0 / NULLIF(new_listings + resurrected_listings + retained_listings, 0) AS resurrection_rate,
  
  -- Active listings ratio
  (new_listings + resurrected_listings + retained_listings) * 100.0 / NULLIF(total_listings, 0) AS active_listing_rate,
  
  -- Price dynamics
  price_increased,
  price_decreased,
  price_unchanged,
  
  -- Price adjustment rates based on active listings
  price_increased * 100.0 / NULLIF(new_listings + resurrected_listings + retained_listings, 0) AS price_increase_rate,
  price_decreased * 100.0 / NULLIF(new_listings + resurrected_listings + retained_listings, 0) AS price_decrease_rate,
  
  avg_days_on_market,
  avg_price,
  (avg_price - LAG(avg_price) OVER (ORDER BY day)) / NULLIF(LAG(avg_price) OVER (ORDER BY day), 0) AS price_change_pct,
  
  -- Comprehensive market health score
  (
    (new_listings * 0.3) + 
    (churned_listings * 0.5) + 
    (resurrected_listings * 0.1) - 
    (price_decreased * 0.2) - 
    (avg_days_on_market * 0.01) - 
    (inactive_listings * 0.1)
  ) AS market_health_score,
  
  -- Supply-demand indicator with corrected ratio
  -- Now comparing new supply (new + resurrected) to demand (churned)
  (churned_listings * 1.0) / NULLIF(new_listings + resurrected_listings, 0) AS supply_demand_ratio
  
FROM daily_metrics
ORDER BY day DESC