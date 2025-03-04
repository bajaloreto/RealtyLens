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
  FROM {{ ref('cumulative_sale_listing') }}
),

monthly_metrics AS (
  SELECT
    DATE_TRUNC('month', date) as year_month,
    -- Create price segments
    CASE 
      WHEN SALE_PRICE < 250000 THEN 'Entry-Level'
      WHEN SALE_PRICE BETWEEN 250000 AND 500000 THEN 'Mid-Market'
      WHEN SALE_PRICE BETWEEN 500001 AND 1000000 THEN 'Premium'
      ELSE 'Luxury'
    END as price_segment,
    COUNT(DISTINCT PROPERTY_ID) as active_listings,
    COUNT(DISTINCT CASE WHEN property_state = 'New' THEN PROPERTY_ID END) as new_listings,
    COUNT(DISTINCT CASE WHEN is_likely_sold = 1 THEN PROPERTY_ID END) as likely_sold,
    AVG(SALE_PRICE) as avg_list_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SALE_PRICE) as median_price,
    AVG(days_on_market) as avg_dom,
    COUNT(DISTINCT CASE WHEN price_state = 'Price Decreased' THEN PROPERTY_ID END) as price_drops,
    AVG(CASE WHEN is_likely_sold = 1 THEN days_on_market END) as avg_days_to_sell
  FROM property_state_changes
  GROUP BY 1, 2
),

seasonal_metrics AS (
  SELECT
    year_month,
    price_segment,
    active_listings,
    new_listings,
    likely_sold,
    avg_list_price,
    median_price,
    avg_dom,
    price_drops,
    avg_days_to_sell,
    -- Market velocity (likely_sold to new listings ratio)
    likely_sold * 1.0 / NULLIF(new_listings, 0) as market_velocity,
    -- Months of inventory (active listings divided by monthly likely_sold)
    active_listings * 1.0 / NULLIF(likely_sold, 0) as months_of_inventory,
    -- Discount pressure (price drops per 100 active listings)
    price_drops * 100.0 / NULLIF(active_listings, 0) as discount_pressure,
    -- Seasonality index (relative to 12-month average)
    likely_sold * 1.0 / NULLIF(AVG(likely_sold) OVER (
      PARTITION BY price_segment 
      ORDER BY year_month 
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ), 0) as seasonality_index,
    CURRENT_DATE as snapshot_date
  FROM monthly_metrics
)

SELECT * FROM seasonal_metrics
ORDER BY year_month DESC, price_segment