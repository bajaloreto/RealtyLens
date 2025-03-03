{{
  config(
    materialized = 'table'
  )
}}

WITH rent_daily_metrics AS (
  SELECT
    LOAD_DATE as date,
    PROPERTY_TYPE,
    STATUS,
    COUNT(1) as listing_count,
    AVG(RENT_PRICE) as avg_rent_price
  FROM {{ ref('fct_rent_listing') }} r
  JOIN {{ ref('dim_property') }} p ON r.property_sk = p.property_sk
  GROUP BY LOAD_DATE, PROPERTY_TYPE, STATUS
),

windowed_metrics AS (
  SELECT
    date,
    PROPERTY_TYPE,
    STATUS,
    listing_count,
    avg_rent_price,
    -- 7-day moving average
    AVG(listing_count) OVER (
      PARTITION BY PROPERTY_TYPE
      ORDER BY date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as seven_day_avg_count,
    
    -- 7-day moving average for price
    AVG(avg_rent_price) OVER (
      PARTITION BY PROPERTY_TYPE
      ORDER BY date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as seven_day_avg_price,
    
    -- Month-to-date cumulative
    SUM(listing_count) OVER (
      PARTITION BY PROPERTY_TYPE, DATE_TRUNC('month', date)
      ORDER BY date
    ) as month_to_date_count,
    
    -- Previous 7-day period
    AVG(listing_count) OVER (
      PARTITION BY PROPERTY_TYPE
      ORDER BY date
      ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ) as previous_seven_day_avg_count,
    
    -- Year-to-date cumulative
    SUM(listing_count) OVER (
      PARTITION BY PROPERTY_TYPE, DATE_TRUNC('year', date)
      ORDER BY date
    ) as year_to_date_count
  FROM rent_daily_metrics
)

SELECT
  date,
  PROPERTY_TYPE,
  STATUS,
  listing_count,
  avg_rent_price,
  seven_day_avg_count,
  seven_day_avg_price,
  month_to_date_count,
  previous_seven_day_avg_count,
  year_to_date_count,
  -- Calculate percent changes
  (seven_day_avg_count - previous_seven_day_avg_count) / NULLIF(previous_seven_day_avg_count, 0) as pct_change_week_over_week
FROM windowed_metrics
ORDER BY date DESC, PROPERTY_TYPE