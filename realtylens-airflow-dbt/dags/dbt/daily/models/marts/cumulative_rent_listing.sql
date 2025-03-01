{{
  config(
    materialized = 'incremental',
    unique_key = ['PROPERTY_ID', 'date']
  )
}}

WITH daily_data AS (
  SELECT
    r.PROPERTY_ID,
    r.RENT_PRICE,
    r.LISTED_DATE,
    r.LOAD_DATE as date
  FROM {{ ref('stg_daily_rent_listing') }} r
  WHERE r.LOAD_DATE = (SELECT MAX(LOAD_DATE) FROM {{ ref('stg_daily_rent_listing') }})
),

previous_data AS (
  {% if is_incremental() %}
    SELECT * FROM {{ this }} WHERE date = (SELECT MAX(date) FROM {{ this }})
  {% else %}
    -- First run scenario - create empty structure
    SELECT
      NULL as PROPERTY_ID,
      NULL as first_listed_date,
      NULL as last_active_date,
      NULL as date,
      NULL as RENT_PRICE,
      NULL as days_on_market,
      NULL as property_state,
      NULL as price_state
    WHERE 1=0
  {% endif %}
),

combined AS (
  SELECT
    COALESCE(t.PROPERTY_ID, p.PROPERTY_ID) as PROPERTY_ID,
    COALESCE(t.LISTED_DATE, p.first_listed_date) as first_listed_date,
    COALESCE(t.date, p.last_active_date) as last_active_date,
    COALESCE(t.date, p.date + INTERVAL '1 day') as date,
    t.RENT_PRICE as current_price,
    p.RENT_PRICE as previous_price,
    -- Days on market calculation
    DATEDIFF(day, COALESCE(t.LISTED_DATE, p.first_listed_date), COALESCE(t.date, p.date + INTERVAL '1 day')) as days_on_market,
    -- Property state tracking
    CASE 
      WHEN p.PROPERTY_ID IS NULL THEN 'New'
      WHEN p.PROPERTY_STATE in ('Retained', 'Resurrected', 'New') AND t.PROPERTY_ID IS NOT NULL THEN 'Retained'
      WHEN p.PROPERTY_STATE IN ('Retained', 'Resurrected', 'New') AND t.PROPERTY_ID IS NULL THEN 'Churned'
      WHEN t.PROPERTY_ID IS NOT NULL AND p.PROPERTY_STATE in ('Churned', 'Inactive') THEN 'Resurrected'
      WHEN p.PROPERTY_STATE = 'Churned' AND t.PROPERTY_ID IS NOT NULL THEN 'Inactive'
      ELSE COALESCE(p.PROPERTY_STATE, 'Unknown')
    END as property_state,
    -- Price change tracking
    CASE
      WHEN p.PROPERTY_ID IS NULL THEN 'New'
      WHEN t.RENT_PRICE > p.RENT_PRICE THEN 'Price Increased'
      WHEN t.RENT_PRICE < p.RENT_PRICE THEN 'Price Decreased'
      WHEN t.RENT_PRICE = p.RENT_PRICE THEN 'Price Unchanged'
      WHEN t.PROPERTY_ID IS NOT NULL AND p.PROPERTY_STATE in ('Churned', 'Inactive') THEN 'Resurrected'
      ELSE 'Unknown'
    END as price_state
  FROM daily_data t
  FULL OUTER JOIN previous_data p
    ON t.PROPERTY_ID = p.PROPERTY_ID
)

SELECT
  PROPERTY_ID,
  COALESCE(first_listed_date, date) as first_listed_date,
  last_active_date,
  date,
  current_price as RENT_PRICE,
  days_on_market,
  property_state,
  price_state
FROM combined