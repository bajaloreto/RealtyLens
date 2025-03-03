{{
  config(
    materialized = 'table'
  )
}}

WITH property_location AS (
    SELECT 
        p.PROPERTY_ID,
        l.ZIP_CODE
    FROM {{ ref('dim_property') }} p
    JOIN {{ ref('dim_location') }} l ON p.location_sk = l.location_sk
    WHERE l.CITY = 'Philadelphia'
),

daily_metrics AS (
    SELECT
        c.date,
        pl.ZIP_CODE,
        p.PROPERTY_TYPE,
        GROUPING(pl.ZIP_CODE) as is_zip_code_grouping,
        GROUPING(p.PROPERTY_TYPE) as is_property_type_grouping,
        COUNT(DISTINCT c.PROPERTY_ID) AS total_properties,
        COUNT(DISTINCT CASE WHEN c.property_state = 'New' THEN c.PROPERTY_ID END) AS new_properties,
        COUNT(DISTINCT CASE WHEN c.property_state = 'Retained' THEN c.PROPERTY_ID END) AS retained_properties,
        COUNT(DISTINCT CASE WHEN c.property_state = 'Churned' THEN c.PROPERTY_ID END) AS churned_properties,
        COUNT(DISTINCT CASE WHEN c.property_state = 'Resurrected' THEN c.PROPERTY_ID END) AS resurrected_properties,
        COUNT(DISTINCT CASE WHEN c.property_state = 'Inactive' THEN c.PROPERTY_ID END) AS inactive_properties,
        COUNT(DISTINCT CASE WHEN c.price_state = 'Price Increased' THEN c.PROPERTY_ID END) AS price_increased,
        COUNT(DISTINCT CASE WHEN c.price_state = 'Price Decreased' THEN c.PROPERTY_ID END) AS price_decreased,
        COUNT(DISTINCT CASE WHEN c.price_state = 'Price Unchanged' THEN c.PROPERTY_ID END) AS price_unchanged,
        AVG(c.RENT_PRICE) AS avg_rent_price,
        MEDIAN(c.RENT_PRICE) AS median_rent_price,
        AVG(c.days_on_market) AS avg_days_on_market
    FROM {{ ref('cumulative_rent_listing') }} c
    LEFT JOIN property_location pl ON c.PROPERTY_ID = pl.PROPERTY_ID
    LEFT JOIN {{ ref('dim_property') }} p ON c.PROPERTY_ID = p.PROPERTY_ID
    WHERE pl.ZIP_CODE IS NOT NULL  -- Ensure we only include Philadelphia properties
    GROUP BY GROUPING SETS (
        (c.date, pl.ZIP_CODE, p.PROPERTY_TYPE),
        (c.date, pl.ZIP_CODE),
        (c.date, p.PROPERTY_TYPE),
        (c.date),
        (DATE_TRUNC('week', c.date), pl.ZIP_CODE, p.PROPERTY_TYPE),
        (DATE_TRUNC('week', c.date), pl.ZIP_CODE),
        (DATE_TRUNC('week', c.date), p.PROPERTY_TYPE),
        (DATE_TRUNC('week', c.date))
    )
)

SELECT
    date,
    ZIP_CODE,
    PROPERTY_TYPE,
    DATE_TRUNC('week', date) AS week,
    is_zip_code_grouping,
    is_property_type_grouping,
    total_properties,
    new_properties,
    retained_properties,
    churned_properties,
    resurrected_properties,
    inactive_properties,
    -- Calculate rates
    new_properties / NULLIF(total_properties, 0) AS new_rate,
    retained_properties / NULLIF(total_properties, 0) AS retention_rate,
    churned_properties / NULLIF(total_properties, 0) AS churn_rate,
    resurrected_properties / NULLIF(total_properties, 0) AS resurrection_rate,
    inactive_properties / NULLIF(total_properties, 0) AS inactive_rate,
    -- Price metrics
    price_increased,
    price_decreased,
    price_unchanged,
    price_increased / NULLIF(total_properties, 0) AS price_increase_rate,
    price_decreased / NULLIF(total_properties, 0) AS price_decrease_rate,
    avg_rent_price,
    median_rent_price,
    avg_days_on_market
FROM daily_metrics
