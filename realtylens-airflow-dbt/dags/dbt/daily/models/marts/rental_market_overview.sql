{{
  config(
    materialized = 'table'
  )
}}

SELECT
    loc.ZIP_CODE,
    p.PROPERTY_TYPE,
    p.BEDROOMS,
    DATE_TRUNC('day', r.load_date_sk) AS day,
    DATE_TRUNC('week', r.load_date_sk) AS week,
    GROUPING(loc.ZIP_CODE) as is_zip_code_grouping,
    GROUPING(p.PROPERTY_TYPE) as is_property_type_grouping,
    GROUPING(p.BEDROOMS) as is_bedrooms_grouping,
    GROUPING(day) as is_day_grouping,
    COUNT(DISTINCT r.listing_id) AS total_listings,
    AVG(r.RENT_PRICE) AS avg_rent_price,
    MEDIAN(r.RENT_PRICE) AS median_rent_price,
    AVG(r.DAYS_ON_MARKET) AS avg_days_on_market,
    SUM(CASE WHEN p.BATHROOMS IS NOT NULL THEN p.BATHROOMS ELSE 0 END) / NULLIF(COUNT(CASE WHEN p.BATHROOMS IS NOT NULL THEN 1 END), 0) AS avg_bathrooms,
    SUM(CASE WHEN p.SQUARE_FOOTAGE IS NOT NULL THEN p.SQUARE_FOOTAGE ELSE 0 END) / NULLIF(COUNT(CASE WHEN p.SQUARE_FOOTAGE IS NOT NULL THEN 1 END), 0) AS avg_square_footage,
    AVG(CASE WHEN p.SQUARE_FOOTAGE > 0 THEN r.RENT_PRICE / p.SQUARE_FOOTAGE ELSE NULL END) AS avg_price_per_sqft
FROM {{ ref('fct_rent_listing') }} r
LEFT JOIN {{ ref('dim_location') }} loc ON r.location_sk = loc.location_sk
LEFT JOIN {{ ref('dim_property') }} p ON r.property_sk = p.property_sk
WHERE loc.CITY = 'Philadelphia' 
GROUP BY GROUPING SETS (
    -- Most granular - all dimensions
    (loc.ZIP_CODE, p.PROPERTY_TYPE, p.BEDROOMS, day, week),
    
    -- ZIP code analysis
    (loc.ZIP_CODE, p.PROPERTY_TYPE, p.BEDROOMS, week),
    (loc.ZIP_CODE, p.PROPERTY_TYPE, p.BEDROOMS),
    (loc.ZIP_CODE, p.PROPERTY_TYPE),
    (loc.ZIP_CODE),
    
    -- Property type analysis
    (p.PROPERTY_TYPE, p.BEDROOMS, day, week),
    (p.PROPERTY_TYPE, p.BEDROOMS),
    (p.PROPERTY_TYPE),
    
    -- Bedroom count analysis
    (p.BEDROOMS, day, week),
    (p.BEDROOMS),
    
    -- Time analysis
    (day, week),
    (week),
    
    -- Overall market (no groupings)
    ()
)
