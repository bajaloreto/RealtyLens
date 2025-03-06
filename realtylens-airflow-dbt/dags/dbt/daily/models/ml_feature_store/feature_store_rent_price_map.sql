{{
    config(
        materialized='table'
            )
}}

SELECT 
    -- Record ID
    OBJECT_CONSTRUCT(
        'listing_id', r.listing_id,
        'property_id', p.property_id
    ) AS record_id,
    
    -- Categorical Features
    OBJECT_CONSTRUCT(
        'property_type', p.property_type,
        'city', p.city,
        'state', p.state,
        'zip_code', p.zip_code,
        'county', p.county,
        'status', r.status,
        'listing_type', r.listing_type,
        'zoning_code', p.zoning_code,
        'zoning_group', p.zoning_group
    ) AS categorical_features,
    
    -- Numerical Features
    OBJECT_CONSTRUCT(
        'bedrooms', p.bedrooms,
        'bathrooms', p.bathrooms,
        'square_footage', p.square_footage,
        'lot_size', p.lot_size,
        'year_built', p.year_built,
        'days_on_market', r.days_on_market
    ) AS numerical_features,
    
    -- Target Variable
    OBJECT_CONSTRUCT(
        'rent_price', r.rent_price
    ) AS target_variable,
    
    -- Metadata
    OBJECT_CONSTRUCT(
        'feature_extraction_date', CURRENT_TIMESTAMP(),
        'data_partition', CASE WHEN UNIFORM(0, 1, RANDOM()) < 0.8 THEN 'TRAIN' ELSE 'TEST' END,
        'is_active', TRUE
    ) AS metadata
FROM 
    {{ ref('fct_rent_listing') }} r
JOIN 
    {{ ref('dim_property') }} p
    ON p.property_sk = r.property_sk
WHERE 
    r.load_date_sk = (
        SELECT MAX(load_date_sk) 
        FROM {{ ref('fct_rent_listing') }}
    )

