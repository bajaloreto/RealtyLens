{{
    config(
        materialized='table'
    )
}}

-- This view simply selects all predicted rent prices
-- This allows us to apply more transformations if needed in the future

SELECT 
    LISTING_SK,
    LISTING_ID,
    SALE_PRICE,
    PREDICTED_RENT_PRICE,
    RENT_TO_PRICE_RATIO,
    LOAD_DATE,
    MODEL_VERSION
FROM 
    {{ source('realtylens', 'PREDICTED_RENT_PRICES') }} å