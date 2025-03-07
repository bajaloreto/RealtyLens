{{
    config(
        materialized='table'
    )
}}


SELECT 
    LISTING_SK,
    LISTING_ID,
    SALE_PRICE,
    PREDICTED_RENT_PRICE,
    RENT_TO_PRICE_RATIO,
    LOAD_DATE,
    MODEL_VERSION
FROM 
    {{ source('realtylens', 'PREDICTED_RENT_PRICES') }}