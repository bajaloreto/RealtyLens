{{ config(
    materialized='view' 
) }}

WITH raw_data AS (
    SELECT
        id,
        formattedAddress AS formatted_address,
        addressLine1 AS address_line1,
        addressLine2 AS address_line2,
        city,
        state,
        zipCode AS zip_code,
        county,
        latitude,
        longitude,
        propertyType AS property_type,
        bedrooms,
        bathrooms,
        squareFootage AS square_footage,
        lotSize AS lot_size,
        yearBuilt AS year_built,
        lastSaleDate AS last_sale_date,
        lastSalePrice AS last_sale_price,
        load_date
    FROM {{ source('realtylens', 'raw_property_details') }} 
)

SELECT
    id AS property_id,
    formatted_address,
    address_line1,
    address_line2,
    city,
    state,
    zip_code,
    county,
    latitude,
    longitude,
    property_type,
    bedrooms,
    bathrooms,
    square_footage,
    lot_size,
    year_built,
    last_sale_date,
    last_sale_price,
    load_date
FROM raw_data
WHERE id IS NOT NULL