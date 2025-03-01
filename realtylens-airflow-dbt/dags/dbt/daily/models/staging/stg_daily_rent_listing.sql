{{
  config(
    materialized = 'view'  
    )
}}

SELECT
  id as property_id,
  formattedAddress as formatted_address,
  addressLine1 as address_line_1,
  addressLine2 as address_line_2,
  city,
  state,
  zipCode as zip_code,
  county,
  latitude,
  longitude,
  propertyType as property_type,
  lotSize as lot_size,
  status,
  price as rent_price,
  listingType as listing_type,
  listedDate as listed_date,
  removedDate as removed_date,
  createdDate as created_date,
  lastSeenDate as last_seen_date,
  daysOnMarket as days_on_market,
  mlsName as mls_name,
  mlsNumber as mls_number,
  load_date,
  'For Rent' as property_status,
  MD5(CONCAT(id, '|', 'For Rent', '|', load_date)) AS listing_id
FROM {{ source('realtylens', 'raw_daily_rent_listing') }}