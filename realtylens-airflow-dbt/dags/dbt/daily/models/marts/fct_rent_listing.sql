version: 2

models:
  - name: fct_rent_listing
    description: >
      Incremental fact table tracking rental property listings with their associations 
      to dimension tables and fact measurements like rent price and days on market.
    config:
      materialized: incremental
      unique_key: listing_sk
      incremental_strategy: merge
    columns:
      - name: listing_sk
        description: Primary surrogate key for the listing
        tests:
          - not_null
          - unique
      
      - name: listing_id
        description: Natural key for the listing
        tests:
          - not_null
      
      - name: property_sk
        description: Foreign key to dim_property
        tests:
          - relationships:
              to: ref('dim_property')
              field: property_sk
              severity: warn
      
      - name: status_sk
        description: Foreign key to dim_listing_status
        tests:
          - relationships:
              to: ref('dim_listing_status')
              field: status_sk
              severity: warn
      
      - name: location_sk
        description: Foreign key to dim_location
        tests:
          - relationships:
              to: ref('dim_location')
              field: location_sk
              severity: warn
      
      - name: mls_sk
        description: Foreign key to dim_mls
        tests:
          - relationships:
              to: ref('dim_mls')
              field: mls_sk
              severity: warn
      
      - name: load_date_sk
        description: Date when the listing was loaded
      
      - name: listed_date_sk
        description: Date when the property was listed for rent
      
      - name: removed_date_sk
        description: Date when the property was removed
      
      - name: created_date_sk
        description: Date when the listing was created
      
      - name: last_seen_date_sk
        description: Date when the listing was last seen
      
      - name: rent_price
        description: Rental price for the property
      
      - name: days_on_market
        description: Number of days on the market
      
      - name: property_status
        description: Current property status
      
      - name: status
        description: Current listing status
      
      - name: listing_type
        description: Type of listing
      
      - name: load_date
        description: Raw load date from source
      
      - name: etl_timestamp
        description: Processing timestamp