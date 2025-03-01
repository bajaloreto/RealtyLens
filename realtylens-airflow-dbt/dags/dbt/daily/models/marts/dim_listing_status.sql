{{
  config(
    materialized = 'table'
  )
}}

-- Simple seed table with all possible listing statuses
SELECT 
  {{ dbt_utils.generate_surrogate_key(['status_code']) }} as status_sk,
  status_code,
  status_description
FROM (
  VALUES
    ('FS', 'For Sale'),
    ('FR', 'For Rent'),
    ('FSFR', 'For Sale/For Rent'),
    ('C', 'Churned'),
    ('I', 'Inactive'),
    ('A', 'Active'),
    ('N', 'New'),
    ('R', 'Resurrected')
) as statuses(status_code, status_description)
