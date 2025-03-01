{{
  config(
    materialized = 'table'
  )
}}

WITH date_spine AS (
  {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2000-01-01' as date)",
    end_date="cast(dateadd(year, 30, current_date) as date)"
  )
  }}
)

SELECT
  date_day as date_sk,
  date_day,
  EXTRACT(DAY FROM date_day) as day_of_month,
  EXTRACT(MONTH FROM date_day) as month_number,
  EXTRACT(YEAR FROM date_day) as year_number,
  EXTRACT(QUARTER FROM date_day) as quarter_number,
  EXTRACT(DAYOFWEEK FROM date_day) as day_of_week,
  DATE_TRUNC('week', date_day) as week_start_date,
  DATE_TRUNC('month', date_day) as month_start_date,
  DATE_TRUNC('quarter', date_day) as quarter_start_date,
  DATE_TRUNC('year', date_day) as year_start_date,
  TO_CHAR(date_day, 'Day') as day_name,
  TO_CHAR(date_day, 'Month') as month_name
FROM date_spine