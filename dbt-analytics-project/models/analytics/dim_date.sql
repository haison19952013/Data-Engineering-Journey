{{
    config(
        materialized='incremental',
        unique_key='full_date'
    )
}}
with
    dim_date__generate as (
        select *
        from
            unnest(
                generate_date_array(
                    date '2015-01-01', date '2025-12-31', interval 1 day
                )
            ) as full_date
    )
select
    full_date,
    format_date('%A', full_date) as day_of_week,  -- Full day name (Monday)
    format_date('%a', full_date) as day_of_week_short,  -- Short day name (Mon)
    case
        when extract(dayofweek from full_date) in (1, 7) then 'Weekend' else 'Weekday'
    end as is_weekday_or_weekend,  -- Weekday or Weekend
    extract(day from full_date) as day_of_month,  -- Day of month (1-31)
    format_date('%Y-%m', full_date) as year_month,  -- Year-Month (e.g. 2024-06)
    cast(extract(month from full_date) as int64) as month,  -- Month as INT
    cast(extract(dayofyear from full_date) as int64) as day_of_year,  -- Day of year as INT
    extract(week from full_date) as week_of_year,  -- Week number (1-53)
    cast(extract(quarter from full_date) as int64) as quarter_number,  -- Quarter as INT
    cast(extract(year from full_date) as int64) as year_number  -- Year as INT
from dim_date__generate
