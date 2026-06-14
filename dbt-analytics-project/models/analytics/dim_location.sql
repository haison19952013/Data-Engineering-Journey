{{
    config(
        materialized='incremental',
        unique_key='location_key'
    )
}}


with dim_location__source AS (
    SELECT *
    FROM {{ref ("stg_dim_location") }} AS dim_location
)

,dim_location__null_handle AS (
SELECT
    location_key,
    ip_address,
    coalesce(country_short_name, 'XNA') AS country_short_name,
    coalesce(country_long_name, 'XNA') AS country_long_name,
    coalesce(region_name, 'XNA') AS region_name,
    coalesce(city_name, 'XNA') AS city_name
FROM dim_location__source
)

,dim_location__undefined_value AS (
SELECT DISTINCT
    location_key,
    ip_address,
    country_short_name,
    country_long_name,
    region_name,
    city_name
FROM dim_location__null_handle

UNION ALL

SELECT
    -1 AS location_key,
    'XNA' AS ip_address,
    'XNA' AS country_short_name,
    'XNA' AS country_long_name,
    'XNA' AS region_name,
    'XNA' AS city_name
)

SELECT * 
FROM dim_location__undefined_value