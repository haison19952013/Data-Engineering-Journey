with
    stg_dim_location__source as (
        select distinct * from {{ source('glamira_src', 'ip_location') }}
    ),
    stg_dim_location__rename as (
        select
            ip as ip_address,
            country_short as country_short_name,
            country_long as country_long_name,
            region as region_name,
            city as city_name
        from stg_dim_location__source
        where country_long <> '-' and ip <> '-' and ip is not null
    ),
    stg_dim_location__handle_invalid as (
        select
            farm_fingerprint(ip_address) as location_key,
            ip_address,
            country_short_name,
            country_long_name,
            case
                when trim(region_name) = '' or region_name = '-'
                then null
                else region_name
            end as region_name,
            -- REPLACE(region_name, '-', 'XNA') AS region_name,
            case
                when trim(city_name) = '' or city_name = '-' then null else city_name
            end as city_name
        -- REPLACE(city_name, '-', 'XNA') AS city_name
        from stg_dim_location__rename
    )

select *
from stg_dim_location__handle_invalid

