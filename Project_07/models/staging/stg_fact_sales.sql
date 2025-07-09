with
    stg_fact_sales__source as (select * from {{ source('glamira_src', 'all') }}),

    stg_fact_sales__unnest_cart_products as (
        select
            cp.product_id,
            ip,
            time_stamp,
            _id,
            cp.price,
            cp.currency,
            cp.amount,
            collection
        from stg_fact_sales__source, unnest(cart_products) as cp
    ),

    stg_fact_sales__checkout_success as (
        select * except (collection)
        from stg_fact_sales__unnest_cart_products
        where collection = 'checkout_success' and product_id is not null
    ),

    stg_fact_sales__remove_null as (
        select * 
        from stg_fact_sales__checkout_success
        where price is not null and currency is not null and currency != '' and amount is not null
    ),

    stg_fact_sales__rename as (
        select
            farm_fingerprint(concat(_id, cast(product_id as string))) as sales_key,
            product_id as product_key,
            farm_fingerprint(ip) as location_key,
            date(timestamp_seconds(time_stamp)) as full_date,
            _id as order_key,
            price,
            currency,
            amount
        from stg_fact_sales__remove_null
    ),

    stg_fact_sales__clean_price as (
        select
            * except (price),
            safe_cast(
                case
                    when regexp_contains(price, '[٠-٩٫]')
                    then replace(replace(price, '٫', '.'), '٠', '0')
                    when regexp_contains(price, r'^\d{1,3}(\'\d{3})*\.\d{2}$')
                    then replace(price, '\'', '')
                    when regexp_contains(price, r'^\d{1,3}(\.\d{3})*,\d{2}$')
                    then replace(replace(price, '.', ''), ',', '.')
                    when regexp_contains(price, r'^\d{1,3}(,\d{3})*\.\d{2}$')
                    then replace(price, ',', '')
                    when regexp_contains(price, r'^\d+,\d{2}$')
                    then replace(price, ',', '.')
                    else replace(price, ',', '')
                end as float64
            ) as price,
            price as original_price
        from stg_fact_sales__rename
    ),

    stg_fact_sales__convert_price_currency as (
        select
            stg_fact_sales__clean_price.* except (price, currency, original_price),
            price * exchange_rate.exchange_rate as price
        from stg_fact_sales__clean_price
        join
            {{ ref('exchange_rate') }} as exchange_rate
            on exchange_rate.symbol = stg_fact_sales__clean_price.currency
    ),

    stg_fact_sales__aggregate_same_product as (
        select
            sales_key,
            product_key,
            location_key,
            full_date,
            order_key,
            sum(price * amount) / sum(amount) as price,
            sum(amount) as amount
        from stg_fact_sales__convert_price_currency
        group by sales_key, product_key, location_key, full_date, order_key
    )

select *
from
    stg_fact_sales__aggregate_same_product

-- where sales_key in (
-- select sales_key
-- from stg_fact_sales__aggregate_same_product
-- group by sales_key
-- having count(*) > 1
-- )

