{{
    config(
        materialized='incremental',
        unique_key='sales_key'
    )
}}

with
    sales_flat_source as (

        select
            sales.*,
            location.* except (location_key),
            coalesce(product.product_name, 'XNA') as product_name,
            date.* except (full_date)
        from {{ ref('fact_sales') }} as sales
        left join
            {{ ref('dim_location') }} as location
            on sales.location_key = location.location_key
        left join
            {{ ref('dim_product') }} as product
            on sales.product_key = product.product_key
        left join {{ ref('dim_date') }} as date on sales.full_date = date.full_date
    )

select *
from sales_flat_source
