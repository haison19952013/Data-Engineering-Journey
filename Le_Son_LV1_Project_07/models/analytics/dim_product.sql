{{
    config(
        materialized='incremental',
        unique_key='product_key'
    )
}}

with
    dim_product__source as (select * from {{ ref ("stg_dim_product") }} as product),
    dim_product__undefined_value as (
        select distinct product_key, product_name
        from dim_product__source

        union all

        select -1 as product_key, 'XNA' as product_name
    )

select *
from dim_product__undefined_value
order by product_key
