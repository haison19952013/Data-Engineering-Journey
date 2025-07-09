{{
    config(
        materialized='incremental',
        unique_key='sales_key'
    )
}}

with
    fact_sales__source as (select * from {{ ref ("stg_fact_sales") }}),
    fact_sales__compute_revenue as (
        select *, price * amount as revenue from fact_sales__source

    )

select *
from fact_sales__compute_revenue
order by order_key
