with
    stg_dim_product__source as (select * from {{ source('glamira_src', 'product') }}),
    stg_dim_product__add_row_number as (
        select
            *, row_number() over (partition by product_id order by name) as row_number
        from stg_dim_product__source

    ),
    stg_dim_product__remove_duplicates as (
        select *
        from stg_dim_product__add_row_number
        where
            row_number = 1
            and (status_code = '200' or status_code = 'playwright_success')
    ),
    stg_dim_product__rename as (
        select product_id as product_key, name as product_name
        from stg_dim_product__remove_duplicates
    ),
    stg_dim_product__cast_type as (
        select cast(product_key as int) as product_key, product_name
        from stg_dim_product__rename
    )

select *
from stg_dim_product__cast_type

    -- union all
    -- select
    -- -1 as product_key,
    -- "unknown" as product_name
    
