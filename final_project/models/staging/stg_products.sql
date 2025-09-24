with source as (
    select * from {{ source('raw', 'products') }}
),

renamed as (
    select
        cast(product_id as int64) as product_id,
        name as product_name,
        category,
        cast(price as numeric) as price,
        sku,
        current_cost
    from source
)

select * from renamed
