with source as (
    select * from {{ source('raw', 'order_items') }}
),

renamed as (
    select
        cast(order_item_id as int64) as order_item_id,
        cast(order_id as int64) as order_id,
        cast(product_id as int64) as product_id,
        cast(quantity as int64) as quantity,
        cast(unit_price as numeric) as unit_price,
        cast(item_total as numeric) as item_total,
        cast(date as date) as file_date
    from source
)

select * from renamed
