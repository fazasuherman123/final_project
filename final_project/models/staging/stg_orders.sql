with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        cast(order_id as int64) as order_id,
        cast(customer_id as int64) as customer_id,
        cast(order_date as date) as order_date,
        order_status,
        cast(shipping_address_id as int64) as shipping_address_id,
        cast(billing_address_id as int64) as billing_address_id,
        cast(total_amount as numeric) as total_amount,
        cast(date as date) as file_date
    from source
)

select * from renamed
