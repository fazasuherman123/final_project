with source as (
    select * from {{ source('raw', 'payments') }}
),

renamed as (
    select
        cast(payment_id as int64) as payment_id,
        cast(order_id as int64) as order_id,
        cast(payment_date as date) as payment_date,
        payment_method,
        cast(amount as numeric) as amount,
        cast(date as date) as file_date
    from source
)

select * from renamed
