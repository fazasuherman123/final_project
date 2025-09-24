with source as (
    select * from {{ source('raw', 'addresses') }}
),

renamed as (
    select
        cast(address_id as int64) as address_id,
        cast(customer_id as int64) as customer_id,
        address_line1 as address_line,
        city,
        state,
        postal_code,
        country,
        cast(date as date) as file_date
    from source
)

select * from renamed