with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        cast(customer_id as int64) as customer_id,
        full_name,
        email,
        dob as date_of_birth,
        phone,
        age,
        status,
        cast(date as date) as file_date
    from source
)

select * from renamed
