with source as (
    select * from {{ ref('stg_customers') }}
)

select
    distinct
    customer_id,
    full_name,
    email,
    phone,
    status,
    date_of_birth,
    age,
from source
