with source as (
    select * from {{ ref('stg_addresses') }}
)

select
    distinct
    address_id,
    customer_id,
    city,
    state,
    postal_code,
    country
from source
