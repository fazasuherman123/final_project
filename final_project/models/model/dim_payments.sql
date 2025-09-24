with source as (
    select * from {{ ref('stg_payments') }}
)

select
    distinct
    payment_id,
    order_id,
    payment_date,
    payment_method,
    amount
from source
