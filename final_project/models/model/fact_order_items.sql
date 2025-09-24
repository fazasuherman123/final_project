with source as (
    select * from {{ ref('stg_order_items') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
)

select
    oi.order_item_id,
    p.payment_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price) as subtotal
from source oi
join payments p
    on oi.order_id = p.order_id
