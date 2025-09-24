with orders as (
    select * from {{ ref('stg_orders') }}
),
order_items as (
    select * from {{ ref('stg_order_items') }}
)

select
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    o.order_status,
    oi.quantity,
    oi.unit_price
from orders o
join order_items oi
    on o.order_id = oi.order_id
