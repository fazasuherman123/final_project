with orders as (
    select
        o.order_id,
        o.order_date,
        oi.quantity,
        oi.unit_price,
        (oi.quantity * oi.unit_price) as subtotal
    from {{ ref('fact_order_items') }} oi
    join {{ ref('fact_orders') }} o
        on oi.order_id = o.order_id
)

select
    order_date,
    sum(subtotal) as total_revenue,
    sum(quantity) as total_quantity
from orders
group by order_date
order by order_date
