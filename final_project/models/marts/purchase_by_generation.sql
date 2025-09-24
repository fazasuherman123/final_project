-- models/marts/purchase_by_generation.sql
with customers as (
  select
    customer_id,
    -- pastikan date_of_birth dapat di-cast ke DATE (format YYYY-MM-DD)
    case
      when extract(year from cast(date_of_birth as date)) between 1997 and 2012 then 'Gen Z'
      when extract(year from cast(date_of_birth as date)) between 1981 and 1996 then 'Millennial'
      when extract(year from cast(date_of_birth as date)) between 1965 and 1980 then 'Gen X'
      when extract(year from cast(date_of_birth as date)) between 1946 and 1964 then 'Baby Boomer'
      else 'Other'
    end as generation
  from {{ ref('dim_customers') }}
),

order_items as (
  select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    coalesce(oi.subtotal, oi.quantity * oi.unit_price) as subtotal,
    f.customer_id,
    f.order_date,
    p.payment_method
  from {{ ref('fact_order_items') }} as oi
  join {{ ref('fact_orders') }} as f
    on oi.order_id = f.order_id
  left join {{ ref('dim_payments') }} as p
    on f.order_id = p.order_id
),

base as (
  -- gabungkan generation ke tiap baris order_item
  select
    oi.*,
    c.generation
  from order_items oi
  left join customers c
    on oi.customer_id = c.customer_id
),

-- agregasi utama per generasi
agg_gen as (
  select
    generation,
    sum(subtotal)       as total_revenue,
    sum(quantity)       as total_quantity
  from base
  group by generation
),

-- hitung frekuensi payment_method per generasi
payment_counts as (
  select
    generation,
    payment_method,
    count(1) as payment_count
  from base
  where payment_method is not null
  group by generation, payment_method
),

-- ambil payment_method teratas per generasi (mode)
top_payment as (
  select
    generation,
    payment_method as top_payment_method
  from (
    select
      generation,
      payment_method,
      row_number() over (partition by generation order by payment_count desc) as rn
    from payment_counts
  )
  where rn = 1
)

select
  a.generation,
  a.total_revenue,
  a.total_quantity,
  tp.top_payment_method
from agg_gen a
left join top_payment tp using (generation)
order by a.total_revenue desc nulls last
