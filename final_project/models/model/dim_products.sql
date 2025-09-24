with source as (
    select * from {{ ref('stg_products') }}
)


select
    distinct
    product_id,
    product_name,
    category,
    price,
from source
