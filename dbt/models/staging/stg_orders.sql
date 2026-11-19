select
    source_row,
    order_id,
    product_id,
    quantity,
    round(quantity * unit_price, 2) as revenue,
    stock < 15 as low_stock
from {{ source('retail_raw', 'orders') }}
