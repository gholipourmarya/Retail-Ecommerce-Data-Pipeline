{{ config(materialized='incremental', unique_key='product_id') }}

with affected_products as (
    select distinct product_id
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where source_row > (select coalesce(max(last_source_row), 0) from {{ this }})
    {% endif %}
)
select
    orders.product_id,
    count(*) as order_lines,
    sum(orders.quantity) as units_sold,
    sum(orders.revenue) as revenue,
    count(*) filter (where orders.low_stock) as low_stock_lines,
    max(orders.source_row) as last_source_row
from {{ ref('stg_orders') }} as orders
inner join affected_products using (product_id)
group by orders.product_id
